# /opt/spark-apps/stream_processor/main.py
#!/usr/bin/env python3
import json
import signal
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, count, length
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
)

# from_avro available when spark-avro package is present
try:
    from pyspark.sql.avro.functions import from_avro
except Exception:
    from_avro = None

from .config import (
    KAFKA_BROKER,
    KAFKA_TOPIC,
    SCHEMA_REGISTRY_URL,
    SCHEMA_SUBJECT,
    POSTGRES_URL,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    CHECKPOINT_BASE,
    PROCESSING_TRIGGER,
    WINDOW_DURATION_5S,
    WINDOW_DURATION_1MIN,
    WINDOW_SLIDE,
    WATERMARK_5S,
    WATERMARK_1MIN,
    VALID_ACTIONS,
    MAX_LOCATION_LEN,
    logger,
)
from .helpers import fetch_avro_schema_from_registry
from .writers import foreach_validate_and_route, write_summary_and_anomaly

# ==========================
# Spark session
# ==========================
spark = SparkSession.builder \
    .appName("StreamProcessor") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.streaming.fileSink.logCleanupDelay", "1 hours") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

users_df = spark.read.jdbc(
    url=POSTGRES_URL,
    table="users",
    properties={"user": POSTGRES_USER, "password": POSTGRES_PASSWORD},
).cache()

# ----------------------
# If using Avro from Schema Registry: get JSON schema string
# ----------------------
SCHEMA_JSON = fetch_avro_schema_from_registry(SCHEMA_SUBJECT, SCHEMA_REGISTRY_URL) if SCHEMA_REGISTRY_URL and SCHEMA_SUBJECT else None
if SCHEMA_JSON is None and SCHEMA_REGISTRY_URL:
    logger.warning("No schema available from Schema Registry.")
elif from_avro is None and SCHEMA_JSON:
    logger.warning("from_avro not available. Ensure spark-avro package is provided.")

# ==========================
# Read from Kafka
# ==========================
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", 1000) \
    .load()

if SCHEMA_JSON and from_avro:
    parsed = kafka_df.select(from_avro(col("value"), json.loads(SCHEMA_JSON)).alias("data")) \
        .filter(col("data").isNotNull()) \
        .select("data.*") \
        .withColumn("event_time", (col("timestamp") / 1000).cast(TimestampType())) \
        .dropna(subset=["user_id", "product_id", "action", "event_time"]) \
        .filter(col("action").isin(VALID_ACTIONS)) \
        .filter(col("location").rlike(f"^.{1,MAX_LOCATION_LEN}$")) \
        .repartition(8, "user_id")
    logger.info("Using from_avro with schema from Schema Registry")
else:
    # Fallback: define schema manually (if no schema registry)
    schema = StructType([
        StructField("user_id", IntegerType(), False),
        StructField("product_id", IntegerType(), False),
        StructField("action", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("device_id", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("location", StringType(), True),
        StructField("user_segment", StringType(), True),
        StructField("ip_address", StringType(), True)
    ])
    parsed = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"),schema).alias("data")) \
        .filter(col("data").isNotNull()) \
        .select("data.*") \
        .withColumn("event_time", to_timestamp(col("timestamp"))) \
        .dropna(subset=["user_id", "product_id", "action", "event_time"]) \
        .repartition(8, "user_id")

# Print schema for debugging
logger.info(f"Parsed schema: {parsed.schema}")

# Start raw stream
raw_query = parsed.writeStream \
    .outputMode("append") \
    .foreachBatch(foreach_validate_and_route) \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/user_events") \
    .trigger(processingTime=PROCESSING_TRIGGER) \
    .start()

# ==========================
# Write Parquet
# ==========================
parquet_query = parsed.writeStream \
    .format("parquet") \
    .option("path", "/tmp/parquet-logs") \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/parquet") \
    .trigger(processingTime=PROCESSING_TRIGGER) \
    .start()

windowed_5s = parsed.withWatermark("event_time", WATERMARK_5S) \
    .groupBy(window(col("event_time"), WINDOW_DURATION_5S, WINDOW_SLIDE), col("user_id")) \
    .agg(count("*").alias("event_count"))
windowed_1min = parsed.withWatermark("event_time", WATERMARK_1MIN) \
    .groupBy(window(col("event_time"), WINDOW_DURATION_1MIN, WINDOW_SLIDE), col("user_id")) \
    .agg(count("*").alias("event_count"))
# Start windowed stream
agg_query_5s = windowed_5s.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda df, id: write_summary_and_anomaly(df, id, "5s")) \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/user_activity_summary_5s") \
    .start()

agg_query_1min = windowed_1min.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda df, id: write_summary_and_anomaly(df, id, "1min")) \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/user_activity_summary_1min") \
    .trigger(processingTime=PROCESSING_TRIGGER) \
    .start()



def shutdown(signum, frame):
    logger.info("Received shutdown signal. Stopping queries gracefully...")
    raw_query.stop()
    parquet_query.stop()
    agg_query_5s.stop()
    agg_query_1min.stop()
    spark.stop()
    sys.exit(0)
signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)

# ==========================
# Await termination
# ==========================
spark.streams.awaitAnyTermination()
