# /opt/spark-apps/stream_processor/validation.py
from pyspark.sql.functions import col, current_timestamp, length, lit, udf
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from datetime import datetime

from spark.spark_app.stream_processor.config import VALID_ACTIONS, MAX_LOCATION_LEN, logger, KAFKA_DLQ_TOPIC
from spark.spark_app.stream_processor.helpers import get_alert_producer

def validate_df(df):
    cond_user = (col("user_id").isNotNull() & (col("user_id") > 0))
    cond_product = (col("product_id").isNotNull() & (col("product_id") > 0))
    cond_action = col("action").isin(*VALID_ACTIONS) if VALID_ACTIONS else col("action").isNotNull()
    cond_time = col("event_time") <= current_timestamp()
    cond_loc_len = length(col("location")) <= MAX_LOCATION_LEN
    valid_cond = cond_user & cond_product & cond_action & cond_time & cond_loc_len
    
    valid_df = df.filter(valid_cond)
    invalid_df = df.filter(~valid_cond)

    #attach reason - use UDF for complex reasons
    invalid_df = invalid_df.withColumn("dq_reason",lit(None).cast(StringType()))
    def reason_fn(user_id, product_id, action, event_time, location):
        reasons = []
        if user_id is None or user_id <= 0:
            reasons.append("invalid_user_id")
        if product_id is None or product_id <= 0:
            reasons.append("invalid_product_id")
        if action not in VALID_ACTIONS:
            reasons.append("invalid_action")
        if event_time is None or event_time > datetime.utcnow():
            reasons.append("invalid_event_time")
        if location and len(location) > MAX_LOCATION_LEN:
            reasons.append("location_too_long")
        return ",".join(reasons) if reasons else None
    reason_udf = F.udf(reason_fn, StringType())
    invalid_df = invalid_df.withColumn("dq_reason", reason_udf(col("user_id"), col("product_id"), col("action"), col("event_time"), col("location")))
    return valid_df, invalid_df

# ----------------------
# Dead-letter: send invalid rows to DLQ topic
# ----------------------
def publish_invalid_to_dlq(invalid_df, batch_id):
    if invalid_df.rdd.isEmpty():
        logger.info(f"[DLQ] Batch {batch_id} empty -> skip")
        return
    prod = get_alert_producer()
    if not prod:
        logger.warning(f"No kafka producer available to publish DLQ records. Count={invalid_df.count()}")
        return
    sent = 0
    for row in invalid_df.collect():  # Optimized to collect once instead of iterator
        try:
            msg = {"batch_id": batch_id, "row": row.asDict(), "dq_reason": row["dq_reason"], "reported_at": datetime.utcnow().isoformat()}
            prod.send(KAFKA_DLQ_TOPIC, msg)
            sent += 1
        except Exception as e:
            logger.error(f"[ERROR] publish_invalid_to_dlq exception for batch {batch_id}: {e}")
    prod.flush(timeout=5)
    logger.info(f"Published {sent} invalid rows to DLQ topic {KAFKA_DLQ_TOPIC}")