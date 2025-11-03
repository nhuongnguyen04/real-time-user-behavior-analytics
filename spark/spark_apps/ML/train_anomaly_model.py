from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from pyspark.sql.functions import col
from stream_processor.config import POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD

spark = SparkSession.builder \
    .appName("AnomalyDetection_KMeans") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# 1. Load data từ Postgres
events_df = spark.read.jdbc(
    url=POSTGRES_URL,
    table="user_events",
    properties={"user": POSTGRES_USER, "password": POSTGRES_PASSWORD}
)

# 2. Aggregate dữ liệu (ví dụ: số event theo user, action)
aggregated_df = events_df.groupBy("user_id", "action") \
    .count().withColumnRenamed("count", "event_count")

# 3. Encode categorical feature "action"
action_indexer = StringIndexer(inputCol="action", outputCol="action_index")
action_encoder = OneHotEncoder(inputCols=["action_index"], outputCols=["action_encoded"])
vector_assembler = VectorAssembler(
    inputCols=["event_count", "action_encoded"],
    outputCol="features"
)

# 4. Train KMeans
kmeans = KMeans(k=5, seed=42, featuresCol="features", predictionCol="cluster")
pipeline = Pipeline(stages=[action_indexer, action_encoder, vector_assembler, kmeans])

model = pipeline.fit(aggregated_df)

# 5. Tính khoảng cách đến cluster centers (để dùng inference sau này)
transformed = model.transform(aggregated_df)

# Add distance to nearest cluster center
centers = model.stages[-1].clusterCenters()
def distance_to_center(features, cluster_id):
    center = centers[cluster_id]
    return float(features.squared_distance(center) ** 0.5)  # Euclidean distance

spark.udf.register("dist_to_center", distance_to_center)

with_dist = transformed.withColumn(
    "distance_to_center",
    col("features").alias("f")
)

# TODO: lưu threshold (ví dụ 95th percentile) để inference
# (ở đây minh họa bằng giá trị cứng)
threshold = with_dist.selectExpr("percentile_approx(distance_to_center, 0.95)").collect()[0][0]

# 6. Save model và threshold
model.write().overwrite().save("/models/anomaly_kmeans")
spark.createDataFrame([(threshold,)], ["threshold"]) \
     .write.mode("overwrite").parquet("/models/anomaly_kmeans_threshold")

print(f"✅ KMeans anomaly model trained. Threshold = {threshold}")

spark.stop()
