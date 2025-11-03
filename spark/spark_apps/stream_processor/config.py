# /opt/spark-apps/stream_processor/config.py
import os
import logging

# CONFIG (edit via env when possible)
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user_events")
KAFKA_ALERT_TOPIC = os.getenv("KAFKA_ALERT_TOPIC", "anomaly_alerts")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "dead_letter_topic")

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
SCHEMA_SUBJECT = os.getenv("SCHEMA_SUBJECT", "user_events-value")

POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/realtime_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")

ES_HOST = os.getenv("ES_HOST", "http://elasticsearch:9200")
ES_INDEX = os.getenv("ES_INDEX", "user_event_logs")

CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", "/opt/spark/spark-checkpoints/realtime")
ANOMALY_THRESHOLD = int(os.getenv("ANOMALY_THRESHOLD", "5"))
COPY_CHUNK = int(os.getenv("COPY_CHUNK", "5000"))
ES_BULK_CHUNK = int(os.getenv("ES_BULK_CHUNK", "2000"))

PROCESSING_TRIGGER = os.getenv("PROCESSING_TRIGGER", "1 seconds")
WINDOW_DURATION_5S = "5 seconds"
WINDOW_DURATION_1MIN = "1 minute"
WINDOW_SLIDE = os.getenv("WINDOW_SLIDE", "5 seconds")
WATERMARK_5S = "30 seconds"
WATERMARK_1MIN = "2 minutes"
Z_THRESHOLD = float(os.getenv("Z_THRESHOLD", "2.0"))

VALID_ACTIONS = os.getenv("VALID_ACTIONS", "click,view,purchase,add_to_cart").split(",")
MAX_LOCATION_LEN = int(os.getenv("MAX_LOCATION_LEN", "200"))

# Logging
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("stream_processor")