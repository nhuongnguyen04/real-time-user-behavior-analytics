# /opt/spark-apps/stream_processor/helpers.py
import io
import json
import time
from datetime import datetime
import requests
import psycopg2

from elasticsearch import Elasticsearch

try:
    from kafka import KafkaProducer
except Exception:
    KafkaProducer = None

from spark.spark_apps.stream_processor.config import POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD, ES_HOST, KAFKA_BROKER, logger, KAFKA_DLQ_TOPIC
# ==========================
# HELPERS: Postgres connection parsing & connect
# ==========================
def parse_jdbc_postgres(jdbc_url: str) -> dict:
    # Parse jdbc:postgresql://host:port/dbname (may include params)
    # returns dict with host, port, dbname

    # remove prefix
    prefix = "jdbc:postgresql://"
    if not jdbc_url.startswith(prefix):
        raise ValueError("Unsupported POSTGRES_URL format. Expect jdbc:postgresql://host:port/db")
    rest = jdbc_url[len(prefix):]
    # split host:port and db (db may contain ?params)
    if "/" not in rest:
        raise ValueError("POSTGRES_URL parsing failed")
    hostport, db_and_qs = rest.split("/", 1)
    host, port = (hostport.split(":", 1) if ":" in hostport else (hostport, "5432"))
    dbname = db_and_qs.split("?", 1)[0]
    return {"host": host, "port": int(port), "dbname": dbname}

PG_CONN_INFO = parse_jdbc_postgres(POSTGRES_URL)

def get_psycopg2_conn():
    params = PG_CONN_INFO
    return psycopg2.connect(
        host=params["host"],
        port=params["port"],
        dbname=params["dbname"],
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        connect_timeout=10
    )


# ----------------------
# Elasticsearch helper
# ----------------------
_es_client = None
def get_es_client():
    global _es_client
    if _es_client is None:
        try:
            _es_client = Elasticsearch([ES_HOST], request_timeout=10)
        except Exception as e:
            logger.warning("ES client init failed: %s", e)
            _es_client = None
    return _es_client

# ----------------------
# Kafka producer helper (alerts & DLQ)
# ----------------------
_alert_producer = None
def get_alert_producer(max_retries=3, retry_delay=5):
    global _alert_producer
    if _alert_producer is None:
        for attempt in range(max_retries):
            try:
                _alert_producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BROKER,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    api_version=(2, 8, 0),
                    compression_type="snappy"
                )
                logger.info("Kafka producer initialized successfully")
                return _alert_producer
            except Exception as e:
                logger.warning("Kafka producer init failed (attempt %d/%d): %s", attempt + 1, max_retries, e)
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        logger.error("Failed to initialize Kafka producer after %d attempts", max_retries)
        _alert_producer = None
    return _alert_producer

# ----------------------
# Fetch schema from Schema Registry
# ----------------------
def fetch_avro_schema_from_registry(subject: str, sr_url: str) -> str | None:
    # Fetch latest schema string for subject from Schema Registry
    # Returns schema string or None on failure
    try:
        url = f"{sr_url}/subjects/{subject}/versions/latest"
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        schema_str = data.get("schema")
        if schema_str:
            logger.info(f"Fetched schema for subject {subject} from Schema Registry")
            return schema_str
        else:
            raise ValueError("Schema not found in response")
    except Exception as e:
        logger.error(f"Failed fetching schema from registry: {e}")
        return None

# ----------------------
# Row to copy buffer
# ----------------------
def row_to_copy_buffer(row: list, columns: list[str]) -> io.StringIO:
    # Convert list of Row objects to a text buffer understood by COPY (tab-separated, \N for null).
    # Use timestamp formatted as "YYYY-MM-DD HH:MM:SS" to match TIMESTAMP without tz.
    buf = io.StringIO()
    for r in row:
        values = []
        for c in columns:
            val = r[c]
            if val is None:
                values.append(r"\N")
            elif isinstance(val, datetime):
                # If it's TimestampType (python datetime), format appropriately
                values.append(val.strftime("%Y-%m-%d %H:%M:%S"))
            else:
                # ensure no newlines or tabs break the COPY: replace \t and \n
                s = str(val).replace("\t", " ").replace("\n", " ")
                values.append(s)
        buf.write("\t".join(values) + "\n")
    buf.seek(0)
    return buf

# Dead-letter: send chunk to DLQ
def send_chunk_to_dlq(chunk_rows: list, batch_id: str, reason: str):
    prod = get_alert_producer()
    if not prod:
        logger.warning("No Kafka producer for DLQ; dropping %d rows", len(chunk_rows))
        return
    for r in chunk_rows:
        msg = {"batch_id": batch_id, "reason": reason, "row": r.asDict(), "reported_at": datetime.utcnow().isoformat()}
        try:
            prod.send(KAFKA_DLQ_TOPIC, msg)
        except Exception as e:
            logger.warning("Failed sending DLQ message: %s", e)
    try:
        prod.flush(timeout=5)
    except Exception:
        pass