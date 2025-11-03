# tests/conftest.py (Sửa: Thêm mock_model fixture, mock datetime.utcnow cho consistency)
import pytest
from pyspark.sql import SparkSession
from unittest.mock import MagicMock
from datetime import datetime

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("pytest-pyspark")
        .master("local[2]")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()
    
@pytest.fixture
def mock_postgres(monkeypatch):
    """Mock Postgres connection (psycopg2)."""
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    monkeypatch.setattr("stream_processor.writers.get_psycopg2_conn", lambda: conn)
    return conn, cur

@pytest.fixture
def mock_es(monkeypatch):
    """Mock Elasticsearch client."""
    es = MagicMock()
    es.bulk = MagicMock()  # Thêm để test calls
    monkeypatch.setattr("stream_processor.writers.get_es_client", lambda: es)
    return es

@pytest.fixture
def mock_kafka(monkeypatch):
    """Mock Kafka producer."""
    producer = MagicMock()
    monkeypatch.setattr("stream_processor.writers.get_alert_producer", lambda: producer)
    return producer

@pytest.fixture
def mock_model(monkeypatch):
    """Mock PipelineModel.load and transform."""
    model = MagicMock()
    def fake_transform(df):
        # Giả lập prediction: high event_count là anomaly (-1)
        from pyspark.sql.functions import when, lit
        return df.withColumn("prediction", when(df.event_count > 50, -1).otherwise(1))
    model.transform = fake_transform
    monkeypatch.setattr("pyspark.ml.PipelineModel.load", lambda path: model)
    return model

@pytest.fixture(autouse=True)
def mock_utcnow(monkeypatch):
    """Mock datetime.utcnow cho tests time-based."""
    fixed_time = datetime(2025, 9, 21, 12, 0, 0)
    monkeypatch.setattr("datetime.datetime.utcnow", lambda: fixed_time)