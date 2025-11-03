# tests/test_writers_iforest.py (Sửa: Sử dụng mock_model, cụ thể mock checks)
import pytest
from pyspark.sql import Row
from datetime import datetime
from pyspark.ml import PipelineModel
from stream_processor.writers import write_summary_with_model

@pytest.mark.usefixtures("mock_postgres", "mock_es", "mock_kafka", "mock_model")
def test_write_summary_with_model_detects_and_persists(spark, mock_postgres, mock_es, mock_kafka, mock_model):
    model = mock_model

    # Fake input batch
    data = [
        Row(window_start=datetime(2025, 9, 20, 10, 0, 0),
            window_end=datetime(2025, 9, 20, 10, 1, 0),
            user_id=1,
            event_count=100,
            action="click",
            user_name="Alice",
            region="HN"),
        Row(window_start=datetime(2025, 9, 20, 10, 0, 0),
            window_end=datetime(2025, 9, 20, 10, 1, 0),
            user_id=2,
            event_count=2,
            action="click",
            user_name="Bob",
            region="HCM"),
    ]
    batch_df = spark.createDataFrame(data)
    users_df = spark.createDataFrame([
        Row(user_id=1, name="Alice", region="HN"),
        Row(user_id=2, name="Bob", region="HCM"),
    ])

    # Run handler
    write_summary_with_model(batch_df, batch_id=1, window_type="5s", users_df=users_df, model=model)

    # Assert Postgres upsert was called
    conn, cur = mock_postgres
    assert cur.execute.called or cur.executemany.called

    # Assert ES bulk index anomalies
    assert mock_es.bulk.called  # Giả sử mock_es là MagicMock với bulk method

    # Assert Kafka producer sent messages
    assert mock_kafka.send.called
    assert mock_kafka.flush.called