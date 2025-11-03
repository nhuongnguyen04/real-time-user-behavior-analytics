import pytest
from pyspark.sql import Row
from pyspark.ml import PipelineModel
from stream_processor.writers import write_summary_with_model


@pytest.mark.usefixtures("mock_postgres", "mock_es", "mock_kafka")
def test_elasticsearch_anomaly_document(spark, mock_postgres, mock_es, mock_kafka, monkeypatch):
    # Load Isolation Forest model đã train
    model = PipelineModel.load("/models/anomaly_iforest")

    # Fake batch có anomaly
    data = [
        Row(window_start="2025-09-20 10:00:00",
            window_end="2025-09-20 10:01:00",
            user_id=101,
            event_count=500,
            action="click",
            user_name="Alice",
            region="HN")
    ]
    batch_df = spark.createDataFrame(data)

    users_df = spark.createDataFrame([
        Row(user_id=101, name="Alice", region="HN"),
    ])

    # Patch es_helpers.bulk để capture docs
    captured_docs = {}

    def fake_bulk(client, docs, chunk_size, raise_on_error):
        captured_docs["docs"] = docs
        return True

    monkeypatch.setattr("stream_processor.writers.es_helpers.bulk", fake_bulk)
    

    # Run handler
    write_summary_with_model(batch_df, batch_id=99, window_type="5s", users_df=users_df, model=model)

    # Đảm bảo bulk được gọi và có docs
    assert "docs" in captured_docs, "Elasticsearch bulk index was not called"
    docs = captured_docs["docs"]
    assert len(docs) >= 1

    # Lấy document đầu tiên
    doc = docs[0]["_source"]

    # Các field cần có
    required_fields = {
        "type",
        "user_id",
        "event_count",
        "window_start",
        "window_end",
        "user_name",
        "region",
        "detected_at",
    }
    assert required_fields.issubset(doc.keys()), f"Missing fields in ES doc: {doc}"

    # Giá trị hợp lý
    assert doc["type"] == "anomaly"
    assert doc["user_id"] == 101
    assert doc["user_name"] == "Alice"
    assert doc["region"] == "HN"
    assert isinstance(doc["detected_at"], str)  # phải là timestamp ISO string
    
