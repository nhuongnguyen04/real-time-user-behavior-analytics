import pytest
from pyspark.sql import Row
from pyspark.ml import PipelineModel
from stream_processor.writers import write_summary_with_model


def test_end_to_end_with_mocks(spark, monkeypatch):
    # ============================
    # 1. Mock Postgres
    # ============================
    pg_calls = {"sql": [], "values": []}

    class FakeCursor:
        def execute(self, sql, params=None):
            pg_calls["sql"].append(sql)
            pg_calls["values"].append(params)

        def close(self): pass

    class FakeConn:
        def __init__(self):
            self.cur = FakeCursor()
            self.committed = False
            self.rolled_back = False

        def cursor(self): return self.cur
        def commit(self): self.committed = True
        def rollback(self): self.rolled_back = True
        def close(self): pass

    fake_conn = FakeConn()
    monkeypatch.setattr("stream_processor.writers.get_psycopg2_conn", lambda: fake_conn)

    # ============================
    # 2. Mock Elasticsearch
    # ============================
    captured_docs = {}
    def fake_bulk(client, docs, chunk_size, raise_on_error):
        captured_docs["docs"] = docs
        return True

    fake_es = object()
    monkeypatch.setattr("stream_processor.writers.get_es_client", lambda: fake_es)
    monkeypatch.setattr("stream_processor.writers.es_helpers.bulk", fake_bulk)

    # ============================
    # 3. Mock Kafka
    # ============================
    class FakeProducer:
        def __init__(self):
            self.sent = []
            self.flushed = False
        def send(self, topic, msg):
            self.sent.append((topic, msg))
        def flush(self, timeout=None):
            self.flushed = True

    fake_producer = FakeProducer()
    monkeypatch.setattr("stream_processor.writers.get_alert_producer", lambda: fake_producer)

    # ============================
    # 4. Load model
    # ============================
    model = PipelineModel.load("/models/anomaly_iforest")

    # ============================
    # 5. Tạo batch giả có anomaly
    # ============================
    data = [
        Row(window_start="2025-09-20 10:00:00",
            window_end="2025-09-20 10:01:00",
            user_id=303,
            event_count=888,
            action="click",
            user_name="Daisy",
            region="HCM")
    ]
    batch_df = spark.createDataFrame(data)

    users_df = spark.createDataFrame([
        Row(user_id=303, name="Daisy", region="HCM"),
    ])

    # ============================
    # 6. Run pipeline
    # ============================
    write_summary_with_model(batch_df, batch_id=123, window_type="1min", users_df=users_df, model=model)

    # ============================
    # 7. Assertions
    # ============================
    # Postgres
    assert any("INSERT INTO user_activity_summary" in sql for sql in pg_calls["sql"])
    assert any("INSERT INTO anomalous_events" in sql for sql in pg_calls["sql"])
    assert fake_conn.committed is True
    assert fake_conn.rolled_back is False

    # ES
    assert "docs" in captured_docs
    doc = captured_docs["docs"][0]["_source"]
    assert doc["type"] == "anomaly"
    assert doc["user_id"] == 303
    assert doc["user_name"] == "Daisy"
    assert doc["region"] == "HCM"

    # Kafka
    assert fake_producer.flushed is True
    assert any(topic == "anomaly_alerts" for topic, _ in fake_producer.sent)
    sent_msg = fake_producer.sent[0][1]
    assert sent_msg["user_id"] == 303
    assert sent_msg["severity"] == "model_detected"
