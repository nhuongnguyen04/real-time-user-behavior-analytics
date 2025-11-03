# tests/test_writers_postgres.py (Sửa: Thêm test rollback case)
import pytest
from pyspark.sql import Row
from datetime import datetime
from pyspark.ml import PipelineModel
from stream_processor.writers import write_summary_with_model

@pytest.mark.usefixtures("mock_model")
def test_postgres_upsert_and_anomaly_insert(spark, monkeypatch, mock_model):
    model = mock_model

    # Mock Postgres connection
    executed_sql = {}
    executed_values = {}

    class FakeCursor:
        def __init__(self):
            self.executed = []

        def execute(self, sql, params=None):
            executed_sql["last"] = sql
            executed_values["last"] = params
            self.executed.append(sql)

    class FakeConn:
        def __init__(self):
            self.cur = FakeCursor()
            self.committed = False
            self.rolled_back = False

        def cursor(self):
            return self.cur

        def commit(self):
            self.committed = True

        def rollback(self):
            self.rolled_back = True

        def close(self):
            pass

    fake_conn = FakeConn()
    monkeypatch.setattr("stream_processor.writers.get_psycopg2_conn", lambda: fake_conn)

    # Fake batch có anomaly
    data = [
        Row(window_start=datetime(2025, 9, 20, 10, 0, 0),
            window_end=datetime(2025, 9, 20, 10, 1, 0),
            user_id=202,
            event_count=1000,
            action="click",
            user_name="Charlie",
            region="DN")
    ]
    batch_df = spark.createDataFrame(data)

    users_df = spark.createDataFrame([
        Row(user_id=202, name="Charlie", region="DN"),
    ])

    # Run handler
    write_summary_with_model(batch_df, batch_id=77, window_type="1min", users_df=users_df, model=model)

    # Kiểm tra Postgres được gọi ít nhất 1 lần
    assert "last" in executed_sql, "No SQL executed on Postgres"

    last_sql = executed_sql["last"]
    assert "INSERT INTO anomalous_events" in last_sql or "INSERT INTO user_activity_summary" in last_sql

    # Kiểm tra commit được gọi
    assert fake_conn.committed is True
    assert fake_conn.rolled_back is False

@pytest.mark.usefixtures("mock_model")
def test_postgres_rollback_on_error(spark, monkeypatch, mock_model):
    model = mock_model

    # Mock Postgres with exception
    class FakeCursor:
        def execute(self, sql, params=None):
            raise Exception("Simulated DB error")

    class FakeConn:
        def __init__(self):
            self.cur = FakeCursor()
            self.committed = False
            self.rolled_back = False

        def cursor(self):
            return self.cur

        def commit(self):
            self.committed = True

        def rollback(self):
            self.rolled_back = True

        def close(self):
            pass

    fake_conn = FakeConn()
    monkeypatch.setattr("stream_processor.writers.get_psycopg2_conn", lambda: fake_conn)

    # Fake batch
    data = [Row(window_start=datetime(2025, 9, 20, 10, 0, 0), window_end=datetime(2025, 9, 20, 10, 1, 0), user_id=202, event_count=1000, action="click", user_name="Charlie", region="DN")]
    batch_df = spark.createDataFrame(data)
    users_df = spark.createDataFrame([Row(user_id=202, name="Charlie", region="DN")])

    # Run and expect no crash, but rollback
    write_summary_with_model(batch_df, batch_id=77, window_type="1min", users_df=users_df, model=model)

    assert fake_conn.rolled_back is True
    assert fake_conn.committed is False