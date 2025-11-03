# tests/test_validation.py
from stream_processor.validation import validate_df

def test_validate_df_basic(spark):
    data = [
        {"user_id": 1, "product_id": 2, "action": "click", "event_time": "2025-09-20 10:00:00", "location": "HN"},
        {"user_id": None, "product_id": 2, "action": "click", "event_time": "2025-09-20 10:00:00", "location": "HN"},
    ]
    df = spark.createDataFrame(data)

    valid_df, invalid_df = validate_df(df)

    valid_rows = [r.asDict() for r in valid_df.collect()]
    invalid_rows = [r.asDict() for r in invalid_df.collect()]

    assert len(valid_rows) == 1
    assert len(invalid_rows) == 1
    assert "invalid_user_id" in invalid_rows[0]["dq_reason"]
