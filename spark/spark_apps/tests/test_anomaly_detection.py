# tests/test_validation.py (Sửa tên file từ test_valiadation.py, thêm cast, test multiple reasons)
from datetime import datetime
from pyspark.sql.types import TimestampType
from stream_processor.validation import validate_df

def test_validate_df_basic(spark):
    data = [
        (1, 2, "click", datetime(2025, 9, 20, 10, 0, 0), "HN"),  # Valid
        (None, 2, "click", datetime(2025, 9, 20, 10, 0, 0), "HN"),  # Invalid user_id
        (3, -1, "invalid_action", datetime(2025, 9, 20, 10, 0, 0), "too_long_location" * 20),  # Multiple invalid
    ]
    schema = ["user_id", "product_id", "action", "event_time", "location"]
    df = spark.createDataFrame(data, schema)

    valid_df, invalid_df = validate_df(df)

    valid_rows = valid_df.collect()
    invalid_rows = invalid_df.collect()

    assert len(valid_rows) == 1
    assert len(invalid_rows) == 2
    assert "invalid_user_id" in invalid_rows[0]["dq_reason"]
    assert all(reason in invalid_rows[1]["dq_reason"] for reason in ["invalid_product_id", "invalid_action", "location_too_long"])