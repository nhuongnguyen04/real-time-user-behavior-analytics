import pytest
from pyspark.sql import Row
from pyspark.ml import PipelineModel
from stream_processor.writers import write_summary_with_model


@pytest.mark.usefixtures("mock_postgres", "mock_es", "mock_kafka")
def test_kafka_alert_message_content(spark, mock_postgres, mock_es, mock_kafka):
    # Load model đã train sẵn
    model = PipelineModel.load("/models/anomaly_iforest")

    # Tạo fake batch có anomaly
    data = [
        Row(window_start="2025-09-20 10:00:00",
            window_end="2025-09-20 10:01:00",
            user_id=999,
            event_count=999,     # deliberately large to trigger anomaly
            action="click",
            user_name="Alice",
            region="HN")
    ]
    batch_df = spark.createDataFrame(data)

    users_df = spark.createDataFrame([
        Row(user_id=999, name="Alice", region="HN"),
    ])

    # Clear mock calls trước khi chạy
    mock_kafka.reset_mock()

    # Gọi handler
    write_summary_with_model(batch_df, batch_id=42, window_type="5s", users_df=users_df, model=model)

    # Kiểm tra Kafka producer được gọi
    assert mock_kafka.send.called, "Kafka producer.send should have been called"
    assert mock_kafka.flush.called, "Kafka producer.flush should have been called"

    # Lấy args từ lần gọi đầu tiên
    topic_arg, msg_arg = mock_kafka.send.call_args[0]

    # Đảm bảo gửi đúng topic
    assert topic_arg == "anomaly_alerts"

    # Đảm bảo message có đủ field
    required_fields = {"user_id", "user_name", "region", "window_start", "window_end", "event_count", "severity", "detected_at"}
    assert required_fields.issubset(msg_arg.keys()), f"Missing fields in Kafka message: {msg_arg}"

    # Check nội dung hợp lý
    assert msg_arg["user_id"] == 999
    assert msg_arg["user_name"] == "Alice"
    assert msg_arg["region"] == "HN"
    assert msg_arg["event_count"] == 999
    assert msg_arg["severity"] == "model_detected"
    assert msg_arg["window_start"] == "2025-09-20 10:00:00"
    assert msg_arg["window_end"] == "2025-09-20 10:01:00"
    assert "detected_at" in msg_arg  # Chỉ cần đảm bảo có field này
    assert isinstance(msg_arg["detected_at"], str)  # Kiểm tra định dạng string
    # Có thể thêm kiểm tra định dạng timestamp nếu cần
    from datetime import datetime
    try:
        datetime.fromisoformat(msg_arg["detected_at"])
    except ValueError:
        pytest.fail(f"detected_at is not a valid ISO format: {msg_arg['detected_at']}")