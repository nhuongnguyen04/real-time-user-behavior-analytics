# realtime-pipeline-demo

Tài liệu ngắn gọn cho dự án Real-time Pipeline (Kafka → Spark Streaming → Postgres / Elasticsearch / Kafka alerts)

Mục đích
- Demo một pipeline realtime đọc sự kiện người dùng từ Kafka, xử lý (validate, windowed aggregation), ghi dữ liệu thô vào Postgres, ghi log/bulk vào Elasticsearch, gửi cảnh báo tới Kafka, và lưu summary/anomalies vào Postgres.

Thành phần chính
- Kafka: topic `user_events` (người dùng gửi sự kiện)
- Spark Structured Streaming: xử lý stream, validate, windowed aggregation, anomaly detection
- Postgres: lưu `user_events`, `user_activity_summary`, `anomalous_events`, bảng `users` để enrich
- Elasticsearch: chỉ mục `user_event_logs` để ghi logs, anomalies
- Schema Registry (tuỳ chọn): chứa Avro schema cho messages

Cấu trúc mã nguồn (tệp quan trọng)
- `spark/spark_app/stream.py` - entrypoint pipeline, read Kafka → parse → validate → write (PG/ES/DLQ/Alerts) và windowed aggregation
- `spark/spark_app/validation.py` - Định nghĩa `validate_df` (data quality)
- `spark/spark_app/schema.py` - Lấy schema từ Schema Registry
- `spark/spark_app/db.py` - helper kết nối Postgres
- `spark/spark_app/es.py` - helper ES client
- `spark/spark_app/kafka_utils.py` - Kafka producer helper (alerts & DLQ)
- `spark/spark_app/config.py` - biến môi trường và cấu hình
- `spark/spark_app/main.py` - runner
- `postgres/init.sql` - schema DB khởi tạo (cần kiểm tra đầy đủ trong thư mục `postgres`)
- Các file khác: `spark/spark_apps/` chứa app phụ trợ (stream_processor, ML)

Các bảng Postgres (tóm tắt)
- `users` - bảng danh sách người dùng (dùng để enrich)
- `user_events` - sự kiện thô (các cột: id, user_id, product_id, action, timestamp, device_id, device_type, location, user_segment, ip_address, ...)
- `user_activity_summary` - summary theo window (window_start, window_end, user_id, event_count)
- `anomalous_events` - events được đánh dấu bất thường

Biến môi trường (quan trọng — có thể chỉnh trong `docker-compose` hoặc runtime):
- KAFKA_BROKER (mặc định: `kafka:9092`)
- KAFKA_TOPIC (mặc định: `user_events`)
- KAFKA_ALERT_TOPIC (mặc định: `anomaly_alerts`)
- KAFKA_DLQ_TOPIC (mặc định: `dead_letter_topic`)
- SCHEMA_REGISTRY_URL, SCHEMA_SUBJECT
- POSTGRES_URL (form `jdbc:postgresql://host:port/dbname`), POSTGRES_USER, POSTGRES_PASSWORD
- ES_HOST, ES_INDEX
- CHECKPOINT_BASE (Spark streaming checkpoint path)
- ANOMALY_THRESHOLD, Z_THRESHOLD
- COPY_CHUNK, ES_BULK_CHUNK
- PROCESSING_TRIGGER, WINDOW_SLIDE, etc.

Chạy local (Docker)
1. Khởi động dịch vụ (trong Windows PowerShell):

```powershell
# từ thư mục repo
docker-compose up -d
```

2. Chạy job Spark (ví dụ dùng spark-submit trong container Spark):
- Nếu dùng image sẵn có trong `docker-compose`, kiểm tra service `spark`/`spark-submit` config trong `docker-compose.yml`.

3. Đọc logs / debug
- Logs Spark: xem container `spark` hoặc log file
- Logs Kafka / Schema Registry / ES: xem từng container tương ứng

Chạy module Python (local dev)
- Để chạy `spark/spark_app/main.py` trực tiếp cho mục đích dev (không khuyến nghị cho production):

```powershell
# đảm bảo Python env có pyspark, kafka-python, elasticsearch, psycopg2-binary, requests
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m spark.spark_app.main
```

Lưu ý: pipeline được thiết kế để chạy trong môi trường Spark; chạy trực tiếp bằng CPython chỉ phù hợp cho unit-test nhỏ.

Testing
- Thư mục `spark/spark_app/tests/` chứa các test PyTest (nếu có). Dùng `pytest` để chạy.

Những điểm cần kiểm tra/hoàn thiện
- Xác thực file `postgres/init.sql` (trong thư mục `postgres`) để đảm bảo các constraint (unique) và tên cột khớp với code.
- Quyết định con đường xử lý Avro: dùng `from_avro` (yêu cầu spark-avro) hoặc dùng JSON fallback. Nếu sử dụng Schema Registry và Avro, đảm bảo spark-submit có `--packages org.apache.spark:spark-avro_2.12:3.x.x`.
- Hiệu năng ghi Postgres: code modular cung cấp `execute_values` insert; monolithic script có phương thức COPY cho throughput cao — cân nhắc phục hồi COPY nếu cần.
- Kiểm tra cài đặt KafkaProducer trong `kafka_utils.py` (kafka-python) có tương thích với cluster Kafka đang dùng.

Tài liệu thêm
- `docs/` chứa thông tin chi tiết về kiến trúc và phát triển (nếu có). Nếu bạn muốn, mình sẽ tạo thêm các file: `docs/architecture.md`, `docs/development.md`.

Liên hệ
- Nếu cần mình có thể:
  - Tạo tài liệu chi tiết hơn (quy trình dev, kịch bản test, sơ đồ kiến trúc)
  - Áp các sửa đổi code nhỏ để đồng bộ module với script gốc (ví dụ: bật fallback Avro, thêm COPY-based writes, chuẩn hoá logging)

---
Cần mình tạo thêm file docs (kiến trúc, hướng dẫn dev) không? Nếu có, cho mình biết bạn muốn tiếng Việt hay tiếng Anh, và mức chi tiết (ngắn/đầy đủ).