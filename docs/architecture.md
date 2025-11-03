# Kiến trúc hệ thống - realtime-pipeline-demo

Tổng quan
- Pipeline hướng tới xử lý sự kiện realtime (user events) từ Kafka, dùng Spark Structured Streaming để validate, aggregate và phát hiện bất thường, sau đó lưu trữ vào Postgres, Elasticsearch và phát cảnh báo về Kafka.

Lưu lượng dữ liệu và đảm bảo
- Ingestion: Kafka làm đầu vào (topic `user_events`).
- Processing: Spark Structured Streaming (micro-batch theo `PROCESSING_TRIGGER`) hoặc có thể chuyển sang continuous nếu cần.
- Durability: Postgres để lưu dữ liệu thô & summary; Spark checkpoint để đảm bảo ứng dụng tái khởi động an toàn.
- Observability: Elasticsearch lưu logs và anomalies để dễ query/visualize.

Luồng dữ liệu chi tiết
1. Producer gửi JSON hoặc Avro message vào `user_events`.
2. Spark đọc Kafka, nếu Schema Registry + spark-avro có sẵn thì decode Avro bằng `from_avro`, ngược lại parse JSON string.
3. Các bản ghi được validate:
   - Nếu invalid → gửi vào DLQ topic (`dead_letter_topic`) cùng `dq_reason`.
   - Nếu valid → ghi thô vào Postgres (bảng `user_events`) và ghi raw Parquet (tuỳ cấu hình).
4. Aggregation windows (5s, 1min) thực hiện groupBy(window, user_id) → event_count.
5. Với mỗi batch windowed, pipeline:
   - Upsert summary vào `user_activity_summary`.
   - Tính statistics (mean, stddev) và z-score để phát hiện anomalies.
   - Ghi anomalies vào `anomalous_events` (INSERT ON CONFLICT DO NOTHING)
   - Ghi anomalies và batch summary vào ES và gửi alert vào `anomaly_alerts` (Kafka).

Component interactions
- Spark ↔ Kafka: Spark Structured Streaming Source/Sink
- Spark ↔ Postgres: JDBC / psycopg2 (COPY for bulk insert - optional) để lưu data
- Spark ↔ Elasticsearch: elasticsearch-py (helpers.bulk)
- Spark ↔ Schema Registry: HTTP call để fetch Avro schema (khi decode messages)

Scaling và hiệu năng
- Tăng `spark.sql.shuffle.partitions` và số worker tuỳ throughput.
- Ghi Postgres hiệu năng: dùng COPY (psycopg2.copy_from) cho throughput cao.
- ES bulk size: cấu hình `ES_BULK_CHUNK` để tối ưu bulk indexing.

Fault-tolerance & Exactly-once
- Spark checkpoint: `CHECKPOINT_BASE` cho mỗi stream sink để đảm bảo restart idempotency.
- Postgres upsert / ON CONFLICT tránh duplicate (thực hiện idempotent writes cho summaries/anomalies).

Bảo mật
- Đảm bảo credentials (DB password, ES, SR) quản lý bằng secrets manager / Vault chứ không commit vào repo.
