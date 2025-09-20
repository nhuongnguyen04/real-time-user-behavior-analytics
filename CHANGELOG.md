# Changelog

## [Unreleased]

### Refactor
- **stream-processor**: Tách `stream_processor.py` thành các module độc lập
  - `config.py`: gom cấu hình từ ENV
  - `helpers.py`: chứa kết nối Postgres, Elasticsearch, Kafka, schema registry, COPY util
  - `validation.py`: validate dữ liệu & đẩy record lỗi vào DLQ
  - `writers.py`: ghi dữ liệu vào Postgres/ES, xử lý anomaly, gửi alert Kafka
  - `main.py`: giữ orchestration Spark → Kafka → xử lý → sink
  - `run_job.sh`: script `spark-submit` tiện dụng
- Cải thiện khả năng bảo trì, mở rộng và testability.
- Tối ưu hiệu năng: 
  - sử dụng `mapPartitions` khi ghi Postgres
  - `broadcast join` với bảng users
  - `bulk indexing` cho Elasticsearch