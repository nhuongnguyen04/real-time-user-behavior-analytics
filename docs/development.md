# Hướng dẫn phát triển & chạy (Development)

Môi trường cần thiết
- Docker & Docker Compose (để khởi bản local Kafka, Postgres, ES, Schema Registry nếu có)
- Python 3.8+ (để chạy unit tests và các scripts hỗ trợ)
- Apache Spark 3.x cho các job streaming

Cài đặt nhanh (local dev)
```powershell
# 1. Start infra
docker-compose up -d

# 2. (Tuỳ chọn) Tạo venv và cài dependencies cho scripts / tests
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# 3. Kiểm tra Postgres schema
# Nếu dùng container postgres, chạy script init SQL (nếu docker-compose chưa thực hiện)
psql "postgresql://user:password@localhost:5432/realtime_db" -f ./postgres/init.sql
```

Chạy Spark job (containerized)
- Thông thường bạn sẽ submit job vào cluster hoặc dùng spark-submit kèm packages (vd. spark-avro)

Ví dụ (chạy local bằng spark-submit - cần Spark được cài):
```powershell
# ví dụ sử dụng spark-submit (nếu spark có sẵn trên system):
$SPARK_HOME\bin\spark-submit --packages org.apache.spark:spark-avro_2.12:3.3.2 \
  --py-files spark/spark_app \
  spark/spark_app/main.py
```

Gỡ lỗi & test
- Unit tests: (nếu repo có tests)
```powershell
pytest -q
```
- Đọc logs container liên quan (Kafka, Spark, Postgres, ES) để debug lỗi kết nối hoặc schema mismatch.

Các bước phát triển thường xuyên
1. Edit code trong `spark/spark_app/` (phân module theo config/db/es/kafka/validation/stream)
2. Viết unit test cho `validation.py` và cho các hàm thuần tuý (không phụ thuộc Spark) trước
3. Deploy container Spark / submit job và theo dõi checkpoint logs

Ghi chú về dependencies
- requirements.txt nên bao gồm tối thiểu: pyspark, kafka-python, elasticsearch, psycopg2-binary, requests, avro
- Spark submit cũng cần packages tương ứng (`spark-avro`) nếu xử lý Avro trong cluster
