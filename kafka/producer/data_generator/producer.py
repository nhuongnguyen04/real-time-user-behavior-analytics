import time
import random
import os
from datetime import datetime
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient
from confluent_kafka.avro import loads

BROKER = os.getenv("KAFKA_BROKER", "localhost:9093")
SCHEMA_REGISTRY = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC = "user_events"

schema_registry_client = CachedSchemaRegistryClient({'url': SCHEMA_REGISTRY})

schema_schema_str = """
{
  "namespace": "user.events",
  "type": "record",
  "name": "UserEvent",
  "fields": [
    {"name": "user_id", "type": "int"},
    {"name": "product_id", "type": "int"},
    {"name": "action", "type": "string"},
    {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"},
    {"name": "device_id", "type": ["null", "string"], "default": null},
    {"name": "device_type", "type": ["null", "string"], "default": null},
    {"name": "location", "type": ["null", "string"], "default": null},
    {"name": "user_segment", "type": ["null", "string"], "default": null},
    {"name": "ip_address", "type": ["null", "string"], "default": null}
  ]
}
"""

producer_config = {
    'bootstrap.servers': BROKER,
    'schema.registry.url': SCHEMA_REGISTRY,
    'compression.type': 'snappy',
    'retries': 5
}

try:
    producer = AvroProducer(
        producer_config,
        default_value_schema=loads(schema_schema_str)
    )
except Exception as e:
    print(f"Lỗi khi khởi tạo AvroProducer: {e}")
    exit(1)

actions = ['view', 'click', 'add_to_cart', 'purchase']
user_ids = list(range(1, 1001))  # Giả sử có 1000 người dùng
product_ids = list(range(1000, 2000))  # Giả sử có 1000 sản phẩm
device_types = ['mobile', 'desktop', 'tablet', 'smart_tv', 'wearable', 'other']
locations = ['Hanoi', 'Ho Chi Minh City', 'Da Nang', 'Hue', 'Can Tho', 'Nha Trang', 'other']
user_segments = ['new', 'loyal', 'promo_hunter', 'high_value', 'other']

def generate_ip():
    return ".".join(str(random.randint(0, 255)) for _ in range(4))

SPAM_USERS = [10, 455, 789, 123, 999]  # Giả sử đây là những người dùng spammer

def generate_event():
    is_spammer = random.random() < 0.4  # 40% xác suất là spammer
    user_id = random.choice(SPAM_USERS) if is_spammer else random.choice(user_ids)
     # Chuyển timestamp sang kiểu long (mili giây)
    ts = int(datetime.utcnow().timestamp() * 1000)
    return {
        'user_id': user_id,
        'product_id': random.choice(product_ids),
        'action': 'click' if is_spammer else random.choice(actions),
        'timestamp': ts,
        'device_id': f"device_{random.randint(10000,99999)}",
        'device_type': random.choice(device_types),
        'location': random.choice(locations),
        'user_segment': 'promo_hunter' if is_spammer else random.choice(user_segments),
        'ip_address': generate_ip()
    }
batch_size = 1000
batch = []
start_time = time.time()
while True:
    try:
        batch.append(generate_event())
        if len(batch) >= batch_size:
            for event in batch:
                producer.produce(topic=TOPIC, value=event)
            producer.flush()
            elapsed = time.time() - start_time
            print(f"Đã gửi {len(batch)} sự kiện trong {elapsed:.2f}s giây (~{len(batch) / elapsed:.2f} sự kiện/giây)")
            batch = []
            start_time = time.time()
            time.sleep(0.1)
    except Exception as e:
        print(f"Lỗi khi gửi message: {e}")
    time.sleep(0.1)