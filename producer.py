from kafka import KafkaProducer
import json
import time
import random

# Список магазинов
stores = ["store-1", "store-2", "store-3"]

# Создаём продюсера
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("🚀 Продюсер запущен. Отправка сообщений...")

try:
    while True:
        # Генерируем событие
        event = {
            "store": random.choice(stores),
            "item": f"product-{random.randint(1, 10)}",
            "price": round(random.uniform(10, 100), 2),
            "ts": int(time.time())  # timestamp
        }
        print(f"📤 Отправлено: {event}")

        # Отправляем в топик 'sales'
        producer.send("sales", key=event["store"].encode(), value=event)

        time.sleep(1)  # одно сообщение в секунду
except KeyboardInterrupt:
    print("\n🛑 Продюсер остановлен.")
    producer.close()