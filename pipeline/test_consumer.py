from kafka import KafkaConsumer
import json

print("Connecting to Kafka...")

consumer = KafkaConsumer(
    "transactions-raw",
    bootstrap_servers="127.0.0.1:9092",
    group_id="test-group",
    auto_offset_reset="earliest",
    consumer_timeout_ms=5000,  # exit after 5s if no messages
)

print("Connected! Reading messages...")

count = 0
for message in consumer:
    data = json.loads(message.value)
    print(f"Got: {data['event_id']} | customer: {data['customer']['customer_id']}")
    count += 1
    if count >= 5:
        break

print(f"Done — read {count} messages")
consumer.close()