
import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "stock-prices",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda message: json.loads(
        message.decode("utf-8")
    ),
    consumer_timeout_ms=15000,
)

output_file = "stock_prices_raw.jsonl"
count = 0

with open(output_file, "a", encoding="utf-8") as file:
    try:
        for message in consumer:
            file.write(json.dumps(message.value) + "\n")
            file.flush()
            count += 1
            print(f"Saved record {count}: {message.value}")
    finally:
        consumer.close()

print(f"Finished. Saved {count} records to {output_file}")
