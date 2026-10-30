import json
import time
import pandas as pd
from datetime import datetime, timezone
from kafka import KafkaProducer

# Configuration
CSV_PATH = "data/indexProcessed.csv"
KAFKA_TOPIC = "stock-prices"
BOOTSTRAP_SERVERS = "localhost:9092"

# Load historical dataset
df = pd.read_csv(CSV_PATH)

df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
df = df.dropna(subset=["Index", "Date"])
df = df.sort_values(["Date", "Index"])

print("Total records:", len(df))
print("Unique indices:", df["Index"].nunique())

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    acks="all",
    retries=5
)

# Publish records to Kafka
sent = 0

try:
    for row in df.to_dict(orient="records"):

        row["Date"] = row["Date"].strftime("%Y-%m-%d")
        row["source"] = "historical_csv"
        row["ingested_at"] = datetime.now(timezone.utc).isoformat()

        producer.send(KAFKA_TOPIC, value=row)
        sent += 1

        if sent % 1000 == 0:
            producer.flush()
            print(f"Published {sent} records")

    producer.flush()

finally:
    producer.close()

print(f"\nSuccessfully published {sent} records to Kafka!")