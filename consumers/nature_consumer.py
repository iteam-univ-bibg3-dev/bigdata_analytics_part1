#!/usr/bin/env python3
"""
🌍 Kafka → Elasticsearch Ingestor
Consumes multi-source nature events from Kafka,
normalizes them (timestamp, geometry), and indexes into Elasticsearch.
"""

import time
import json
import random
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from elasticsearch import Elasticsearch, helpers, exceptions

# -------- CONFIG --------
BROKERS = ["kafka1:9092", "kafka2:9093", "kafka3:9094"]
TOPIC = "nature_data"
ES_HOST = "http://elasticsearch:9200"
ES_INDEX = "nature_data"

BATCH_SIZE = 50
POLL_TIMEOUT = 5

# -------- UTILS --------
def to_millis(ts):
    """Convert seconds or milliseconds to epoch_millis (int)."""
    if ts is None:
        return int(time.time() * 1000)
    ts = float(ts)
    return int(ts * 1000) if ts < 1e12 else int(ts)

def safe_float(value, default=None):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

def colorize(type_name):
    """Colorize log output by event type."""
    colors = {
        "floods": "\033[94m",       # Blue
        "wildfires": "\033[91m",    # Red
        "storms": "\033[96m",       # Cyan
        "earthquakes": "\033[93m",  # Yellow
        "volcanoes": "\033[95m",    # Magenta
        "satellite": "\033[92m",    # Green
    }
    return colors.get(type_name, "\033[0m")

RESET = "\033[0m"

# -------- INIT KAFKA --------
for i in range(10):
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BROKERS,
            auto_offset_reset="earliest",
            group_id=f"nature_data-group-{int(time.time())}",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        print("Connected to Kafka brokers.")
        break
    except NoBrokersAvailable:
        print(f"Kafka not ready, retrying {i+1}/10...")
        time.sleep(5)
else:
    raise RuntimeError("Could not connect to Kafka after 10 retries.")

# -------- INIT ELASTICSEARCH --------
es = Elasticsearch([ES_HOST])
for i in range(10):
    try:
        if es.ping():
            print("Connected to Elasticsearch.")
            break
    except exceptions.ConnectionError:
        pass
    print(f"Elasticsearch not ready, retrying {i+1}/10...")
    time.sleep(5)
else:
    raise RuntimeError("Cannot connect to Elasticsearch")

print("Starting ingestion loop...")

# -------- INDEXING --------
def index_batch(events):
    if not events:
        return

    actions = []
    for event in events:
        event_type = event.get("type", "unknown")
        event["timestamp"] = to_millis(event.get("timestamp"))
        event["latitude"] = safe_float(event.get("latitude"))
        event["longitude"] = safe_float(event.get("longitude"))
        event["altitude_km"] = safe_float(event.get("altitude_km"), 0)
        event["velocity_kms"] = safe_float(event.get("velocity_kms"), 0)

        # ✅ Ensure geometry field exists
        if "geometry" not in event and event["latitude"] is not None and event["longitude"] is not None:
            event["geometry"] = {
                "lat": event["latitude"],
                "lon": event["longitude"]
            }

        # Unique ID
        doc_id = event.get("id") or f"{event.get('source', 'unknown')}-{int(time.time() * 1000)}-{random.randint(0, 9999)}"

        actions.append({
            "_index": ES_INDEX,
            "_id": doc_id,
            "_source": event
        })

        color = colorize(event_type)
        print(f"{color}Indexing {event_type:<12} | lat={event['latitude']} lon={event['longitude']} ts={event['timestamp']}{RESET}")

    try:
        helpers.bulk(es, actions, raise_on_error=False)
        print(f"Indexed batch of {len(events)} events\n")
    except Exception as ex:
        print("Elasticsearch bulk error:", ex)

# -------- CONSUME LOOP --------
batch = []
last_flush = time.time()

try:
    while True:
        records = consumer.poll(timeout_ms=1000, max_records=BATCH_SIZE)
        for partition_records in records.values():
            for record in partition_records:
                event = record.value
                batch.append(event)

        # Flush batch
        if batch and (len(batch) >= BATCH_SIZE or time.time() - last_flush >= POLL_TIMEOUT):
            index_batch(batch)
            batch = []
            last_flush = time.time()

except KeyboardInterrupt:
    print("\n Interrupted, flushing remaining events...")
finally:
    if batch:
        index_batch(batch)
    consumer.close()
    print(" Consumer closed.")
