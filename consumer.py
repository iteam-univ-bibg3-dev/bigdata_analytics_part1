# KAFKA CONSUMER

import os, json, logging
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch, helpers

logging.basicConfig(level=logging.INFO)
KAFKA = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
ES_URL = os.getenv("ES_URL", "http://localhost:9200")

consumer = KafkaConsumer(
    'football-events',
    bootstrap_servers=KAFKA,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    consumer_timeout_ms=1000
)

es = Elasticsearch([ES_URL])
actions = []
BULK_SIZE = 200

for msg in consumer:
    doc = msg.value
    fixture_id = doc.get("fixture", {}).get("id", "unknown")
    doc_id = f"{fixture_id}_{doc.get('timestamp')}"
    actions.append({
        "_index": "football_raw",
        "_id": doc_id,
        "_source": doc
    })
    if len(actions) >= BULK_SIZE:
        helpers.bulk(es, actions)
        logging.info(f"Indexed {len(actions)} docs to ES")
        actions = []

# flush remaining
if actions:
    helpers.bulk(es, actions)
    logging.info("Flushed remaining docs")
consumer.close()
