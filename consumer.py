# KAFKA CONSUMER (Kafka → Elasticsearch)
import os
import json
import time
import logging
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch, helpers

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "football.raw_events")
ES_URL = os.getenv("ES_URL", "http://localhost:9200")
ES_INDEX = os.getenv("ES_INDEX", "football_events")

# --- Initialisation Elasticsearch ---
logging.info(f"Connexion à Elasticsearch ({ES_URL})...")
for _ in range(10):  # retry jusqu'à 10 fois
    try:
        es = Elasticsearch([ES_URL])
        if es.ping():
            logging.info("****Connecté à Elasticsearch")
            break
    except Exception as e:
        logging.warning(f"Elasticsearch non prêt ({e}), nouvelle tentative...")
        time.sleep(5)
else:
    logging.error("!!!! Impossible de se connecter à Elasticsearch.")
    exit(1)

# --- Initialisation Kafka Consumer ---
logging.info(f"Connexion à Kafka ({KAFKA}), topic: {TOPIC}")
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='football-consumer-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    consumer_timeout_ms=1000
)

logging.info(" !!!! Consumer démarré, en attente de messages...")

# --- Traitement ---
actions = []
BULK_SIZE = 200

try:
    for message in consumer:
        data = message.value

        # Nettoyage des données
        if 'events' in data:
            del data['events']

        if 'fixture_id' not in data:
            data['fixture_id'] = data.get('status', 'unknown')

        # Ajout à la liste d’indexation
        actions.append({
            "_index": ES_INDEX,
            "_source": data
        })

        logging.info(f" ** Message reçu - fixture_id: {data.get('fixture_id', 'unknown')}")

        # Indexation par batch
        if len(actions) >= BULK_SIZE:
            helpers.bulk(es, actions)
            logging.info(f"Indexed {len(actions)} documents dans {ES_INDEX}")
            actions = []

except KeyboardInterrupt:
    logging.info(" Arrêt manuel du consumer.")
except Exception as e:
    logging.error(f"Erreur inattendue: {e}")
finally:
    if actions:
        helpers.bulk(es, actions)
        logging.info(f"Indexed les {len(actions)} derniers documents restants.")
    consumer.close()
    logging.info("Consumer fermé proprement.")
