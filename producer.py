# KAFKA PRODUCER

import time
import json
import requests
from kafka import KafkaProducer
from datetime import datetime
from datetime import timezone

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "football.raw_events"
LIVE_SCORE_API_URL = "https://livescore-api.com/api-client/scores/live.json"
API_KEY = "n43jTG7tnHWSmTdm"
API_SECRET = "XutWaHPASvvUOW1c15wVLDo9kk1Hef4b"


def create_producer():
    """Initialise KafkaProducer avec gestion de reconnexion."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
            )
            print("[INFO] **** KafkaProducer connecté avec succès.****")
            return producer
        except Exception as e:
            print(f"[ERREUR] Kafka non disponible ({e}), nouvelle tentative dans 5s...")
            time.sleep(5)


def live_scores():
    """Récupère les scores en direct via l’API Live Score."""
    params = {"key": API_KEY, "secret": API_SECRET}
    try:
        response = requests.get(LIVE_SCORE_API_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("data", {}).get("match", [])
    except Exception as e:
        print(f"[ERREUR] Impossible de récupérer les scores : {e}")
        return []


def publish_matches(producer, matches):
    """Envoie la liste des matchs à Kafka."""
    for match in matches:
        match_id = match.get("id") or f"{match.get('home_name','home')}_{match.get('away_name','away')}"
        message = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "live-score-api",
            "match_id": str(match_id),
            "competition": match.get("competition", {}).get("name"),
            "home_team": match.get("home_name"),
            "away_team": match.get("away_name"),
            "home_score": match.get("home_score"),
            "away_score": match.get("away_score"),
            "status": match.get("status"),
            #"events_count": match.get("events", []),
        }
        producer.send(KAFKA_TOPIC, value=message)

    producer.flush()
    print(f"[INFO] {len(matches)} matchs envoyés à Kafka.")


def main():
    producer = create_producer()

    while True:
        matches = live_scores()
        if matches:
            publish_matches(producer, matches)
        else:
            print("[INFO] Aucun match en direct pour le moment.")

        time.sleep(30)


if __name__ == "__main__":
    main()
