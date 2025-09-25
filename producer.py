# KAFKA PRODUCER 

import os, time, json, logging, requests
from kafka import KafkaProducer
from datetime import datetime

logging.basicConfig(level=logging.INFO)
KAFKA = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "football-events"
API_KEY = os.getenv("FOOTBALL_API_KEY")  #set in .env_api
BASE = "https://v3.football.api-sports.io"

if not API_KEY:
    raise SystemExit("Set FOOTBALL_API_KEY env var (x-apisports-key)")

producer = KafkaProducer(
    bootstrap_servers=KAFKA,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    retries=5,
    compression_type="gzip",
    linger_ms=50
)

HEADERS = {
    "x-apisports-key": API_KEY,
    "x-rapidapi-host": "v3.football.api-sports.io"
}

def fetch_live_fixtures():
    # many plans: live param returns live matches (if any)
    url = f"{BASE}/fixtures?live=all"  # returns live fixtures; if none, empty
    r = requests.get(url, headers=HEADERS, timeout=10)
    r.raise_for_status()
    obj = r.json()
    # API returns {"get":"fixtures", "results":X, "response":[...]}
    return obj.get("response", [])

def build_message(fixture):
    # normalize fields
    return {
        "fixture": fixture.get("fixture", {}),
        "league": fixture.get("league", {}),
        "teams": fixture.get("teams", {}),
        "goals": fixture.get("goals", {}),
        "score": fixture.get("score", {}),
        "events": fixture.get("events", []),  # may be empty
        "timestamp": int(time.time()*1000),
        "source": "api-football"
    }

def main(poll=10):
    try:
        while True:
            fixtures = fetch_live_fixtures()
            now = datetime.utcnow().isoformat()
            if fixtures:
                for f in fixtures:
                    key = str(f.get("fixture", {}).get("id", "unknown"))
                    msg = build_message(f)
                    producer.send(TOPIC, key=key, value=msg)
                producer.flush()
                logging.info(f"[{now}] Produced {len(fixtures)} live fixtures to {TOPIC}")
            else:
                logging.info(f"[{now}] No live fixtures found")
            time.sleep(poll)
    except KeyboardInterrupt:
        logging.info("Stopped by user")
    except Exception as e:
        logging.exception("Producer error")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
