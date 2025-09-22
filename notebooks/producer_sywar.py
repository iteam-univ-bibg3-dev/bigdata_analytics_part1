import json
import requests
import time
from kafka import KafkaProducer  # ✅ Fixed import

API_URL = "https://api.aviationstack.com/v1/flights"
API_KEY = "7cdae43a6755c4d4f67428e8b919b3ed"
TOPIC = "fly_sywar"

# ✅ Correct bootstrap_servers format
producer = KafkaProducer(
    bootstrap_servers=["kafka1:19092", "kafka2:19093", "kafka3:19094"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_flights(limit=5):
    params = {"access_key": API_KEY, "limit": limit}
    try:
        response = requests.get(API_URL, params=params, timeout=10)
        response.raise_for_status()
        return response.json().get("data", [])
    except Exception as e:
        print(f"❌ API Error: {e}")
        return []

while True:
    flights = fetch_flights(limit=5)
    if flights:
        for flight in flights:
            producer.send(TOPIC, flight)
            print(f"✅ Sent flight: {flight.get('flight', {}).get('iata', 'N/A')}")
        producer.flush()  # ✅ ensure messages are sent
    time.sleep(10)
