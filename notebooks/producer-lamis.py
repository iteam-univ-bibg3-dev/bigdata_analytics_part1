import json
import time
import requests
from kafka import KafkaProducer

API_URL = "https://api.aviationstack.com/v1/flights"
API_KEY = "11fbedc1bc8d202c05db4f395dca9fca" 
TOPIC = "fly-lamis"

producer = KafkaProducer(
    bootstrap_servers=["kafka1:19092","kafka2:19093","kafka3:19094"],
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
    time.sleep(10)
