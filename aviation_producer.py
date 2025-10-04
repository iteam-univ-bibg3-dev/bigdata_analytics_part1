import requests
import json
import time
from kafka import KafkaProducer
import schedule

API_KEY = "ae5b7b8ef3bcf49099bd097241beddcb"
API_URL = f"http://api.aviationstack.com/v1/flights?access_key={API_KEY}"

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'aviation_flights'


def fetch_and_send():
    try:
        response = requests.get(API_URL)
        data = response.json()
        
        # Vérification basique
        if "data" in data:
            for flight in data["data"][:5]:  # on prend seulement 5 vols pour le test
                producer.send(TOPIC_NAME, flight)
                print(f" Message envoyé: {flight.get('flight', {}).get('iata', 'N/A')}")
        else:
            print(" Pas de données reçues.")
    except Exception as e:
        print(f" Erreur: {e}")


schedule.every(15).seconds.do(fetch_and_send)
print("Démarrage du producteur Kafka pour les données de vol...")
while True:
    schedule.run_pending()
    time.sleep(1)
