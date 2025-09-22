from kafka import KafkaConsumer
import json

BROKERS = ["kafka1:19092", "kafka2:19093", "kafka3:19094"]
TOPIC = "fly-topic"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BROKERS,
    auto_offset_reset='earliest',  # lire depuis le début
    enable_auto_commit=True,
    group_id='flight-consumer-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("📖 Reading all messages from fly-topic...\n")

for msg in consumer:
    flight = msg.value
    flight_code = flight.get("flight", {}).get("iata", "N/A")
    
    # sécuriser "live" et "is_ground"
    live_info = flight.get("live") or {}
    is_ground = live_info.get("is_ground", True)
    
    status = "En vol" if is_ground == False else "Au sol"
    
    print(f"✈️ Flight {flight_code}, Status: {status}")
