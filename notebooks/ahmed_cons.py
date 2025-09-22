from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "fly-topic",
    bootstrap_servers=["kafka1:19092", "kafka2:19093", "kafka3:19094"],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='flight-consumer-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None
)

for msg in consumer:
    if msg.value is None:
        continue  # ignorer les messages vides
    flight = msg.value
    flight_code = flight.get("flight", {}).get("iata", "N/A")
    live_info = flight.get("live") or {}
    is_ground = live_info.get("is_ground", True)
    status = "En vol" if is_ground == False else "Au sol"
    print(f"✈️ Flight {flight_code}, Status: {status}")