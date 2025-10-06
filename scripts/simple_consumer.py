import json
from kafka import KafkaConsumer

# Configuration
KAFKA_SERVER = 'localhost:9092'
TOPIC_NAME = 'raw-vitals'

# Create Kafka consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_SERVER],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

def main():
    print("Starting Kafka Consumer...")
    print("Press Ctrl+C to stop")
    print("-" * 50)
    
    try:
        for message in consumer:
            data = message.value
            print(f"Received data:")
            print(f"  Patient: {data['patient_id']}")
            print(f"  Heart Rate: {data['heart_rate']} bpm")
            print(f"  SpO2: {data['oxygen_saturation']}%")
            print(f"  BP: {data['blood_pressure_systolic']}/{data['blood_pressure_diastolic']}")
            print(f"  Time: {data['timestamp']}")
            print("-" * 30)
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()