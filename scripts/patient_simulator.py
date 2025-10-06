import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Configuration
KAFKA_SERVER = 'localhost:9092'
TOPIC_NAME = 'raw-vitals'

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def generate_patient_data(patient_id):
    """Generate fake patient vital signs"""
    return {
        'patient_id': patient_id,
        'timestamp': datetime.now().isoformat(),
        'heart_rate': random.randint(60, 100),
        'blood_pressure_systolic': random.randint(110, 140),
        'blood_pressure_diastolic': random.randint(70, 90),
        'oxygen_saturation': random.randint(95, 100),
        'respiratory_rate': random.randint(12, 20),
        'temperature': round(random.uniform(36.0, 38.0), 1)
        
    }

def main():
    patient_ids = [f"patient_{i:03d}" for i in range(1, 6)]  # 5 patients
    
    print("Starting patient data simulation...")
    print("Press Ctrl+C to stop")
    
    try:
        while True:
            for patient_id in patient_ids:
                data = generate_patient_data(patient_id)
                producer.send(TOPIC_NAME, value=data)
                print(f"Sent data for {patient_id}: HR={data['heart_rate']}, SpO2={data['oxygen_saturation']}%")
            time.sleep(5)  # Send data every 5 seconds
    except KeyboardInterrupt:
        print("\nStopping simulation...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()