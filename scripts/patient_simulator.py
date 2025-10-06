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

def generate_normal_vitals(patient_id):
    """Generate normal vital signs"""
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

def generate_abnormal_vitals(patient_id):
    """Generate abnormal vital signs to trigger alerts"""
    abnormality_type = random.choice(['high_hr', 'low_hr', 'low_spo2', 'high_bp'])
    
    base_data = {
        'patient_id': patient_id,
        'timestamp': datetime.now().isoformat(),
        'respiratory_rate': random.randint(12, 20),
        'temperature': round(random.uniform(36.0, 38.0), 1)
    }
    
    if abnormality_type == 'high_hr':
        base_data.update({
            'heart_rate': random.randint(120, 160),
            'blood_pressure_systolic': random.randint(130, 160),
            'blood_pressure_diastolic': random.randint(80, 100),
            'oxygen_saturation': random.randint(92, 98)
        })
    elif abnormality_type == 'low_hr':
        base_data.update({
            'heart_rate': random.randint(40, 55),
            'blood_pressure_systolic': random.randint(90, 110),
            'blood_pressure_diastolic': random.randint(50, 65),
            'oxygen_saturation': random.randint(94, 98)
        })
    elif abnormality_type == 'low_spo2':
        base_data.update({
            'heart_rate': random.randint(90, 120),
            'blood_pressure_systolic': random.randint(120, 150),
            'blood_pressure_diastolic': random.randint(75, 90),
            'oxygen_saturation': random.randint(85, 89)
        })
    elif abnormality_type == 'high_bp':
        base_data.update({
            'heart_rate': random.randint(70, 100),
            'blood_pressure_systolic': random.randint(150, 190),
            'blood_pressure_diastolic': random.randint(95, 110),
            'oxygen_saturation': random.randint(94, 98)
        })
    
    return base_data

def main():
    patient_ids = [f"patient_{i:03d}" for i in range(1, 6)]  # 5 patients
    
    print("Starting ENHANCED patient data simulation...")
    print("Now with occasional abnormal values to test alerting!")
    print("Press Ctrl+C to stop")
    
    try:
        while True:
            for patient_id in patient_ids:
                # 20% chance of abnormal data for more interesting testing
                if random.random() < 0.2:
                    data = generate_abnormal_vitals(patient_id)
                    print(f"🚨 GENERATING ABNORMAL DATA for {patient_id}")
                else:
                    data = generate_normal_vitals(patient_id)
                
                producer.send(TOPIC_NAME, value=data)
                print(f"Sent data for {patient_id}: HR={data['heart_rate']}, SpO2={data['oxygen_saturation']}%, BP={data['blood_pressure_systolic']}/{data['blood_pressure_diastolic']}")
            
            time.sleep(5)  # Send data every 5 seconds
            
    except KeyboardInterrupt:
        print("\nStopping simulation...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()