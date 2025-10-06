import json
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import time
from datetime import datetime

# Configuration
KAFKA_SERVER = 'localhost:9092'
ELASTICSEARCH_HOST = 'http://localhost:9200'
TOPIC_NAME = 'raw-vitals'

# Connect to Elasticsearch
es = Elasticsearch(ELASTICSEARCH_HOST)

# Create Kafka consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_SERVER],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True
)

def check_critical_conditions(data):
    """Check if vital signs are in critical ranges"""
    alerts = []
    
    # Critical heart rate
    if data['heart_rate'] > 120:
        alerts.append(f"CRITICAL: High heart rate ({data['heart_rate']} bpm)")
    elif data['heart_rate'] < 50:
        alerts.append(f"CRITICAL: Low heart rate ({data['heart_rate']} bpm)")
    
    # Critical oxygen saturation
    if data['oxygen_saturation'] < 90:
        alerts.append(f"CRITICAL: Low oxygen saturation ({data['oxygen_saturation']}%)")
    
    # Critical blood pressure
    if data['blood_pressure_systolic'] > 180:
        alerts.append(f"CRITICAL: High systolic BP ({data['blood_pressure_systolic']})")
    elif data['blood_pressure_systolic'] < 90:
        alerts.append(f"CRITICAL: Low systolic BP ({data['blood_pressure_systolic']})")
    
    return alerts

def create_alert_index_if_not_exists():
    """Create alerts index if it doesn't exist"""
    if not es.indices.exists(index="patient-alerts"):
        es.indices.create(
            index="patient-alerts",
            body={
                "mappings": {
                    "properties": {
                        "patient_id": {"type": "keyword"},
                        "timestamp": {"type": "date"},
                        "alert_type": {"type": "keyword"},
                        "alert_message": {"type": "text"},
                        "vital_sign": {"type": "keyword"},
                        "value": {"type": "float"},
                        "severity": {"type": "keyword"}
                    }
                }
            }
        )
        print("Created Elasticsearch index: patient-alerts")

def send_alert_to_elasticsearch(patient_id, alert_message, vital_sign, value):
    """Send alert to Elasticsearch"""
    try:
        alert_data = {
            "patient_id": patient_id,
            "timestamp": datetime.utcnow().isoformat(),
            "alert_message": alert_message,
            "alert_type": "critical_vital",
            "vital_sign": vital_sign,
            "value": value,
            "severity": "critical"
        }
        
        doc_id = f"{patient_id}_{datetime.utcnow().timestamp()}"
        
        response = es.index(
            index="patient-alerts",
            id=doc_id,
            document=alert_data
        )
        return response
    except Exception as e:
        print(f"Error sending alert to Elasticsearch: {e}")
        return None

def main():
    print("Starting Enhanced Kafka to Elasticsearch Connector...")
    print("Now with critical condition alerting!")
    print("Press Ctrl+C to stop")
    print("-" * 50)
    
    # Create indices if they don't exist
    if not es.indices.exists(index="patient-vitals"):
        es.indices.create(index="patient-vitals")
    create_alert_index_if_not_exists()
    
    message_count = 0
    alert_count = 0
    
    try:
        for message in consumer:
            data = message.value
            message_count += 1
            
            # Send to main vitals index
            result = es.index(
                index="patient-vitals",
                id=f"{data['patient_id']}_{data['timestamp'].replace(':', '-').replace('.', '-')}",
                document=data
            )
            
            # Check for critical conditions
            alerts = check_critical_conditions(data)
            
            if alerts:
                for alert in alerts:
                    # Extract vital sign from alert message
                    vital_sign = "unknown"
                    if "heart rate" in alert.lower():
                        vital_sign = "heart_rate"
                    elif "oxygen" in alert.lower():
                        vital_sign = "oxygen_saturation"
                    elif "blood pressure" in alert.lower():
                        vital_sign = "blood_pressure"
                    
                    # Send alert
                    alert_result = send_alert_to_elasticsearch(
                        data['patient_id'], 
                        alert, 
                        vital_sign,
                        data.get(vital_sign, 0)
                    )
                    
                    if alert_result and alert_result['result'] == 'created':
                        alert_count += 1
                        print(f"🚨 ALERT [{alert_count}]: {data['patient_id']} - {alert}")
            
            if result['result'] == 'created':
                status = "✅"
                if alerts:
                    status = "🚨"
                print(f"{status} [{message_count}] {data['patient_id']} - HR: {data['heart_rate']}, SpO2: {data['oxygen_saturation']}%")
            
    except KeyboardInterrupt:
        print(f"\nStopping connector... Processed {message_count} messages, generated {alert_count} alerts")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()