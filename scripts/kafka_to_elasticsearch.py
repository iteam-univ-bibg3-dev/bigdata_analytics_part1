import json
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import time

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

def create_index_if_not_exists():
    """Create Elasticsearch index if it doesn't exist"""
    if not es.indices.exists(index="patient-vitals"):
        es.indices.create(
            index="patient-vitals",
            body={
                "mappings": {
                    "properties": {
                        "patient_id": {"type": "keyword"},
                        "timestamp": {"type": "date"},
                        "heart_rate": {"type": "integer"},
                        "blood_pressure_systolic": {"type": "integer"},
                        "blood_pressure_diastolic": {"type": "integer"},
                        "oxygen_saturation": {"type": "integer"},
                        "respiratory_rate": {"type": "integer"},
                        "temperature": {"type": "float"}
                    }
                }
            }
        )
        print("Created Elasticsearch index: patient-vitals")

def send_to_elasticsearch(data):
    """Send data to Elasticsearch"""
    try:
        # Create a document ID using patient_id and timestamp
        doc_id = f"{data['patient_id']}_{data['timestamp'].replace(':', '-').replace('.', '-')}"
        
        response = es.index(
            index="patient-vitals",
            id=doc_id,
            document=data
        )
        return response
    except Exception as e:
        print(f"Error sending to Elasticsearch: {e}")
        return None

def main():
    print("Starting Kafka to Elasticsearch Connector...")
    print("Press Ctrl+C to stop")
    print("-" * 50)
    
    # Create index if it doesn't exist
    create_index_if_not_exists()
    
    message_count = 0
    try:
        for message in consumer:
            data = message.value
            message_count += 1
            # Send to Elasticsearch
            result = send_to_elasticsearch(data)
            if result and result['result'] == 'created':
                print(f"✅ [{message_count}] Sent {data['patient_id']} to Elasticsearch (HR: {data['heart_rate']})")
            else:
                print(f"❌ Failed to send {data['patient_id']} to Elasticsearch")
    except KeyboardInterrupt:
        print(f"\nStopping connector... Processed {message_count} messages")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()