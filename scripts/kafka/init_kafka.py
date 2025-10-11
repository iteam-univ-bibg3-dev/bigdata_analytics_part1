import time
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

BROKERS = ["kafka1:9092", "kafka2:9093", "kafka3:9094"]
TOPICS = {
    "nature_data": (3, 3),   # name: (partitions, replication_factor)

}


KAFKA_GROUP_ID = "nature_data-group"

RETRIES = 20
SLEEP_SEC = 5

# Wait for Kafka brokers
for attempt in range(RETRIES):
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=BROKERS, client_id='init-kafka')
        break
    except NoBrokersAvailable:
        print(f"Waiting for Kafka brokers... attempt {attempt+1}/{RETRIES}")
        time.sleep(SLEEP_SEC)
else:
    print("Kafka brokers not available after retries. Exiting.")
    exit(1)

# Create topics if not exist
topic_list = [NewTopic(name=name, num_partitions=parts, replication_factor=repl) 
              for name, (parts, repl) in TOPICS.items()]

try:
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    print(f"Topics created successfully: {', '.join(TOPICS.keys())}")
except TopicAlreadyExistsError as e:
    print(f"Some topics already exist: {e}")

admin_client.close()
