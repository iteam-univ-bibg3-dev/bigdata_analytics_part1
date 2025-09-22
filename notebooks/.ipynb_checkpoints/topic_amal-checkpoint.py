from confluent_kafka.admin import AdminClient, NewTopic
TOPIC='fly-amal'
admin_client = AdminClient({
    "bootstrap.servers": "kafka1:19092,kafka2:19093,kafka3:19094"
})

topic_list = [NewTopic(TOPIC, num_partitions=3, replication_factor=2)]
fs = admin_client.create_topics(topic_list)

for topic, f in fs.items():
    try:
        f.result()
        print(f"Topic {topic} créé avec succès ✅")
    except Exception as e:
        print(f"Erreur création topic {topic}: {e}")