from elasticsearch import Elasticsearch

es = Elasticsearch('http://localhost:9200')

# Check index exists
if es.indices.exists(index="patient-vitals"):
    print("✅ Index 'patient-vitals' exists")
    # Count documents
    count = es.count(index="patient-vitals")['count']
    print(f"📊 Total documents in index: {count}")
    # Get a sample of documents
    response = es.search(
        index="patient-vitals",
        body={
            "size": 3,
            "sort": [{ "timestamp": { "order": "desc" } }]
        }
    )
    print("\n📄 Sample documents:")
    for hit in response['hits']['hits']:
        print(f"Patient: {hit['_source']['patient_id']}, HR: {hit['_source']['heart_rate']}")
else:
    print("❌ Index 'patient-vitals' does not exist")