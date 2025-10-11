# KAFKA CONSUMER + PYSPARK ANALYSIS

import os
import json
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_utc_timestamp
from pyspark.sql.types import StructType, StructField, StringType, MapType
from elasticsearch import Elasticsearch, helpers
from datetime import datetime, timezone


# Config Kafka
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "football.raw_events")
ES_HOST = os.getenv("ES_HOST", "http://elasticsearch:9200")
ES_INDEX_RAW = os.getenv("ES_INDEX_RAW", "events-raw")
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "/checkpoints")

# SparkSession
spark = (SparkSession.builder
         .appName("FootballLiveSpark")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())

# Schema 
event_schema = StructType([
    StructField("timestamp", StringType()),
    StructField("source", StringType()),
    StructField("match_id", StringType()),
    StructField("competition", StringType()),
    StructField("home_team", StringType()),
    StructField("away_team", StringType()),
    StructField("home_score", StringType()),
    StructField("away_score", StringType()),
    StructField("status", StringType()),
    StructField("raw", MapType(StringType(), StringType()))
])

# Lire le flux Kafka
df_kafka = (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "latest")
            .load())

df_values = df_kafka.selectExpr("CAST(value AS STRING) as value")
df_parsed = df_values.select(from_json(col("value"), event_schema).alias("data")).select("data.*")
df_parsed = df_parsed.withColumn("fetched_ts_utc", to_utc_timestamp(col("timestamp"), "UTC"))

# Write batch in Elasticsearch
def write_to_es(batch_df, batch_id):
    es = Elasticsearch([ES_HOST])
    actions = []

    for row in batch_df.collect():
        obj = row.asDict()
        obj.pop("events", None) 
        doc_key = obj.get("competition") or obj.get("status") or "unknown"
        ts = obj.get("timestamp") or datetime.now(timezone.utc).isoformat()
        safe_ts = re.sub(r'[:.]', "-", ts)
        doc_id = f"{doc_key}_{safe_ts}"
        actions.append({"_index": ES_INDEX_RAW, "_id": doc_id, "_source": obj})

    if actions:
        helpers.bulk(es, actions)
        print(f"[INFO] Batch {batch_id} indexé : {len(actions)} documents")

# Write continue
query = (df_parsed.writeStream
         .foreachBatch(write_to_es)
         .outputMode("append")
         .option("checkpointLocation", f"{CHECKPOINT_ROOT}/raw")
         .start())

query.awaitTermination()
