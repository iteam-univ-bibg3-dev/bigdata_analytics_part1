#pyspark analysis

# spark_job.py
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType, TimestampType
from elasticsearch import Elasticsearch, helpers

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "football.raw_events")
ES_HOST = os.getenv("ES_HOST", "http://elasticsearch:9200")
ES_INDEX_RAW = os.getenv("ES_INDEX_RAW", "events-raw")
ES_INDEX_AGG = os.getenv("ES_INDEX_AGG", "events-agg")
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "/checkpoints")

spark = (SparkSession.builder
         .appName("FootballLiveSpark")
         .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTS", "2"))
         .getOrCreate())

# Define schema for the message value
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
    StructField("events", ArrayType(MapType(StringType(), StringType()))),
    StructField("raw", MapType(StringType(), StringType()))
])

# Read Kafka stream
df_kafka = (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "latest")
            .load())

df_values = df_kafka.selectExpr("CAST(value AS STRING) as value")

# Parse JSON
df_parsed = df_values.select(from_json(col("value"), event_schema).alias("data")).select("data.*")

# Convert timestamp string to timestamp type
df_parsed = df_parsed.withColumn("fetched_ts", to_timestamp(col("timestamp")))

# Write raw events to Elasticsearch via foreachBatch
def write_raw_to_es(batch_df, batch_id):
    es = Elasticsearch([ES_HOST])
    docs = batch_df.toJSON().collect()
    actions = []
    for d in docs:
        obj = json.loads(d)
        actions.append({"_index": ES_INDEX_RAW, "_source": obj})
    if actions:
        helpers.bulk(es, actions)

raw_query = (df_parsed.writeStream
             .foreachBatch(write_raw_to_es)
             .outputMode("append")
             .option("checkpointLocation", f"{CHECKPOINT_ROOT}/raw")
             .start())

# --- Aggregation example: filter by event_type == 'goal'
from pyspark.sql.functions import explode, get_json_object

# explode events array
df_events = df_parsed.select("fetched_ts", "match_id", "home_team", "away_team", explode(col("events")).alias("ev"))
# ev is a map: access ev['type'] etc.
df_events = df_events.withColumn("ev_type", col("ev").getItem("type")) \
                     .withColumn("ev_team", col("ev").getItem("team")) \
                     .withColumn("ev_minute", col("ev").getItem("minute").cast(IntegerType()))

# Filter goals
df_goals = df_events.filter(col("ev_type") == "goal")

# Count goals per team per 10-min window
df_goals_agg = (df_goals
                .groupBy(window(col("fetched_ts"), "10 minutes"), col("ev_team"), col("match_id"))
                .count()
                .selectExpr("window.start as window_start", "window.end as window_end", "ev_team as team", "match_id", "count as goals_count"))

def write_agg_to_es(batch_df, batch_id):
    es = Elasticsearch([ES_HOST])
    records = batch_df.toJSON().collect()
    actions = [{"_index": ES_INDEX_AGG, "_source": json.loads(r)} for r in records]
    if actions:
        helpers.bulk(es, actions)

agg_query = (df_goals_agg.writeStream
             .foreachBatch(write_agg_to_es)
             .outputMode("update")
             .option("checkpointLocation", f"{CHECKPOINT_ROOT}/agg")
             .start())

raw_query.awaitTermination()
agg_query.awaitTermination()
