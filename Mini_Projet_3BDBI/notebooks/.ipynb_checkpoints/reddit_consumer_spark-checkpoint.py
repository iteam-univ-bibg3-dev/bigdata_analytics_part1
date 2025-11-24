from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pymongo import MongoClient

# ===  Création de la session Spark ===
spark = SparkSession.builder \
    .appName("RedditToMongo") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# === Définition du schéma JSON ===
reddit_schema = StructType([
    StructField("id", StringType()),
    StructField("title", StringType()),
    StructField("author", StringType()),
    StructField("score", IntegerType()),
    StructField("created_utc", DoubleType()),
    StructField("url", StringType())
])

# === Lecture du flux Kafka ===
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:19093,kafka3:19094") \
    .option("subscribe", "reddit") \
    .option("startingOffsets", "latest") \
    .load()

# ===  Parsing JSON ===
df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), reddit_schema).alias("data")) \
    .select("data.*")

# === 5Fonction d’insertion dans MongoDB (sans Pandas) ===
def write_to_mongo(batch_df, batch_id):
    records = [row.asDict() for row in batch_df.collect()]
    if records:
        client = MongoClient("mongodb://admin:admin123@mongo:27017/?authSource=admin")
        db = client["reddit_db"]
        collection = db["reddit_stream"]
        collection.insert_many(records)
        client.close()
        print(f"✅ Batch {batch_id} inséré dans Mongo ({len(records)} documents)")

# === Démarrage du streaming ===
query = df_parsed.writeStream \
    .foreachBatch(write_to_mongo) \
    .outputMode("append") \
    .start()

query.awaitTermination()
