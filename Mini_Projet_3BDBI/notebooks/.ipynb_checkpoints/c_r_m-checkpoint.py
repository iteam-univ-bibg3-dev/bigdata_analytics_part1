# reddit_consumer_spark_mongo.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, concat_ws, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.types import IntegerType
import joblib
from pymongo import MongoClient

# === Charger le modèle batch ===
model_path = "/home/jovyan/work/Mini_Projet_3BDBI/models/reddit_batch_model.pkl"
pipeline = joblib.load(model_path)

# === UDF pour appliquer le modèle sur le texte ===
def predict_label(text):
    try:
        pred = pipeline.predict([text])
        return int(pred[0])
    except:
        return 0

predict_udf = udf(predict_label, IntegerType())

# === Créer Spark session avec Kafka ===
spark = SparkSession.builder \
    .appName("RedditDashboardMongo") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ===  Définir le schema Kafka ===
reddit_schema = StructType([
    StructField("id", StringType()),
    StructField("title", StringType()),
    StructField("selftext", StringType()),
    StructField("author", StringType()),
    StructField("score", IntegerType()),
    StructField("created_utc", DoubleType()),
    StructField("url", StringType())
])

# === Lire le flux Kafka ===
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:19093,kafka3:19094") \
    .option("subscribe", "reddit") \
    .option("startingOffsets", "earliest") \
    .load()

df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), reddit_schema).alias("data")) \
    .select(
        col("data.id"),
        col("data.title"),
        col("data.selftext"),
        col("data.author"),
        col("data.score"),
        col("data.created_utc"),
        col("data.url")
    )

# === Créer colonne texte et prédiction ===
df_text = df_parsed.withColumn("text", concat_ws(" ", col("title"), col("selftext")))
df_pred = df_text.withColumn("prediction", predict_udf(col("text")))

# === Fonction pour insérer dans MongoDB ===
def write_to_mongo(batch_df, batch_id):
    records = [row.asDict() for row in batch_df.collect()]
    if records:
        client = MongoClient("mongodb://admin:admin123@mongo:27017/?authSource=admin")
        db = client["reddit_db"]                # Mongo crée la DB si elle n’existe pas
        collection = db["reddit_predictions"]   # Mongo crée la collection si elle n’existe pas
        collection.insert_many(records)
        client.close()
        print(f" Batch {batch_id} inséré dans Mongo ({len(records)} documents)")

# === Démarrer le streaming avec insertion Mongo et console ===
query = df_pred.writeStream \
    .foreachBatch(write_to_mongo) \
    .outputMode("append") \
    .start()

# Pour debug en console (optionnel)
query_console = df_pred.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
query_console.awaitTermination()
