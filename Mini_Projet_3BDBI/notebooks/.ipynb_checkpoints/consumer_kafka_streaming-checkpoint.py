# reddit_consumer_spark.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# 🔹 Créer la session Spark avec le connecteur Kafka compatible Scala 2.13
spark = SparkSession.builder \
    .appName("RedditDashboard") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 🔹 Définir le schema JSON du message Reddit
reddit_schema = StructType([
    StructField("id", StringType()),
    StructField("title", StringType()),
    StructField("author", StringType()),
    StructField("score", IntegerType()),
    StructField("created_utc", DoubleType()),
    StructField("url", StringType())
])

# 🔹 Lire le flux depuis Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:19093,kafka3:19094") \
    .option("subscribe", "reddit") \
    .option("startingOffsets", "earliest") \
    .load()

# 🔹 Convertir la valeur en string et parser JSON
df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), reddit_schema).alias("data")) \
    .select(
        col("data.id"),
        col("data.title"),
        col("data.author"),
        col("data.score"),
        col("data.created_utc"),
        col("data.url")
    )

# 🔹 Affichage console pour test
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
