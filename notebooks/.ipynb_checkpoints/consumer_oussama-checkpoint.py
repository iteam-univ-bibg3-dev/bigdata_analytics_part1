from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

# 🔹 Créer la session Spark avec le connecteur Kafka
spark = SparkSession.builder \
    .appName("FlightsDashboard") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 🔹 Définir le schema JSON du message
flight_schema = StructType([
    StructField("flight", StructType([
        StructField("iata", StringType())
    ])),
    StructField("live", StructType([
        StructField("is_ground", BooleanType())
    ]))
])

# 🔹 Lire le flux depuis Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:19093,kafka3:19094") \
    .option("subscribe", "fly-Safa") \
    .option("startingOffsets", "earliest") \
    .load()

# 🔹 Convertir la valeur en string et parser JSON
df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), flight_schema).alias("data")) \
    .select(
        col("data.flight.iata").alias("flight_code"),
        expr("CASE WHEN data.live.is_ground = false THEN 'En vol' ELSE 'Au sol' END").alias("status")
    )

# 🔹 Affichage console pour test
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
