from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, lit
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, StructType

# 🔹 Créer la session Spark avec le connecteur Kafka
spark = SparkSession.builder \
    .appName("FlightsDashboard") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 🔹 Définir le schema JSON du message complet
flight_schema = StructType([
    StructField("flight_date", StringType()),
    StructField("flight_status", StringType()),
    StructField("departure", StructType([
        StructField("airport", StringType()),
        StructField("iata", StringType()),
        StructField("scheduled", StringType())
    ])),
    StructField("arrival", StructType([
        StructField("airport", StringType()),
        StructField("iata", StringType()),
        StructField("scheduled", StringType())
    ])),
    StructField("airline", StructType([
        StructField("name", StringType()),
        StructField("iata", StringType())
    ])),
    StructField("flight", StructType([
        StructField("number", StringType()),
        StructField("iata", StringType())
    ])),
    StructField("live", StructType([
        StructField("is_ground", BooleanType())
    ]))
])

# 🔹 Lire le flux depuis Kafka, seulement les nouvelles données
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:19093,kafka3:19094") \
    .option("subscribe", "fly-topic") \
    .option("startingOffsets", "latest") \
    .load()

# 🔹 Convertir la valeur en string et parser JSON
df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), flight_schema).alias("data")) \
    .select(
        col("data.flight.iata").alias("flight_code"),
        col("data.flight_date").alias("flight_date"),
        col("data.airline.name").alias("airline"),
        col("data.departure.airport").alias("departure_airport"),
        col("data.arrival.airport").alias("arrival_airport"),
        expr("""
            CASE 
                WHEN data.live IS NOT NULL AND data.live.is_ground = false THEN 'En vol'
                ELSE 'Au sol'
            END
        """).alias("status")
    )

# 🔹 Affichage console pour test
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
