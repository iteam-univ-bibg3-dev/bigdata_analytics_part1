# dashboard.py
import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

st.title("📊 Live Flights Dashboard")

# 🔹 Créer la session Spark
spark = SparkSession.builder \
    .appName("FlightsDashboard") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 🔹 Définir le schema
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

# 🔹 Lire les nouvelles données depuis Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:19093,kafka3:19094") \
    .option("subscribe", "fly-topic") \
    .option("startingOffsets", "latest") \
    .load()

# 🔹 Parser JSON et créer le DataFrame final
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

# 🔹 Boucle pour récupérer les nouvelles données en micro-batch
query = df_parsed.writeStream \
    .format("memory") \
    .queryName("flights_table") \
    .outputMode("append") \
    .start()

import time

while True:
    try:
        # 🔹 Lire la table en mémoire Spark
        flights_df = spark.sql("SELECT * FROM flights_table ORDER BY flight_date DESC")
        # Convertir en pandas pour Streamlit
        flights_pd = flights_df.toPandas()
        st.dataframe(flights_pd)
        time.sleep(5)  # actualiser toutes les 5 sec
    except KeyboardInterrupt:
        query.stop()
        break
