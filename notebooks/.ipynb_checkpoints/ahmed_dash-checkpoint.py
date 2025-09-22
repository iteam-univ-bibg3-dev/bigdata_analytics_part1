import streamlit as st
import time
import pandas as pd
import plotly.express as px
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

st.set_page_config(page_title="📊 Live Flights Dashboard", layout="wide")
st.title("📊 Live Flights Dashboard")

# 🔹 Créer la session Spark
spark = SparkSession.builder \
    .appName("FlightsDashboard") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 🔹 Définir le schema JSON
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

query = df_parsed.writeStream \
    .format("memory") \
    .queryName("flights_table") \
    .outputMode("append") \
    .start()

# 🔹 Placeholders
bar_placeholder = st.empty()
map_placeholder = st.empty()
table_placeholder = st.empty()

def get_flights_data():
    try:
        flights_df = spark.sql("SELECT * FROM flights_table ORDER BY flight_date DESC")
        return flights_df.toPandas()
    except:
        return pd.DataFrame()

# 🔹 Boucle de rafraîchissement
REFRESH_INTERVAL = 5  # secondes

while True:
    flights_pd = get_flights_data()
    if not flights_pd.empty:
        # Tableau
        table_placeholder.dataframe(flights_pd)

        # Statut des vols
        status_count = flights_pd['status'].value_counts().reset_index()
        status_count.columns = ['statut', 'nombre']
        fig_status = px.bar(status_count, x='statut', y='nombre', color='statut',
                            labels={'statut':'Statut', 'nombre':'Nombre de vols'},
                            title="Statut des vols")
        bar_placeholder.plotly_chart(fig_status, use_container_width=True, key=f"bar_{time.time()}")

        # Carte des vols
        fig_map = px.scatter_geo(flights_pd, locations="departure_airport",
                                 hover_name="flight_code", color="status",
                                 title="Carte des vols")
        map_placeholder.plotly_chart(fig_map, use_container_width=True, key=f"map_{time.time()}")
    
    time.sleep(REFRESH_INTERVAL)
