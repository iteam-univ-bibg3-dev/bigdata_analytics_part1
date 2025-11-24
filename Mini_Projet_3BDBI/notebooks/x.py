from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("KafkaRedditStream") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "reddit") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("group.id", "reddit_raw_" + str(int(time.time()))) \
    .load()

df_parsed = df.selectExpr("CAST(value AS STRING) as message")

query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
