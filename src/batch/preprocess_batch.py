from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, when


spark = SparkSession.builder \
    .appName("RedditPreprocessBatch") \
    .getOrCreate()

# Charger données
df = spark.read.parquet("data/raw/reddit_posts.parquet")

# Sélection des colonnes utiles
df = df.select(
    col("id"),
    col("subreddit"),
    col("title"),
    col("selftext"),
    col("score"),
    col("num_comments"),
    col("created_utc")
)

# Feature engineering simple
df = df.withColumn("title_length", length(col("title")))
df = df.withColumn("selftext_length", length(col("selftext")))
df = df.withColumn("is_popular", when(col("score") > 10, 1).otherwise(0))  # Label binaire

# Sauvegarder données preprocessées
df.write.mode("overwrite").parquet("data/processed/reddit_preprocessed.parquet")

print(" Preprocessing terminé")
