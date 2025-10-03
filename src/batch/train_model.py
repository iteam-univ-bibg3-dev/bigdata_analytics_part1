# batch/train_model.py
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

def train_model(input_path="../data/processed/reddit_features.parquet",
                model_path="../models/reddit_model"):
    spark = SparkSession.builder.appName("RedditTrainModel").getOrCreate()

    df = spark.read.parquet(input_path)

    feature_cols = ["title_length", "selftext_length", "num_comments", "score"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    data = assembler.transform(df).select("features", "label")

    # Split train/test
    train, test = data.randomSplit([0.8, 0.2], seed=42)

    lr = LogisticRegression(featuresCol="features", labelCol="label")
    model = lr.fit(train)

    predictions = model.transform(test)
    evaluator = BinaryClassificationEvaluator(labelCol="label")
    auc = evaluator.evaluate(predictions)

    print(f"Test AUC = {auc}")

    model.save(model_path)
    print(f" Model saved at {model_path}")

    spark.stop()

if __name__ == "__main__":
    train_model()
