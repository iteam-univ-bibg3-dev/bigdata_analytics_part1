# reddit_producer.py
import os
import json
import time
import praw
from kafka import KafkaProducer
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "reddit"  # ton topic créé

# Configurer Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Configurer Reddit API
reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    user_agent=REDDIT_USER_AGENT
)

SUBREDDIT = "python"  # tu peux mettre ton subreddit préféré

def fetch_posts(limit=5):
    """Récupérer les derniers posts du subreddit"""
    posts_data = []
    try:
        subreddit = reddit.subreddit(SUBREDDIT)
        for submission in subreddit.new(limit=limit):
            posts_data.append({
                "id": submission.id,
                "title": submission.title,
                "author": str(submission.author),
                "score": submission.score,
                "created_utc": submission.created_utc,
                "url": submission.url
            })
    except Exception as e:
        print(f" Reddit API Error: {e}")
    return posts_data

# Boucle producer
while True:
    posts = fetch_posts(limit=5)
    if posts:
        for post in posts:
            producer.send(TOPIC, post)
            print(f" Sent post: {post['title']}")
    time.sleep(10)
