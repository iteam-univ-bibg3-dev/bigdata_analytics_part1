import os
import json
from kafka import KafkaProducer
from dotenv import load_dotenv
import praw

# Charger les variables d'environnement
load_dotenv()

REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
SUBREDDIT = os.getenv("SUBREDDIT")

# Configurer Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Configurer Reddit API
reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    user_agent=REDDIT_USER_AGENT
)

# Streamer les posts Reddit
subreddit = reddit.subreddit(SUBREDDIT)
print(f"Streaming posts from r/{SUBREDDIT}...")

for submission in subreddit.stream.submissions(skip_existing=True):
    data = {
        "id": submission.id,
        "title": submission.title,
        "author": str(submission.author),
        "score": submission.score,
        "created_utc": submission.created_utc,
        "url": submission.url
    }
    producer.send(KAFKA_TOPIC, value=data)
    print(f"Sent post: {submission.title}")
