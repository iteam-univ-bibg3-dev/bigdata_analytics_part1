import praw
import json
from kafka import KafkaProducer
import time

# Identifiants Reddit 

reddit = praw.Reddit(
    client_id="e_ZWWZTpjv9MK5EgAGtPdg",
    client_secret="XKOizaIitX0euOw3_0g4Nv_Z-PWcFQ",
    user_agent="reddit-streamer:v1.0 (by /u/streamer_ghada)"
)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

#  Recherche de posts contenant "iPhone 17"

def stream_posts(subreddit_name="all", query="iPhone 17"):
    print(" Recherche en cours sur Reddit...")
    subreddit = reddit.subreddit(subreddit_name)
    for submission in subreddit.search(query, sort="new", limit=10):
        post_data = {
            "subreddit": str(submission.subreddit),
            "author": str(submission.author),
            "body": submission.title,        
            "score": submission.score,
            "url": submission.url,
            "created_utc": submission.created_utc,
            "num_comments": submission.num_comments,
            "id": submission.id
        }
        print(f"send post : {post_data['body']}")
        producer.send('iphone17-tweets', post_data)
        producer.flush()  
        time.sleep(1)  

if __name__ == "__main__":
    while True:
        try:
            stream_posts()
            print("Attente 60s avant nouvelle recherche...\n")
            time.sleep(60)
        except Exception as e:
            print("Erreur :", e)
            time.sleep(5)
