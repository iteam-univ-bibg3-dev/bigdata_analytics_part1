# reddit_batch.py
from dotenv import load_dotenv
import os
import praw
import pandas as pd
from datetime import datetime

# Charger les variables d'environnement
load_dotenv()  # <- c'est essentiel pour lire le fichier .env

# Récupérer les clés Reddit depuis le .env
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.getenv("REDDIT_USER_AGENT")

# Vérification
if not all([REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT]):
    raise ValueError("Les variables d'environnement Reddit ne sont pas toutes définies.")

#  Initialisation de PRAW
reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    user_agent=REDDIT_USER_AGENT
)



#  Paramètres
SUBREDDIT = "python"
LIMIT = 10000
OUTPUT_DIR = "../data"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_PATH = os.path.join(OUTPUT_DIR, f"reddit_{SUBREDDIT}.csv")

#  Récupération des posts
print(f" Récupération des {LIMIT} posts de r/{SUBREDDIT}...")

posts = []
try:
    subreddit = reddit.subreddit(SUBREDDIT)
    for post in subreddit.new(limit=LIMIT):
        posts.append({
            "id": post.id,
            "title": post.title,
            "author": str(post.author),
            "score": post.score,
            "num_comments": post.num_comments,
            "created_utc": datetime.utcfromtimestamp(post.created_utc),
            "url": post.url,
            "selftext": post.selftext
        })
except Exception as e:
    print(f"Erreur Reddit API : {e}")

#  Conversion en DataFrame et sauvegarde CSV
if posts:
    df = pd.DataFrame(posts)
    df.to_csv(OUTPUT_PATH, index=False)
    print(f" {len(df)} posts de r/{SUBREDDIT} sauvegardés dans {OUTPUT_PATH} !")
else:
    print(" Aucun post récupéré.")
