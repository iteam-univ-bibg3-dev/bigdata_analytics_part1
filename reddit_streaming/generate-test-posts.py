from pymongo import MongoClient
import time

# Connexion à MongoDB
mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['reddit_db']
collection = db['iphone17_posts']

# Fonction pour récupérer le top 10 d’un subreddit
def get_top_posts(subreddit, limit=10):
    return list(collection.find({"subreddit": subreddit}).sort("score", -1).limit(limit))

# Fonction d’affichage du Top 10
def print_top_posts(subreddit):
    top_posts = get_top_posts(subreddit)
    print(f"\n=== TOP 10 ACTUEL du subreddit '{subreddit}' ===")
    for i, post in enumerate(top_posts, 1):
        print(f"{i}. {post['title']} (score: {post['score']}) | Sentiment: {post['sentiment']}")
    print("==============================================\n")

# Subreddit à surveiller
subreddit = "iphone17"

print(f"Surveillance en cours sur les nouveaux posts du subreddit '{subreddit}'...")
print_top_posts(subreddit)

# Boucle de surveillance en temps réel
with collection.watch([{"$match": {"operationType": "insert"}}]) as stream:
    for change in stream:
        full_doc = change["fullDocument"]
        if full_doc.get("subreddit") == subreddit:
            print(f"\nNouveau post détecté : {full_doc['title']}")
            print(f"   ➤ Score : {full_doc['score']} | Sentiment : {full_doc['sentiment']}")
            # Mise à jour du TOP 10
            print_top_posts(subreddit)
        time.sleep(0.5)

