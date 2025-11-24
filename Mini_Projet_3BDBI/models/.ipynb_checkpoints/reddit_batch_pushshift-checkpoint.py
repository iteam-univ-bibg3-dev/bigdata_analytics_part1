import requests
import pandas as pd
import time
import os

def fetch_pushshift_batch(subreddit="python", total_posts=2000):
    """
    Récupère un batch de posts depuis Pushshift API
    avec le même schéma que ton flux Kafka (id, title, author, score, created_utc, url).
    """
    url = "https://api.pullpush.io/reddit/submission/search"
    all_posts = []
    before = None

    while len(all_posts) < total_posts:
        remaining = total_posts - len(all_posts)
        size = 100 if remaining > 100 else remaining

        params = {
            "subreddit": subreddit,
            "size": size,
            "sort": "desc",
            "sort_type": "created_utc",
            "before": before
        }

        response = requests.get(url, params=params)
        if response.status_code != 200:
            print(f"⚠️ Erreur {response.status_code}")
            break

        data = response.json().get("data", [])
        if not data:
            break

        for post in data:
            all_posts.append({
                "id": post.get("id"),
                "title": post.get("title"),
                "author": post.get("author"),
                "score": post.get("score", 0),
                "created_utc": post.get("created_utc"),
                "url": post.get("url")
            })

        before = data[-1]["created_utc"]
        print(f"📦 {len(all_posts)} posts collectés...")
        time.sleep(1)

    df = pd.DataFrame(all_posts)
    os.makedirs("../data", exist_ok=True)
    output_path = "../data/reddit_batch_pushshift.csv"
    df.to_csv(output_path, index=False)
    print(f"✅ Données batch sauvegardées dans {output_path} ({len(df)} lignes)")
    return df


if __name__ == "__main__":
    df = fetch_pushshift_batch(subreddit="python", total_posts=2000)
    print(df.head())
