from pmaw import PushshiftAPI
import pandas as pd
from datetime import datetime, timedelta

api = PushshiftAPI()

# Exemple : collecter posts du subreddit "datascience" sur les 7 derniers jours
end_time = int(datetime.utcnow().timestamp())
start_time = int((datetime.utcnow() - timedelta(days=7)).timestamp())

subreddit = "datascience"
print(f"Collecting posts from r/{subreddit} between {start_time} and {end_time}")

posts = list(api.search_submissions(
    subreddit=subreddit,
    after=start_time,
    before=end_time,
    limit=5000
))

print(f"Collected {len(posts)} posts")

# Convertir en DataFrame
df = pd.DataFrame(posts)

# Sauvegarde JSON et Parquet
df.to_json("data/raw/reddit_posts.json", orient="records", lines=True)
df.to_parquet("data/raw/reddit_posts.parquet", engine="pyarrow")

print(" Data saved to data/raw/")
