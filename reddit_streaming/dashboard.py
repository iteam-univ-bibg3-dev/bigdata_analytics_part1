import streamlit as st
from pymongo import MongoClient
import pandas as pd

# Connexion MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["reddit_db"]
collection = db["iphone17_posts"]

# Chargement des données
data = list(collection.find({}, {"_id": 0}))  

st.set_page_config(page_title="Dashboard Reddit iPhone 17 Analytics", layout="wide")
st.title("Dashboard Reddit iPhone 17 Analytics")

if not data:
    st.warning("Aucune donnée trouvée. Lancez le producer.py pour alimenter MongoDB.")
else:
    df = pd.DataFrame(data)

    # --- Aperçu des publications ---
    st.subheader("Aperçu des publications collectées")
    st.dataframe(df.head())

    # --- Statistiques générales ---
    st.subheader(" Statistiques générales")
    col1, col2, col3 = st.columns(3)
    col1.metric("Nombre total de posts", len(df))

    if "score" in df.columns:
        col2.metric("Score moyen", round(df["score"].mean(), 2))
    else:
        col2.metric("Score moyen", "N/A")

    if "num_comments" in df.columns:
        col3.metric("Commentaires moyens", round(df["num_comments"].mean(), 2))
    elif "comments" in df.columns:
        col3.metric("Commentaires moyens", round(df["comments"].mean(), 2))
    else:
        col3.metric("Commentaires moyens", "N/A")

    # --- Évolution temporelle (Octobre 2025 uniquement) ---
    date_col = None
    for col in ["created_utc", "timestamp", "date_posted"]:
        if col in df.columns:
            date_col = col
            break

    if date_col:
        df[date_col] = pd.to_datetime(df[date_col], unit='s', errors='coerce')
        df = df.dropna(subset=[date_col])

        #  Filtrer uniquement les publications d'octobre 2025
        df_october = df[(df[date_col].dt.year == 2025) & (df[date_col].dt.month == 10)]

        if not df_october.empty:
            posts_by_date = df_october.groupby(df_october[date_col].dt.date).size().reset_index(name="count")
            posts_by_date.rename(columns={date_col: "date"}, inplace=True)
            st.subheader("Évolution des publications – Octobre 2025")
            st.line_chart(posts_by_date.set_index("date"))
        else:
            st.info("Aucune publication trouvée pour octobre 2025.")
    else:
        st.info("Aucun champ de date disponible pour tracer l’évolution dans le temps.")

    # --- Top 10 des publications ---
    st.subheader("Top 10 des publications les plus populaires")
    available_columns = df.columns.tolist()
    cols_to_display = [col for col in ["title", "score", "num_comments", "subreddit"] if col in available_columns]

    if "score" in df.columns:
        top_posts = df.sort_values(by="score", ascending=False).head(10)
        st.table(top_posts[cols_to_display])
    else:
        st.info("Aucune donnée de score disponible pour classer les publications.")

    # --- Analyse des sentiments ---
    if "sentiment" in df.columns:
        st.subheader("Répartition des sentiments")
        sentiment_counts = df['sentiment'].value_counts()
        st.bar_chart(sentiment_counts)

