import streamlit as st
from pymongo import MongoClient
import pandas as pd
import altair as alt

# --- Connexion MongoDB ---
MONGO_USER = "admin"
MONGO_PASS = "admin123"
MONGO_HOST = "mongo"
MONGO_PORT = 27017
MONGO_DB = "reddit_db"
MONGO_COLLECTION = "reddit_predictions"

uri = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}?authSource=admin"
client = MongoClient(uri)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

# --- Paramètres Streamlit ---
st.set_page_config(
    page_title="Dashboard Reddit MongoDB",
    layout="wide",
    initial_sidebar_state="expanded"
)
st.title("Dashboard Reddit MongoDB")

# --- Rafraîchissement automatique ---
refresh_interval = st.sidebar.slider("Intervalle de rafraîchissement (s)", 5, 60, 10)
st.sidebar.write(f"Rafraîchissement toutes les {refresh_interval} secondes")

# --- Vérification rapide du nombre de documents ---
doc_count = collection.count_documents({})
st.sidebar.write(f"Nombre de documents : {doc_count}")

# --- Lecture des données ---
@st.cache_data(ttl=refresh_interval)
def load_data():
    data = list(collection.find())
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    if "_id" in df.columns:
        df["_id"] = df["_id"].astype(str)
    return df

df = load_data()

if df.empty:
    st.warning("⚠️ Aucune donnée trouvée dans MongoDB.")
else:
    # --- KPI ---
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Posts", len(df))
    col2.metric("Score Moyen", round(df['score'].mean(),2) if 'score' in df.columns else 0)
    if 'prediction' in df.columns:
        col3.metric("Posts prédits", len(df[df['prediction']==1]))

   

    # --- Top auteurs ---
    if "author" in df.columns:
        st.subheader("### Top 10 auteurs")
        top_authors = df["author"].value_counts().head(10).reset_index()
        top_authors.columns = ["author", "count"]
        chart2 = alt.Chart(top_authors).mark_bar().encode(
            x='author:N',
            y='count:Q'
        ).properties(width=700, height=400)
        st.altair_chart(chart2, use_container_width=True)
if "prediction" in df.columns and "score" in df.columns:
    st.subheader("📈 Comparaison Score Réel vs Prédiction")

    chart4 = alt.Chart(df).mark_boxplot(extent="min-max").encode(
        x=alt.X("prediction:N", title="Prédiction (0=Non populaire, 1=Populaire)"),
        y=alt.Y("score:Q", title="Score réel"),
        color="prediction:N",
        tooltip=["prediction:N", "score:Q"]
    ).properties(width=700, height=400)

    st.altair_chart(chart4, use_container_width=True)
    st.markdown("---")
   # === 🔹 5️⃣ Table interactive ===
st.subheader("📋 Détails des posts")
st.dataframe(
    df[["author", "title", "score", "prediction", "created_date", "url"]].sort_values("created_date", ascending=False).head(50),
    use_container_width=True
) 


 