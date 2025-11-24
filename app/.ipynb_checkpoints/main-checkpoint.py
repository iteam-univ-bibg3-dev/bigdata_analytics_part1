# import streamlit as st
# from pymongo import MongoClient
# import pandas as pd
# import altair as alt



# # --- Connexion MongoDB ---
# MONGO_USER = "admin"
# MONGO_PASS = "admin123"
# MONGO_HOST = "mongo"
# MONGO_PORT = 27017
# MONGO_DB = "reddit_db"
# MONGO_COLLECTION = "reddit_predictions"

# uri = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}?authSource=admin"
# client = MongoClient(uri)
# db = client[MONGO_DB]
# collection = db[MONGO_COLLECTION]

# # --- Paramètres Streamlit ---
# st.set_page_config(
#     page_title="Dashboard Reddit ",
#     layout="wide",
#     initial_sidebar_state="expanded"
# )
# st.title("Dashboard Reddit – Analyse des posts Reddit prédits")

# # --- Rafraîchissement automatique ---
# refresh_interval = st.sidebar.slider("Intervalle de rafraîchissement (s)", 5, 60, 10)
# st.sidebar.write(f"Rafraîchissement toutes les {refresh_interval} secondes")

# # --- Vérification rapide du nombre de documents ---
# doc_count = collection.count_documents({})
# st.sidebar.write(f"Nombre de documents : {doc_count}")

# # --- Lecture des données ---
# @st.cache_data(ttl=refresh_interval)
# def load_data():
#     data = list(collection.find())
#     if not data:
#         return pd.DataFrame()
#     df = pd.DataFrame(data)
#     if "_id" in df.columns:
#         df["_id"] = df["_id"].astype(str)
#     return df

# df = load_data()

# if df.empty:
#     st.warning(" Aucune donnée trouvée dans MongoDB.")
# else:
#     # --- KPI ---
#     col1, col2, col3, col4 = st.columns(4)
#     col1.metric("Total Posts", len(df))
#     col2.metric("Score Moyen", round(df['score'].mean(),2) if 'score' in df.columns else 0)
#     if 'prediction' in df.columns:
#         col3.metric("Posts prédits", len(df[df['prediction']==1]))
#     col4.metric("Taux de posts populaires", 
#             f"{(df['prediction'].mean() * 100 if 'prediction' in df.columns else 0):.1f} %")
     
#     # === Top auteurs ===
#     if "author" in df.columns:
#         st.subheader(" Top 10 Auteurs les plus actifs")

#         top_authors = df["author"].value_counts().head(10).reset_index()
#         top_authors.columns = ["author", "count"]

#         chart3 = alt.Chart(top_authors).mark_bar(color="#4e79a7").encode(
#             x=alt.X("count:Q", title="Nombre de posts"),
#             y=alt.Y("author:N", sort='-x', title="Auteur"),
#             tooltip=["author:N", "count:Q"]
#         ).properties(width=700, height=400)

#         st.altair_chart(chart3, use_container_width=True)
#         st.markdown("---")
import streamlit as st
from pymongo import MongoClient
import pandas as pd
import altair as alt

# --- Paramètres Streamlit ---
st.set_page_config(
    page_title="Dashboard Reddit",
    layout="wide",
    initial_sidebar_state="expanded"
)
st.title("Dashboard Reddit – Analyse des posts Reddit prédits")

# --- Rafraîchissement automatique simple ---
refresh_interval = st.sidebar.slider("Intervalle de rafraîchissement (s)", 5, 60, 10)
st.sidebar.write(f"Rafraîchissement toutes les {refresh_interval} secondes")
# Injection HTML pour auto-refresh via navigateur
st.markdown(f'<meta http-equiv="refresh" content="{refresh_interval}">', unsafe_allow_html=True)

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
    st.warning("Aucune donnée trouvée dans MongoDB.")
else:
    # --- KPI ---
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Posts", len(df))
    col2.metric("Score Moyen", round(df['score'].mean(),2) if 'score' in df.columns else 0)
    if 'prediction' in df.columns:
        col3.metric("Posts prédits", len(df[df['prediction']==1]))
        col4.metric("Taux de posts populaires", f"{(df['prediction'].mean() * 100):.1f} %")
    else:
        col3.metric("Posts prédits", 0)
        col4.metric("Taux de posts populaires", "0 %")

    # --- Top 10 auteurs ---
    if "author" in df.columns:
        st.subheader("Top 10 auteurs les plus actifs")
        top_authors = df["author"].value_counts().head(10).reset_index()
        top_authors.columns = ["author", "count"]
        chart_authors = alt.Chart(top_authors).mark_bar(color="#4e79a7").encode(
            x=alt.X("count:Q", title="Nombre de posts"),
            y=alt.Y("author:N", sort='-x', title="Auteur"),
            tooltip=["author:N", "count:Q"]
        ).properties(width=700, height=400)
        st.altair_chart(chart_authors, use_container_width=True)
        st.markdown("---")

    

        