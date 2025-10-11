import streamlit as st
from pymongo import MongoClient
import pandas as pd
import plotly.express as px


#  Connexion MongoDB

try:
    client = MongoClient("mongodb://localhost:27017/")
    db = client.iphones_17_reddit
    collection = db['iphone17_posts']
    client.server_info()  
except Exception as e:
    st.error(f" Erreur de connexion a MongoDB : {e}")
    st.stop()

#  Titre de l'application

st.title(" Dashboard Reddit iPhone 17 Analytics")



data = list(collection.find())
if not data:
    st.warning("Aucune donnee trouvee. Lancez le producer.py pour alimenter MongoDB.")
    st.stop()


# Preparation des data

df = pd.DataFrame(data)

if 'created_utc' not in df.columns:
    st.error(" Le champ 'created_utc' est manquant dans les donnees.")
    st.stop()

df = df[df['created_utc'].notnull()]
df['created_utc'] = pd.to_datetime(df['created_utc'], unit='s')


#  Filtre par date

st.sidebar.subheader(" Filtrer par date")

min_date = df['created_utc'].min().date()
max_date = df['created_utc'].max().date()

start_date = st.sidebar.date_input("Date debut", value=min_date, min_value=min_date, max_value=max_date)
end_date = st.sidebar.date_input("Date fin", value=max_date, min_value=min_date, max_value=max_date)

if start_date > end_date:
    st.sidebar.error(" La date de debut doit etre anterieure a la date de fin.")
    st.stop()

filtered_df = df

st.sidebar.success(f" {len(filtered_df)} posts/commentaires selectionnes")


# Statistiques globales

st.subheader("Statistiques globales")

col1, col2, col3 = st.columns(3)
col1.metric("Posts / Comments", len(filtered_df))
col2.metric("Auteurs uniques", filtered_df['author'].nunique())
col3.metric("Score moyen", round(filtered_df['score'].mean(), 2) if 'score' in filtered_df.columns else "N/A")

# Gaphique : Top auteurs

st.subheader(" Nombre de posts/commentaires par auteur")

author_counts = filtered_df['author'].value_counts().reset_index()
author_counts.columns = ['author', 'count']

fig = px.bar(author_counts.head(20), x='author', y='count', title="Top 20 auteurs")
st.plotly_chart(fig)


# Liste des posts recents

st.subheader("Derniers posts/commentaires")

for _, row in filtered_df.sort_values('created_utc', ascending=False).head(10).iterrows():
    st.markdown(f"**Auteur :** {row.get('author', 'N/A')}")
    if row.get('title'):
        st.markdown(f"**Titre :** {row['title']}")
    if row.get('selftext'):
        st.markdown(f"**Texte :** {row['selftext']}")
    if row.get('body'):
        st.markdown(f"**Commentaire :** {row['body']}")
    st.markdown(f"**Score :** {row.get('score', 'N/A')}")
    st.markdown(f"**Date :** {row['created_utc']}")
    st.markdown("---")
