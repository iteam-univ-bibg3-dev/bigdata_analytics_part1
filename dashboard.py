


import streamlit as st
from elasticsearch import Elasticsearch
import pandas as pd
import plotly.express as px

ES = Elasticsearch(["http://localhost:9200"])
st.title("Football Live Dashboard - KPIs")

def fetch_latest_kpis(index="football_kpis", size=500):
    q = {"size": size, "sort": [{"@timestamp": {"order": "desc"}}], "query": {"match_all": {}}}
    res = ES.search(index=index, body=q)
    hits = [h["_source"] for h in res["hits"]["hits"]]
    return pd.DataFrame(hits)

df = fetch_latest_kpis("football_raw", 200)
if df.empty:
    st.write("Aucune donnée trouvée")
else:
    # list fixtures and their goals/events
    df['fixture_id'] = df['fixture'].apply(lambda f: f.get('id') if isinstance(f, dict) else None)
    sel_fixture = st.selectbox("Fixture", options=sorted(df['fixture_id'].dropna().unique()))
    sel = df[df['fixture_id'] == sel_fixture].sort_values('timestamp')
    st.write(sel[['fixture','teams','goals','events','timestamp']].tail(10))
