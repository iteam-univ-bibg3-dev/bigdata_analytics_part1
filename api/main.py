# backend/api.py
from fastapi import FastAPI
from elasticsearch import Elasticsearch
from fastapi.middleware.cors import CORSMiddleware
import time


app = FastAPI(title="Disaster Globe API")

# Allow React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
es = Elasticsearch("http://elasticsearch:9200")


@app.get("/events")
def get_events():
    query = {
        "size": 500,  # limit to latest 500 events
        "query": {"match_all": {}}
    }
    res = es.search(index="nature_data*", body=query)
    events = []
    for hit in res["hits"]["hits"]:
        events.append(hit["_source"])
    return {"events": events}

@app.get("/")
def root():
    return {"message": " Disaster Globe API is running!"}
