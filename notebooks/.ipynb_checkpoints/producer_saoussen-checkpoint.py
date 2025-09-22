import json
import time
import requests
from kafka import KafkaProducer

API_URL = "https://api.aviationstack.com/v1/flights"
API_KEY = "188fb1f9da5402ba1d3386434d625d6b" 
TOPIC = "fly-saoussen"

producer = KafkaProducer(
            bootstrap_servers=['kafka1:19092','kafka2:19093','kafka3:19094'],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")  
        )





