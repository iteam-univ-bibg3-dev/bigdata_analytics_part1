#!/usr/bin/env python3
"""
Real-Time Multi-Source Space & Nature Events Producer
- Collects live/recent data from free APIs (aurora, earthquakes, volcanoes, storms, floods, wildfires, ISS)
- Balances events by type for visual diversity
- Streams them to Kafka topic `nature_data`
"""

import json, time, random, requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from datetime import datetime


BROKERS = ["kafka1:9092", "kafka2:9093", "kafka3:9094"]
TOPIC = "nature_data"

# ---------------- Helper: safe HTTP get ----------------
def fetch_json(url):
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print("Error fetching JSON:", e)
    return None

# ---------------- Parse timestamp ----------------
def parse_date(d):
    if d is None:
        return None
    if isinstance(d, int) or isinstance(d, float):
        return d / 1000 if d > 1e12 else d
    if isinstance(d, str):
        try:
            return datetime.strptime(d, "%Y-%m-%dT%H:%M:%SZ").timestamp()
        except ValueError:
            try:
                return datetime.fromisoformat(d.replace("Z", "+00:00")).timestamp()
            except:
                return None
    return None


# ---------------- Individual fetchers ----------------

def fetch_aurora():
    """Aurora forecast (NOAA SWPC) — free, geospatial-ready, timestamps in seconds."""
    try:
        r = requests.get(
            "https://services.swpc.noaa.gov/json/ovation_aurora_latest.json",
            timeout=10
        ).json()

        coords_list = r.get("coordinates", [])
        ts_raw = r.get("time", time.time())  # NOAA timestamp in seconds
        ts = parse_date(ts_raw) or time.time()
        intensity = r.get("kp", 1.0)  # Kp index as proxy for intensity

        events = []

        # coords_list = [ [ [lon, lat], [lon, lat], ... ] ]
        for poly in coords_list:
            if not poly or not isinstance(poly[0], list):
                continue
            points = poly[0]  # flatten first polygon ring
            for lon, lat in points:
                events.append({
                    "source": "NOAA_SWPC",
                    "type": "aurora",
                    "latitude": lat,
                    "longitude": lon,
                    "altitude_km": 100,
                    "velocity_kms": 0,
                    "intensity": intensity,
                    "timestamp": ts,           # seconds since epoch
                    "description": "Aurora probability region (NOAA SWPC)"
                })

        # Limit to 50 points for visualization
        if len(events) > 50:
            events = random.sample(events, 50)

        print(f"Aurora: {len(events)} points, ts={ts}")
        return events

    except Exception as e:
        print(f"[NOAA Aurora] Error: {e}")
        return []



def fetch_earthquakes():
    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
    data = fetch_json(url)
    if not data: return []
    events = []
    for f in data["features"]:
        coords = f["geometry"]["coordinates"]
        props = f["properties"]
        events.append({
            "type": "earthquake",
            "latitude": coords[1],
            "longitude": coords[0],
            "intensity": props.get("mag"),
            "description": props.get("place"),
            "timestamp": props.get("time") / 1000
        })
    return events[:50]

def fetch_eonet(category, limit=50):
    url = f"https://eonet.gsfc.nasa.gov/api/v3/events?categories={category}"
    try:
        r = requests.get(url, timeout=30)  # increase timeout
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"Error fetching EONET {category}: {e}")
        return []

    events = []

    for ev in data.get("events", []):
        for g in ev.get("geometry", []):
            ts = parse_date(g.get("date"))
            if ts is None:
                print(f"Skipping {ev.get('title')} due to invalid date: {g.get('date')}")
                continue

            coords = g.get("coordinates")
            geom_type = g.get("type")

            lat = lon = None

            if geom_type == "Point" and isinstance(coords, list) and len(coords) >= 2:
                lon, lat = coords[0], coords[1]

            elif geom_type in ["Polygon", "LineString"] and isinstance(coords, list):
                # flatten and take centroid
                points = []
                def flatten(c):
                    if isinstance(c[0], list):
                        for x in c:
                            flatten(x)
                    else:
                        points.append(c)
                flatten(coords)
                if points:
                    lon = sum(p[0] for p in points) / len(points)
                    lat = sum(p[1] for p in points) / len(points)

            if lat is None or lon is None:
                continue

            events.append({
                "type": category.lower()[:-1] if category.endswith("s") else category.lower(),
                "latitude": lat,
                "longitude": lon,
                "description": ev.get("title", ""),
                "timestamp": ts
            })

    return events[:limit]

def fetch_iss():
    url = "http://api.open-notify.org/iss-now.json"
    data = fetch_json(url)
    if not data: return []
    pos = data.get("iss_position", {})
    return [{
        "type": "iss",
        "latitude": float(pos.get("latitude", 0)),
        "longitude": float(pos.get("longitude", 0)),
        "altitude_km": 408,
        "velocity_kms": 7.66,
        "description": "International Space Station",
        "timestamp": data.get("timestamp")
    }]

# ---------------- Kafka Producer ----------------
def create_producer():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=BROKERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        except NoBrokersAvailable:
            print("Waiting for Kafka...")
            time.sleep(3)

# ---------------- Main Loop ----------------
def main():
    producer = create_producer()
    print("Multi-Source Producer started.")

    while True:
        all_events = []
        all_events += fetch_aurora()
        all_events += fetch_eonet("volcanoes")
        all_events += fetch_eonet("severeStorms")
        all_events += fetch_eonet("floods")
        all_events += fetch_eonet("wildfires")
        all_events += fetch_earthquakes()
        all_events += fetch_iss()

        random.shuffle(all_events)
        for ev in all_events:
            producer.send(TOPIC, value=ev)
            print(f"** New {ev} sent to Kafka topic '{TOPIC}'.")

        producer.flush()
        print(f"Sent {len(all_events)} events to Kafka topic '{TOPIC}'.\n")
        time.sleep(60)

if __name__ == "__main__":
    main()
