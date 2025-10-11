#!/bin/sh
set -e

ES_URL="http://elasticsearch:9200"
INDEX_NAME="nature_data"
TEMPLATE_FILE="/nature_events_template.json"
PIPELINE_NAME="nature_data_geo"

echo "Waiting for Elasticsearch to be ready at $ES_URL ..."
until curl -s "$ES_URL" >/dev/null; do
  sleep 3
done
echo "Elasticsearch is up!"

# ---------------- Create ingest pipeline ----------------
echo "Creating ingest pipeline '$PIPELINE_NAME'..."
curl -sf -X PUT "$ES_URL/_ingest/pipeline/$PIPELINE_NAME" \
  -H 'Content-Type: application/json' \
  -d '{
    "description": "Generate geo_point from latitude and longitude for multi-source events",
    "processors": [
      { "convert": { "field": "latitude", "type": "float", "ignore_missing": true } },
      { "convert": { "field": "longitude", "type": "float", "ignore_missing": true } },
      { "set": { "field": "geometry", "value": "{{latitude}},{{longitude}}", "ignore_empty_value": true } },
      { "convert": { "field": "altitude_km", "type": "float", "ignore_missing": true } },
      { "convert": { "field": "velocity_kms", "type": "float", "ignore_missing": true } },
      { "convert": { "field": "intensity", "type": "float", "ignore_missing": true } },
      { "rename": { "field": "details", "target_field": "metadata", "ignore_missing": true } }
    ]
  }' \
  && echo "Ingest pipeline created or updated."

# ---------------- Apply index template ----------------
if [ -f "$TEMPLATE_FILE" ]; then
  echo "Applying index template..."
  curl -sf -X PUT "$ES_URL/_index_template/${INDEX_NAME}_template" \
    -H 'Content-Type: application/json' \
    -d @"$TEMPLATE_FILE" \
    && echo "Template applied."
else
  echo "Template file '$TEMPLATE_FILE' not found — skipping template step."
fi

# ---------------- Create index if it does not exist ----------------
INDEX_EXISTS=$(curl -s -o /dev/null -w "%{http_code}" "$ES_URL/$INDEX_NAME")
if [ "$INDEX_EXISTS" = "404" ]; then
  echo "Creating index '$INDEX_NAME' with ingest pipeline..."
  curl -sf -X PUT "$ES_URL/$INDEX_NAME?pipeline=$PIPELINE_NAME" \
    -H 'Content-Type: application/json' \
    -d '{
      "settings": {
        "number_of_shards": 3,
        "number_of_replicas": 1,
        "refresh_interval": "30s"
      },
      "mappings": {
        "dynamic": true,
        "properties": {
          "type":        { "type": "keyword" },
          "source":      { "type": "keyword", "null_value": "unknown" },
          "satellite_name": { "type": "keyword", "null_value": "N/A" },
          "latitude":    { "type": "float" },
          "longitude":   { "type": "float" },
          "altitude_km": { "type": "float", "null_value": 0 },
          "velocity_kms": { "type": "float", "null_value": 0 },
          "intensity":   { "type": "float", "null_value": 0 },
          "description": { "type": "text", "analyzer": "standard" },
          "timestamp":   { "type": "date", "format": "epoch_millis" },
          "geometry":    { "type": "geo_point" },
          "metadata":    { "type": "object", "enabled": true }
        }
      }
    }' \
    && echo "Index created successfully."
else
  echo "Index '$INDEX_NAME' already exists — skipping creation."
  # Ensure pipeline is attached to index
  curl -sf -X PUT "$ES_URL/$INDEX_NAME/_settings" \
    -H 'Content-Type: application/json' \
    -d '{"index.default_pipeline":"'"$PIPELINE_NAME"'"}' \
    && echo "Ingest pipeline attached to existing index."
fi

echo "Elasticsearch initialization completed."
