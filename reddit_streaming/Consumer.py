import json
from kafka import KafkaConsumer
from pymongo import MongoClient
from transformers import pipeline


# Initialiser le modèle NLP HuggingFace

sentiment_analyzer = pipeline("sentiment-analysis")

def analyze_sentiment(text):
    """Retourne Positif / Neutre / Négatif avec HuggingFace"""
    if not text.strip():
        return "Neutre"
    result = sentiment_analyzer(text[:512])[0]  
    label = result['label']
    if label == 'NEGATIVE':
        return "Négatif"
    elif label == 'POSITIVE':
        return "Positif"
    else:
        return "Neutre"


def main():
   
    # Connexion MongoDB
   
    try:
        mongo_client = MongoClient('mongodb://localhost:27017/')
        db = mongo_client['reddit_db']
        collection = db['iphone17_posts']
    except Exception as e:
        print(f" Erreur de connexion a MongoDB : {e}")
        return

    # Connexion Kafka
   
    try:
        consumer = KafkaConsumer(
            'iphone17-tweets',  
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='iphone17-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    except Exception as e:
        print(f"Erreur de connexion à Kafka : {e}")
        return

    print(" Consumer en écoute sur le topic 'iphone17-tweets'...\n")


    # Lire les messages et insérer dans MongoDB
    
    try:
        for message in consumer:
            data = message.value

            
            text = data.get('title', '') + " " + data.get('body', '')
            data['sentiment'] = analyze_sentiment(text)

            print(f"[{data.get('subreddit','inconnu')}] {data.get('title','')[:50]}... | Sentiment: {data['sentiment']}")

            collection.insert_one(data)

    except KeyboardInterrupt:
        print("\n Arrêt du consumer par l'utilisateur.")
    except Exception as e:
        print(f"Erreur pendant la consommation des messages : {e}")


# Lancer le consumer

if __name__ == "__main__":
    main()

