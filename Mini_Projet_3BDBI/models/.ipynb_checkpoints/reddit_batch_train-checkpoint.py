import pandas as pd
from datetime import datetime
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.metrics import classification_report
import joblib

# === Nettoyage / normalisation du batch ===
def clean_batch(csv_path):
    df = pd.read_csv(csv_path)

    # created_utc : convertir en datetime
    df['created_utc'] = df['created_utc'].apply(
        lambda ts: datetime.utcfromtimestamp(ts) if isinstance(ts, (int,float)) else ts
    )
    
    # author : convertir en string
    df['author'] = df['author'].astype(str)
    
    # remplir les NaN pour le texte
    df['title'] = df['title'].fillna('')
    df['selftext'] = df['selftext'].fillna('')
    
    # concaténer title + selftext pour le texte complet
    df['text'] = df['title'] + ' ' + df['selftext']
    
    # target fictive pour exemple : score > 5
    df['label'] = (df['score'] > 5).astype(int)
    
    return df

# === Entraîner le modèle ML ===
def train_model(df, model_path="reddit_batch_model.pkl"):
    X_train, X_test, y_train, y_test = train_test_split(df['text'], df['label'], test_size=0.2, random_state=42)

    pipeline = Pipeline([
        ('tfidf', TfidfVectorizer(max_features=5000, stop_words='english')),
        ('clf', LogisticRegression(max_iter=500))
    ])

    pipeline.fit(X_train, y_train)
    y_pred = pipeline.predict(X_test)
    print(classification_report(y_test, y_pred))

    # Sauvegarder le modèle
    joblib.dump(pipeline, model_path)
    print(f"Modèle enregistré : {model_path}")

    return pipeline

# === MAIN ===
if __name__ == "__main__":
    csv_path = "../data/reddit_python.csv"

    # Nettoyage batch
    df = clean_batch(csv_path)

    # Entraîner modèle
    train_model(df)
