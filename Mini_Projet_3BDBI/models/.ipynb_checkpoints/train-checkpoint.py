import pandas as pd
from datetime import datetime
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.metrics import classification_report
import joblib
import re

# === Nettoyage / normalisation du batch ===
def clean_batch(csv_path):
    df = pd.read_csv(csv_path)

    # Nettoyage de base
    df['created_utc'] = df['created_utc'].apply(
        lambda ts: datetime.utcfromtimestamp(ts) if isinstance(ts, (int,float)) else ts
    )
    df['author'] = df['author'].astype(str)
    df['title'] = df['title'].fillna('')
    df['selftext'] = df['selftext'].fillna('')

    # Concaténer title + selftext
    df['text'] = df['title'] + ' ' + df['selftext']

    # Nettoyage du texte
    df['text'] = df['text'].apply(lambda x: re.sub(r'http\S+|www.\S+', '', x))  # Supprimer les liens
    df['text'] = df['text'].apply(lambda x: re.sub(r'[^a-zA-Z\s]', '', x))       # Enlever la ponctuation
    df['text'] = df['text'].str.lower()                                          # Minuscule

    # Label : score > 5
    df['label'] = (df['score'] > 5).astype(int)
    return df

# === Entraînement amélioré ===
def train_model(df, model_path="reddit_batch_model1.pkl"):
    X_train, X_test, y_train, y_test = train_test_split(
        df['text'], df['label'], test_size=0.2, random_state=42, stratify=df['label']
    )

    pipeline = Pipeline([
        ('tfidf', TfidfVectorizer(
            ngram_range=(1,2),          # Ajouter bigrams
            max_features=10000,         # Plus de vocabulaire
            stop_words='english'
        )),
        ('clf', LogisticRegression(
            max_iter=1000,
            class_weight='balanced'     # Gère le déséquilibre
        ))
    ])

    # === Recherche d’hyperparamètres ===
    params = {
        'clf__C': [0.1, 1, 10],        # Force de régularisation
        'tfidf__min_df': [2, 5],       # Ignore mots trop rares
    }

    grid = GridSearchCV(
        pipeline, params, cv=5, scoring='accuracy', n_jobs=-1, verbose=1
    )
    grid.fit(X_train, y_train)

    print("🔎 Meilleurs paramètres :", grid.best_params_)
    y_pred = grid.predict(X_test)
    print("\n=== Rapport de classification ===")
    print(classification_report(y_test, y_pred))

    joblib.dump(grid.best_estimator_, model_path)
    print(f"\n✅ Modèle optimisé enregistré : {model_path}")

    return grid.best_estimator_

# === MAIN ===
if __name__ == "__main__":
    csv_path = "../data/reddit_python.csv"
    df = clean_batch(csv_path)
    train_model(df)
