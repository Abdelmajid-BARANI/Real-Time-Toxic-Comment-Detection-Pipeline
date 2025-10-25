# fichier : predict_api.py

from kafka import KafkaConsumer
import json
import numpy as np
from tensorflow.keras.models import load_model
from tensorflow.keras.preprocessing.sequence import pad_sequences
import joblib
import re
import string
import nltk
from nltk.tokenize import TweetTokenizer

# Télécharger les stopwords si nécessaire
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')

from nltk.corpus import stopwords

# --- Configuration ---
KAFKA_TOPIC = "facebook_comments"
KAFKA_SERVER = "localhost:9092"
MODEL_PATH = r"C:\Users\abdel\OneDrive\Desktop\0Stage PFA\facebook-moderation\model\toxic_model.h5"
TOKENIZER_PATH = r"C:\Users\abdel\OneDrive\Desktop\0Stage PFA\facebook-moderation\model\tokenizer.pkl"

# Paramètres du modèle (doivent correspondre à ceux utilisés lors de l'entraînement)
max_length = 20
trunc_type = 'post'
padding_type = 'post'

# --- Charger le modèle et le tokenizer ---
print("🔄 Chargement du modèle et du tokenizer...")
model = load_model(MODEL_PATH)
tokenizerDL = joblib.load(TOKENIZER_PATH)
print("✅ Modèle et tokenizer chargés avec succès.")

# --- Fonctions de nettoyage (identiques à celles du notebook) ---
tokenizer = TweetTokenizer(strip_handles=True)
stop_words = stopwords.words('english')

def clean(tweet):
    """Nettoie le texte"""
    # Contractions
    tweet = re.sub(r"he's", "he is", tweet)
    tweet = re.sub(r"there's", "there is", tweet)
    tweet = re.sub(r"We're", "We are", tweet)
    tweet = re.sub(r"That's", "That is", tweet)
    tweet = re.sub(r"won't", "will not", tweet)
    tweet = re.sub(r"they're", "they are", tweet)
    tweet = re.sub(r"Can't", "Cannot", tweet)
    tweet = re.sub(r"wasn't", "was not", tweet)
    tweet = re.sub(r"aren't", "are not", tweet)
    tweet = re.sub(r"isn't", "is not", tweet)
    tweet = re.sub(r"What's", "What is", tweet)
    tweet = re.sub(r"haven't", "have not", tweet)
    tweet = re.sub(r"hasn't", "has not", tweet)
    tweet = re.sub(r"can't", "cannot", tweet)
    tweet = re.sub(r"don't", "do not", tweet)
    tweet = re.sub(r"you're", "you are", tweet)
    tweet = re.sub(r"i've", "I have", tweet)
    tweet = re.sub(r"that's", "that is", tweet)
    tweet = re.sub(r"doesn't", "does not", tweet)
    tweet = re.sub(r"didn't", "did not", tweet)
    
    # Character entity references
    tweet = re.sub(r"&gt;", ">", tweet)
    tweet = re.sub(r"&lt;", "<", tweet)
    tweet = re.sub(r"&amp;", "&", tweet)
    
    # URLs
    tweet = re.sub(r"http\S+", "", tweet)
    
    # Numbers
    tweet = re.sub(r'[0-9]', '', tweet)
    
    # Mentions
    tweet = re.sub(r"(@[A-Za-z0-9_]+)", "", tweet)
    
    # Remove punctuation (keep '!')
    for p in string.punctuation.replace('!', ''):
        tweet = tweet.replace(p, '')
    
    # Tokenize
    tweet_words = tokenizer.tokenize(tweet)
    
    # Remove short words
    tweet = [w for w in tweet_words if len(w) > 2]
    
    # Remove stopwords
    tweet = [w.lower() for w in tweet if w.lower() not in stop_words]
    
    # Join back
    tweet = ' '.join(tweet)
    
    return tweet

def prepare_string(tweet):
    """Prépare le texte pour la prédiction"""
    tweet = clean(tweet)
    return tweet

def predict_toxicity(comment):
    """Prédit la toxicité d'un commentaire"""
    labels_name = ["Toxic", "Severe toxic", "Obscene", "Threat", "Insult", "Identity hate"]
    
    # Préparation du commentaire
    modify_comment = [prepare_string(comment)]
    modify_comment = tokenizerDL.texts_to_sequences(modify_comment)
    modify_comment = np.array(pad_sequences(
        modify_comment, 
        maxlen=max_length, 
        padding=padding_type, 
        truncating=trunc_type
    ))
    
    # Prédiction
    prediction = model.predict(modify_comment, verbose=0)[0]
    prediction = (prediction > 0.5).astype(int)
    
    # Convertir en dictionnaire
    result = dict(zip(labels_name, prediction.tolist()))
    return result

# --- Kafka Consumer ---
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='toxicity-predictor-group'
)

print(f"🎧 Écoute des messages sur le topic '{KAFKA_TOPIC}'...")

# --- Traitement des messages ---
try:
    for message in consumer:
        comment_data = message.value
        
        # Extraire le texte du commentaire
        comment_text = comment_data.get("message", "")
        
        if not comment_text:
            print("⚠️  Commentaire vide ignoré")
            continue
        
        # Prédire la toxicité
        prediction = predict_toxicity(comment_text)
        
        # Afficher le résultat
        print("\n" + "="*80)
        print(f"📝 Commentaire : {comment_text}")
        print(f"👤 Utilisateur : {comment_data.get('user', 'Inconnu')}")
        print(f"📅 Date : {comment_data.get('created_time', 'Inconnue')}")
        print(f"🔍 Prédiction : {prediction}")
        
        # Vérifier si le commentaire est toxique
        is_toxic = any(prediction.values())
        if is_toxic:
            print("⚠️  ALERTE : Commentaire potentiellement toxique détecté!")
            toxic_categories = [k for k, v in prediction.items() if v == 1]
            print(f"   Catégories : {', '.join(toxic_categories)}")
        else:
            print("✅ Commentaire sain")
        print("="*80)

except KeyboardInterrupt:
    print("\n🛑 Arrêt du consommateur Kafka...")
finally:
    consumer.close()
    print("✅ Consommateur Kafka fermé.")