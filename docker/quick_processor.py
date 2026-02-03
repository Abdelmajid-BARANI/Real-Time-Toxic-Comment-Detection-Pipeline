#!/usr/bin/env python3
"""
Processeur rapide - Contourne Spark pour tests
Lit depuis Kafka et écrit directement dans PostgreSQL
"""
import json
import re
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_SERVERS = "kafka:9092"
KAFKA_TOPIC = "data.raw.stream"
POSTGRES_CONFIG = {
    'host': 'postgres',
    'port': 5432,
    'database': 'toxic_coments_db',
    'user': 'postgres',
    'password': 'majid2020'
}

def clean_text(text):
    """Nettoyage simple du texte"""
    text = text.lower()
    text = re.sub(r'http\S+', '', text)
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def simple_toxicity_check(text):
    """Détection simple de toxicité par mots-clés"""
    toxic_words = ['fuck', 'shit', 'ass', 'gay', 'bitch', 'damn', 'hell', 'stupid', 'idiot', 'hate']
    text_lower = text.lower()
    score = sum(1 for word in toxic_words if word in text_lower) / max(len(text.split()), 1)
    is_toxic = score > 0.1
    return min(score * 2, 1.0), is_toxic

def simple_sentiment(text):
    """Analyse de sentiment simple"""
    positive_words = ['good', 'great', 'love', 'excellent', 'happy', 'nice', 'smart']
    negative_words = ['bad', 'hate', 'terrible', 'awful', 'sad', 'fuck', 'shit']
    
    text_lower = text.lower()
    pos_count = sum(1 for word in positive_words if word in text_lower)
    neg_count = sum(1 for word in negative_words if word in text_lower)
    
    if pos_count > neg_count:
        return 'positive', 0.7
    elif neg_count > pos_count:
        return 'negative', 0.3
    else:
        return 'neutral', 0.5

def process_message(msg, conn):
    """Traite un message et l'insère dans PostgreSQL"""
    try:
        data = json.loads(msg.value.decode('utf-8'))
        
        comment = data['message']
        cleaned = clean_text(comment)
        toxicity_score, is_toxic = simple_toxicity_check(comment)
        sentiment, sentiment_score = simple_sentiment(comment)
        
        label = 'toxique' if is_toxic else 'non_toxique'
        word_count = len(comment.split())
        char_count = len(comment)
        
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO facebook_comments_analysis 
            (post_id, comment_id, username, comment, cleaned_comment, toxicity_score, 
             label, is_toxic, sentiment, sentiment_score, word_count, char_count, 
             created_time, ingestion_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (comment_id) 
            DO UPDATE SET
                toxicity_score = EXCLUDED.toxicity_score,
                label = EXCLUDED.label,
                is_toxic = EXCLUDED.is_toxic,
                sentiment = EXCLUDED.sentiment,
                sentiment_score = EXCLUDED.sentiment_score,
                ingestion_time = EXCLUDED.ingestion_time
        """, (
            data['post_id'],
            data['comment_id'],
            data['user'],
            comment,
            cleaned,
            float(toxicity_score),
            label,
            is_toxic,
            sentiment,
            float(sentiment_score),
            word_count,
            char_count,
            data['created_time'],
            datetime.now()
        ))
        conn.commit()
        cursor.close()
        
        logger.info(f"✅ Traité: {data['user'][:20]} | {comment[:40]} | toxic={is_toxic}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur: {e}")
        conn.rollback()
        return False

def main():
    logger.info("🚀 Démarrage processeur rapide...")
    
    # Connexion Kafka
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        auto_offset_reset='earliest',
        group_id='quick_processor'
    )
    
    # Connexion PostgreSQL
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    
    logger.info(f"📡 Écoute sur {KAFKA_TOPIC}")
    
    count = 0
    for message in consumer:
        if process_message(message, conn):
            count += 1
            if count % 10 == 0:
                logger.info(f"📊 {count} messages traités")
    
    conn.close()

if __name__ == "__main__":
    main()
