import json
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime
import re

POSTGRES_CONFIG = {
    'host': 'postgres', 
    'port': 5432, 
    'database': 'toxic_coments_db', 
    'user': 'postgres', 
    'password': 'majid2020'
}

def clean(t):
    return re.sub(r'\s+', ' ', re.sub(r'[^\w\s]', ' ', t.lower())).strip()

def is_toxic(t):
    return any(w in t.lower() for w in ['fuck', 'shit', 'ass', 'bitch', 'gay', 'hate'])

consumer = KafkaConsumer(
    'data.raw.stream', 
    bootstrap_servers='kafka:9092', 
    auto_offset_reset='earliest', 
    consumer_timeout_ms=15000, 
    group_id='quick_fix_v2'
)

conn = psycopg2.connect(**POSTGRES_CONFIG)
cursor = conn.cursor()

count = 0
for msg in consumer:
    try:
        d = json.loads(msg.value)
        toxic = is_toxic(d['message'])
        
        cursor.execute('''
            INSERT INTO facebook_comments_analysis 
            (post_id, comment_id, username, comment, cleaned_comment, toxicity_score, 
             label, is_toxic, sentiment, sentiment_score, word_count, char_count, 
             created_time, ingestion_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (comment_id) 
            DO UPDATE SET 
                toxicity_score = EXCLUDED.toxicity_score, 
                ingestion_time = EXCLUDED.ingestion_time
        ''', (
            d['post_id'], d['comment_id'], d['user'], d['message'], clean(d['message']), 
            0.8 if toxic else 0.1, 
            'toxique' if toxic else 'non_toxique', 
            toxic, 
            'negative' if toxic else 'neutral', 
            0.2 if toxic else 0.5,
            len(d['message'].split()), 
            len(d['message']), 
            d['created_time'], 
            datetime.now()
        ))
        
        conn.commit()
        count += 1
        username = d['user'][:15]
        message = d['message'][:30]
        print(f'OK {count}: {username} - {message}')
        
    except Exception as e:
        print(f'ERR: {str(e)[:50]}')
        conn.rollback()

cursor.close()
conn.close()
print(f'Total traite: {count} commentaires')
