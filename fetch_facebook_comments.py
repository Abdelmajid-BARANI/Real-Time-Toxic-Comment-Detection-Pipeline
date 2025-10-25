# fichier : fetch_facebook_comments.py

import requests
from kafka import KafkaProducer
import json

# --- Configuration (remplace ici) ---
ACCESS_TOKEN = "EAATQPWLZCNMkBPxqSA8ENn8w68rfTCGNEi9eR2OSMq3WAa8xuCrrG2mwOHtGmjoubnPEr1AxOClJOiXxLPiQINNgi48AlqCVfrLtdwtOWp3db0NsHloPdFWfJWOuDOddPjUYkZAXiMce0VLqZBh3mJZBsHMrZCbEFZCfjyp7AtWU5OYsnGDR6vYxrp688UeZCiONalQIBY8qTZAPjw8uyxDOV6ZCViJid3rrffORKcGGcmAZDZD"
PAGE_ID = "143748515489111"
KAFKA_TOPIC = "facebook_comments"
KAFKA_SERVER = "localhost:9092"

# --- Kafka Producer ---
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# --- Récupérer les commentaires ---
def get_comments():
    """Récupère les commentaires d'une page Facebook"""
    url = f"https://graph.facebook.com/v17.0/{PAGE_ID}/posts?fields=message,comments{{message,from,created_time}}&access_token={ACCESS_TOKEN}"
    response = requests.get(url)
    data = response.json()
    comments_list = []

    for post in data.get("data", []):
        comments = post.get("comments", {}).get("data", [])
        for comment in comments:
            comments_list.append({
                "post_id": post.get("id"),
                "comment_id": comment.get("id"),
                "user": comment.get("from", {}).get("name"),
                "message": comment.get("message"),
                "created_time": comment.get("created_time")
            })
    return comments_list

# --- Envoyer les commentaires à Kafka ---
def send_to_kafka(message):
    """Envoie un message à Kafka"""
    producer.send(KAFKA_TOPIC, message)
    producer.flush()
    
def fetch_and_send():
    comments = get_comments()
    for comment in comments:
        send_to_kafka(comment)
    print(f"{len(comments)} commentaires envoyés à Kafka.")

# --- Point d'entrée ---
if __name__ == "__main__":
    fetch_and_send()
