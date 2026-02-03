"""
============================================================================
PRODUCER - Ingestion Facebook → data.raw.stream
============================================================================
Collecte les commentaires Facebook et les publie sur le topic Kafka brut.

Message format:
{
  "source": "facebook",
  "post_id": "123",
  "comment_id": "456",
  "user": "John Doe",
  "message": "This is a comment",
  "created_time": "2025-01-01T10:00:00Z"
}
"""

import os
import sys
import json
import time
import signal
import logging
import hashlib
from datetime import datetime
from typing import Dict, List, Optional, Set

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ===========================================================================
# CONFIGURATION
# ===========================================================================

FACEBOOK_ACCESS_TOKEN = os.getenv(
    "FACEBOOK_ACCESS_TOKEN",
    "EAATQPWLZCNMkBQDZBomulx6iX8Jhp0vulFJXcYj2q8aYTes92wTLfbQifMoynVJzM3dj6rAnjtRsXVU6Fz0JdYy3qM76rdB4y3oyZCWGbULrZCAIZAG5DAZCZBMVtywNl4PD7f1VmiZA4wEKF2Y1KZCpIZCUy1sNO4UfshgBv2lV0FrTP92tIYdwsWpz4xUTu4BI0FXSu0nVusnOrNFQH7JolF8RA02EbUGHXKXzbADOUZD"
)
FACEBOOK_PAGE_ID = os.getenv("FACEBOOK_PAGE_ID", "143748515489111")
FACEBOOK_API_VERSION = "v17.0"
FACEBOOK_BASE_URL = f"https://graph.facebook.com/{FACEBOOK_API_VERSION}"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "data.raw.stream")

POLLING_INTERVAL = int(os.getenv("POLLING_INTERVAL", "15"))
MAX_COMMENTS_PER_REQUEST = 100

# ===========================================================================
# LOGGING
# ===========================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ===========================================================================
# PRODUCER CLASS
# ===========================================================================

class FacebookKafkaProducer:
    """Producteur Kafka pour les commentaires Facebook bruts"""
    
    def __init__(self):
        self.producer: Optional[KafkaProducer] = None
        self.seen_comments: Set[str] = set()
        self.running: bool = False
        self.stats = {
            "total_fetched": 0,
            "total_sent": 0,
            "duplicates_skipped": 0,
            "errors": 0
        }
        
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        logger.info("🛑 Signal d'arrêt reçu...")
        self.running = False
    
    def connect_kafka(self, max_retries: int = 10) -> bool:
        """Établit la connexion à Kafka avec retry"""
        for attempt in range(max_retries):
            try:
                logger.info(f"📡 Connexion Kafka (tentative {attempt + 1}/{max_retries})...")
                
                self.producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',
                    retries=3,
                    retry_backoff_ms=1000,
                    max_block_ms=10000,
                )
                logger.info(f"✅ Connecté à Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
                return True
                
            except KafkaError as e:
                logger.warning(f"⚠️ Échec connexion Kafka: {e}")
                time.sleep(5)
        
        logger.error("❌ Impossible de se connecter à Kafka")
        return False
    
    def fetch_posts(self) -> List[Dict]:
        """Récupère les posts de la page Facebook"""
        url = f"{FACEBOOK_BASE_URL}/{FACEBOOK_PAGE_ID}/posts"
        params = {
            "access_token": FACEBOOK_ACCESS_TOKEN,
            "fields": "id,message,created_time",
            "limit": 25
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            data = response.json()
            
            if "error" in data:
                logger.error(f"❌ Erreur API Facebook: {data['error'].get('message')}")
                return []
            
            posts = data.get("data", [])
            logger.info(f"📄 {len(posts)} posts récupérés")
            return posts
            
        except Exception as e:
            logger.error(f"❌ Erreur fetch posts: {e}")
            return []
    
    def fetch_comments(self, post_id: str) -> List[Dict]:
        """Récupère les commentaires d'un post"""
        url = f"{FACEBOOK_BASE_URL}/{post_id}/comments"
        params = {
            "access_token": FACEBOOK_ACCESS_TOKEN,
            "fields": "id,message,from,created_time",
            "limit": MAX_COMMENTS_PER_REQUEST
        }
        
        comments = []
        
        try:
            while url:
                response = requests.get(url, params=params, timeout=30)
                data = response.json()
                
                if "error" in data:
                    logger.warning(f"⚠️ Erreur commentaires: {data['error'].get('message')}")
                    break
                
                comments.extend(data.get("data", []))
                
                # Pagination
                paging = data.get("paging", {})
                url = paging.get("next")
                params = {}  # URL next contient déjà les params
            
            return comments
            
        except Exception as e:
            logger.error(f"❌ Erreur fetch comments: {e}")
            return []
    
    def format_raw_message(self, post_id: str, comment: Dict) -> Dict:
        """Formate un commentaire au format brut pour Kafka"""
        return {
            "source": "facebook",
            "post_id": post_id,
            "comment_id": comment.get("id", ""),
            "user": comment.get("from", {}).get("name", "Unknown"),
            "message": comment.get("message", ""),
            "created_time": comment.get("created_time", datetime.utcnow().isoformat())
        }
    
    def send_to_kafka(self, message: Dict) -> bool:
        """Envoie un message au topic Kafka"""
        comment_id = message.get("comment_id", "")
        
        # Déduplication
        if comment_id in self.seen_comments:
            self.stats["duplicates_skipped"] += 1
            return False
        
        try:
            future = self.producer.send(
                KAFKA_TOPIC_RAW,
                key=comment_id,
                value=message
            )
            future.get(timeout=10)
            
            self.seen_comments.add(comment_id)
            self.stats["total_sent"] += 1
            
            logger.info(f"📤 → {KAFKA_TOPIC_RAW}: {message['user'][:20]}... | {message['message'][:50]}...")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur envoi Kafka: {e}")
            self.stats["errors"] += 1
            return False
    
    def fetch_and_publish(self):
        """Récupère les commentaires Facebook et les publie"""
        posts = self.fetch_posts()
        
        for post in posts:
            post_id = post.get("id")
            if not post_id:
                continue
            
            comments = self.fetch_comments(post_id)
            self.stats["total_fetched"] += len(comments)
            
            for comment in comments:
                if comment.get("message"):
                    raw_message = self.format_raw_message(post_id, comment)
                    self.send_to_kafka(raw_message)
    
    def run(self):
        """Boucle principale du producteur"""
        logger.info("🚀 Démarrage du Producer Facebook → Kafka")
        logger.info(f"   Topic: {KAFKA_TOPIC_RAW}")
        logger.info(f"   Interval: {POLLING_INTERVAL}s")
        
        if not self.connect_kafka():
            sys.exit(1)
        
        self.running = True
        
        while self.running:
            try:
                logger.info("🔄 Récupération des commentaires Facebook...")
                self.fetch_and_publish()
                
                logger.info(f"📊 Stats: {self.stats['total_sent']} envoyés, "
                           f"{self.stats['duplicates_skipped']} dupliqués, "
                           f"{self.stats['errors']} erreurs")
                
                logger.info(f"⏳ Pause {POLLING_INTERVAL}s...")
                time.sleep(POLLING_INTERVAL)
                
            except Exception as e:
                logger.error(f"❌ Erreur boucle principale: {e}")
                time.sleep(5)
        
        self.cleanup()
    
    def cleanup(self):
        """Nettoyage des ressources"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
        logger.info("🛑 Producer arrêté proprement")

# ===========================================================================
# TEST MODE - Génère des commentaires de test
# ===========================================================================

class TestProducer(FacebookKafkaProducer):
    """Producer de test avec commentaires simulés"""
    
    TEST_COMMENTS = [
        {"user": "Alice", "message": "Great post! I love this content."},
        {"user": "Bob", "message": "This is terrible, you're so stupid!"},
        {"user": "Charlie", "message": "Interesting perspective on the topic."},
        {"user": "Dave", "message": "I hate everything about this!"},
        {"user": "Eve", "message": "Beautiful work, keep it up!"},
        {"user": "Frank", "message": "You're an idiot, this is garbage."},
        {"user": "Grace", "message": "Thanks for sharing this information."},
        {"user": "Henry", "message": "Die in a fire, moron!"},
        {"user": "Ivy", "message": "Very insightful analysis."},
        {"user": "Jack", "message": "This is so offensive and racist!"},
    ]
    
    def __init__(self):
        super().__init__()
        self.comment_index = 0
    
    def fetch_and_publish(self):
        """Génère et publie des commentaires de test"""
        for _ in range(5):  # 5 commentaires par cycle
            test_comment = self.TEST_COMMENTS[self.comment_index % len(self.TEST_COMMENTS)]
            
            comment_id = hashlib.md5(
                f"{test_comment['message']}{datetime.utcnow().isoformat()}".encode()
            ).hexdigest()[:16]
            
            raw_message = {
                "source": "facebook_test",
                "post_id": f"test_post_{self.comment_index // 10}",
                "comment_id": comment_id,
                "user": test_comment["user"],
                "message": test_comment["message"],
                "created_time": datetime.utcnow().isoformat() + "Z"
            }
            
            self.send_to_kafka(raw_message)
            self.comment_index += 1
            time.sleep(0.5)

# ===========================================================================
# MAIN
# ===========================================================================

if __name__ == "__main__":
    mode = os.getenv("PRODUCER_MODE", "test")
    
    if mode == "facebook":
        producer = FacebookKafkaProducer()
    else:
        logger.info("🧪 Mode TEST activé (commentaires simulés)")
        producer = TestProducer()
    
    producer.run()
