"""
============================================================================
PRODUCTEUR KAFKA - Collecte des Commentaires Facebook en Temps Réel
============================================================================
Utilise la Facebook Graph API pour collecter les commentaires
et les publier sur un topic Kafka.

Fonctionnalités:
- Pagination automatique
- Mode temps réel (polling)
- Gestion des erreurs et retry
- Déduplication des commentaires
- Logging détaillé
"""

import os
import sys
import json
import time
import signal
import logging
from datetime import datetime
from typing import Dict, List, Optional, Set

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ===========================================================================
# CONFIGURATION
# ===========================================================================

# Facebook API
ACCESS_TOKEN = os.getenv(
    "FACEBOOK_ACCESS_TOKEN",
    "EAATQPWLZCNMkBQEzRmzTCeG8ZAih1zHwen9h5ZA6Q4fuG63PyUJUgccWsMsjQMuGkUW1NAb5PfeBlIsJtsX3dyijzBhqQpg0HYuhjqBVIWiSdjhjkJGmdMopraMtwV3ZCr8A1mwMZB65Lb1CUulZAiY7NeZCBdDxLvf1jZBWhFNNOh0P3tIU0OZBa8eGggGpxFlpA5sEMog3njILhS0seiCfFPdcHJZA78ckOw73zytgZDZD"  # Remplacer par votre token
)
PAGE_ID = os.getenv("FACEBOOK_PAGE_ID", "143748515489111")
API_VERSION = "v17.0"
BASE_URL = f"https://graph.facebook.com/{API_VERSION}"

# Kafka - Utiliser 127.0.0.1 au lieu de localhost pour éviter les problèmes IPv6
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092").split(",")
KAFKA_TOPIC = "facebook_comments"

# Mode temps réel
POLLING_INTERVAL = int(os.getenv("POLLING_INTERVAL", "30"))  # secondes
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
# CLASSE PRINCIPALE
# ===========================================================================

class FacebookKafkaProducer:
    """
    Producteur Kafka pour les commentaires Facebook.
    
    Collecte les commentaires via la Graph API et les publie
    sur un topic Kafka en temps réel.
    """
    
    def __init__(self):
        """Initialise le producteur"""
        self.producer: Optional[KafkaProducer] = None
        self.seen_comments: Set[str] = set()  # Pour déduplication
        self.running: bool = False
        self.stats = {
            "total_fetched": 0,
            "total_sent": 0,
            "duplicates_skipped": 0,
            "errors": 0,
            "start_time": None
        }
        
        # Gestion du signal d'arrêt
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Gère l'arrêt propre"""
        logger.info("🛑 Signal d'arrêt reçu...")
        self.running = False
    
    def connect_kafka(self) -> bool:
        """Établit la connexion à Kafka"""
        logger.info(f"📡 Connexion à Kafka: {KAFKA_SERVERS}")
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                retry_backoff_ms=1000,
                max_block_ms=60000,
            )
            logger.info("✅ Connexion Kafka établie")
            return True
            
        except KafkaError as e:
            logger.error(f"❌ Erreur Kafka: {e}")
            return False
    
    def validate_token(self) -> bool:
        """Valide le token d'accès Facebook"""
        logger.info("🔑 Validation du token Facebook...")
        
        url = f"{BASE_URL}/debug_token"
        params = {
            "input_token": ACCESS_TOKEN,
            "access_token": ACCESS_TOKEN
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            if "data" in data and data["data"].get("is_valid"):
                logger.info("✅ Token valide")
                return True
            else:
                error = data.get("error", {}).get("message", "Token invalide")
                logger.error(f"❌ {error}")
                return False
                
        except Exception as e:
            logger.warning(f"⚠️ Impossible de valider le token: {e}")
            return True  # Continuer quand même
    
    def fetch_posts(self) -> List[Dict]:
        """Récupère les posts de la page"""
        url = f"{BASE_URL}/{PAGE_ID}/posts"
        params = {
            "access_token": ACCESS_TOKEN,
            "fields": "id,message,created_time",
            "limit": 25
        }
        
        posts = []
        
        try:
            while url:
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                posts.extend(data.get("data", []))
                
                # Pagination
                paging = data.get("paging", {})
                url = paging.get("next")
                params = {}  # L'URL next contient déjà les params
                
                if len(posts) >= 50:  # Limiter pour éviter trop de requêtes
                    break
                    
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erreur récupération posts: {e}")
            self.stats["errors"] += 1
        
        return posts
    
    def fetch_comments(self, post_id: str) -> List[Dict]:
        """Récupère les commentaires d'un post avec pagination"""
        url = f"{BASE_URL}/{post_id}/comments"
        params = {
            "access_token": ACCESS_TOKEN,
            "fields": "id,message,from,created_time,like_count",
            "limit": MAX_COMMENTS_PER_REQUEST
        }
        
        comments = []
        
        try:
            while url:
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                for comment in data.get("data", []):
                    comments.append({
                        "post_id": post_id,
                        "comment_id": comment.get("id"),
                        "username": comment.get("from", {}).get("name", "Unknown"),
                        "message": comment.get("message", ""),
                        "created_time": comment.get("created_time"),
                        "like_count": comment.get("like_count", 0),
                        "fetched_at": datetime.utcnow().isoformat()
                    })
                
                # Pagination
                paging = data.get("paging", {})
                url = paging.get("next")
                params = {}
                
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erreur récupération commentaires pour {post_id}: {e}")
            self.stats["errors"] += 1
        
        return comments
    
    def fetch_all_comments(self) -> List[Dict]:
        """Récupère tous les commentaires de la page"""
        all_comments = []
        
        logger.info(f"📥 Récupération des posts de la page {PAGE_ID}...")
        posts = self.fetch_posts()
        logger.info(f"   📝 {len(posts)} posts trouvés")
        
        for post in posts:
            post_id = post.get("id")
            if post_id:
                comments = self.fetch_comments(post_id)
                all_comments.extend(comments)
                self.stats["total_fetched"] += len(comments)
        
        logger.info(f"   💬 {len(all_comments)} commentaires récupérés au total")
        return all_comments
    
    def send_to_kafka(self, comment: Dict) -> bool:
        """Envoie un commentaire à Kafka"""
        comment_id = comment.get("comment_id")
        
        # Déduplication
        if comment_id in self.seen_comments:
            self.stats["duplicates_skipped"] += 1
            return False
        
        try:
            future = self.producer.send(
                KAFKA_TOPIC,
                key=comment_id,
                value=comment
            )
            
            # Attendre la confirmation
            future.get(timeout=10)
            
            self.seen_comments.add(comment_id)
            self.stats["total_sent"] += 1
            
            return True
            
        except KafkaError as e:
            logger.error(f"❌ Erreur envoi Kafka: {e}")
            self.stats["errors"] += 1
            return False
    
    def process_batch(self, comments: List[Dict]) -> int:
        """Traite un batch de commentaires"""
        sent_count = 0
        
        for comment in comments:
            if self.send_to_kafka(comment):
                sent_count += 1
                
                # Afficher le commentaire
                username = comment.get("username", "Unknown")
                message = comment.get("message", "")[:50]
                logger.info(f"   📤 [{username}]: {message}...")
        
        # Flush pour s'assurer que tout est envoyé
        self.producer.flush()
        
        return sent_count
    
    def print_stats(self):
        """Affiche les statistiques"""
        runtime = ""
        if self.stats["start_time"]:
            elapsed = datetime.now() - self.stats["start_time"]
            runtime = f" (Runtime: {elapsed})"
        
        logger.info(f"""
╔═══════════════════════════════════════════════════════════════╗
║                    📊 STATISTIQUES{runtime:^27}║
╠═══════════════════════════════════════════════════════════════╣
║  Total récupérés:     {self.stats['total_fetched']:>10}                          ║
║  Total envoyés:       {self.stats['total_sent']:>10}                          ║
║  Doublons ignorés:    {self.stats['duplicates_skipped']:>10}                          ║
║  Erreurs:             {self.stats['errors']:>10}                          ║
╚═══════════════════════════════════════════════════════════════╝
        """)
    
    def run_once(self):
        """Exécute une seule collecte"""
        logger.info("🔄 Collecte unique des commentaires...")
        
        comments = self.fetch_all_comments()
        sent = self.process_batch(comments)
        
        logger.info(f"✅ {sent} nouveaux commentaires envoyés à Kafka")
        self.print_stats()
    
    def run_continuous(self):
        """Exécute en mode continu (temps réel)"""
        self.stats["start_time"] = datetime.now()
        self.running = True
        
        logger.info(f"""
╔═══════════════════════════════════════════════════════════════╗
║       🚀 DÉMARRAGE DU PRODUCTEUR KAFKA (Mode Temps Réel)      ║
╠═══════════════════════════════════════════════════════════════╣
║  Page Facebook: {PAGE_ID:<45}║
║  Topic Kafka:   {KAFKA_TOPIC:<45}║
║  Intervalle:    {POLLING_INTERVAL} secondes{' '*37}║
╚═══════════════════════════════════════════════════════════════╝
        """)
        
        while self.running:
            try:
                logger.info(f"🔄 Nouvelle collecte ({datetime.now().strftime('%H:%M:%S')})...")
                
                comments = self.fetch_all_comments()
                sent = self.process_batch(comments)
                
                if sent > 0:
                    logger.info(f"✅ {sent} nouveaux commentaires envoyés")
                else:
                    logger.info("ℹ️  Aucun nouveau commentaire")
                
                # Attendre avant la prochaine collecte
                logger.info(f"💤 Prochaine collecte dans {POLLING_INTERVAL} secondes...")
                
                for _ in range(POLLING_INTERVAL):
                    if not self.running:
                        break
                    time.sleep(1)
                    
            except Exception as e:
                logger.error(f"❌ Erreur dans la boucle principale: {e}")
                self.stats["errors"] += 1
                time.sleep(5)
        
        self.print_stats()
        logger.info("👋 Arrêt du producteur")
    
    def close(self):
        """Ferme les connexions"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("🔌 Connexion Kafka fermée")

# ===========================================================================
# POINT D'ENTRÉE
# ===========================================================================

def main():
    """Point d'entrée principal"""
    print("""
╔═══════════════════════════════════════════════════════════════╗
║        📱 COLLECTEUR DE COMMENTAIRES FACEBOOK → KAFKA         ║
║                       Mode Local                               ║
╚═══════════════════════════════════════════════════════════════╝
    """)
    
    # Créer le producteur
    producer = FacebookKafkaProducer()
    
    try:
        # Connexion à Kafka
        if not producer.connect_kafka():
            logger.error("Impossible de se connecter à Kafka. Vérifiez que Kafka est démarré.")
            sys.exit(1)
        
        # Valider le token Facebook
        producer.validate_token()
        
        # Mode d'exécution
        if len(sys.argv) > 1 and sys.argv[1] == "--once":
            producer.run_once()
        else:
            producer.run_continuous()
            
    except KeyboardInterrupt:
        logger.info("\n🛑 Interruption utilisateur")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
