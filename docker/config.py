"""
============================================================================
Configuration Centralisée - Pipeline Streaming Temps Réel Multi-Topics
============================================================================
Architecture conforme au PDF : 5 Topics Kafka
"""

import os
from pathlib import Path

# ===========================================================================
# CHEMINS
# ===========================================================================

BASE_DIR = Path(__file__).parent.absolute()
MODEL_DIR = BASE_DIR / "model"
DATASET_DIR = BASE_DIR / "dataset"
CHECKPOINT_DIR = BASE_DIR / "checkpoints"
KAFKA_DIR = BASE_DIR / "kafka"
LOGS_DIR = BASE_DIR / "logs"

# Créer les répertoires si nécessaire
for directory in [CHECKPOINT_DIR, LOGS_DIR]:
    directory.mkdir(exist_ok=True)

# ===========================================================================
# FACEBOOK GRAPH API
# ===========================================================================

FACEBOOK_CONFIG = {
    "access_token": os.getenv(
        "FACEBOOK_ACCESS_TOKEN",
        "YOUR_ACCESS_TOKEN_HERE"
    ),
    "page_id": os.getenv("FACEBOOK_PAGE_ID", "143748515489111"),
    "api_version": "v17.0",
    "base_url": "https://graph.facebook.com",
    "polling_interval": 30,
    "max_comments_per_request": 100,
}

# ===========================================================================
# APACHE KAFKA (KRaft Mode) - MULTI-TOPICS ARCHITECTURE
# ===========================================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# Topics Kafka conformes au PDF
KAFKA_TOPICS = {
    "raw": "data.raw.stream",           # Commentaires Facebook bruts (JSON)
    "cleaned": "data.cleaned.stream",    # Données nettoyées et structurées
    "features": "data.features.stream",  # Features NLP
    "predictions": "data.predictions.stream",  # Résultats toxicité/sentiment
    "labels": "data.labels.stream",      # Labels réels (optionnel)
}

KAFKA_CONFIG = {
    "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
    "topics": KAFKA_TOPICS,
    "group_id": "toxic_comment_processor",
    "auto_offset_reset": "earliest",
    "enable_auto_commit": True,
    
    # Configuration KRaft
    "kraft": {
        "node_id": 1,
        "controller_quorum_voters": "1@kafka:9093",
        "listeners": "PLAINTEXT://kafka:9092,CONTROLLER://kafka:9093",
        "num_partitions": 3,
        "replication_factor": 1,
    },
    
    # Producer
    "producer": {
        "acks": "all",
        "retries": 3,
        "batch_size": 16384,
        "linger_ms": 1,
    },
    
    # Consumer
    "consumer": {
        "fetch_min_bytes": 1,
        "fetch_max_wait_ms": 500,
        "max_poll_records": 500,
    }
}

# ===========================================================================
# POSTGRESQL (Obligatoire)
# ===========================================================================

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB", "toxic_coments_db"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "majid2020"),
}

POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
POSTGRES_URL = f"postgresql://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"

# ===========================================================================
# PYSPARK STREAMING
# ===========================================================================

SPARK_CONFIG = {
    "app_name": "ToxicCommentPipeline",
    "master": os.getenv("SPARK_MASTER", "local[*]"),
    "driver_memory": "2g",
    "executor_memory": "2g",
    
    "packages": [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.postgresql:postgresql:42.7.0",
    ],
    
    "streaming": {
        "trigger_interval": "5 seconds",
        "output_mode": "append",
        "starting_offsets": "earliest",
    },
    
    "checkpoints": {
        "normalizer": str(CHECKPOINT_DIR / "normalizer"),
        "feature_builder": str(CHECKPOINT_DIR / "feature_builder"),
        "model_serving": str(CHECKPOINT_DIR / "model_serving"),
    }
}

# ===========================================================================
# MODÈLE ML
# ===========================================================================

MODEL_CONFIG = {
    "toxic_model_path": str(MODEL_DIR / "toxic_model.h5"),
    "tokenizer_path": str(MODEL_DIR / "tokenizer.pkl"),
    "glove_path": str(DATASET_DIR / "glove.6B" / "glove.6B.100d.txt"),
    "max_sequence_length": 200,
    "toxicity_threshold": 0.5,
    "embedding_dim": 100,
    "toxicity_labels": [
        "toxic", "severe_toxic", "obscene",
        "threat", "insult", "identity_hate"
    ]
}

# ===========================================================================
# STREAMLIT DASHBOARD
# ===========================================================================

DASHBOARD_CONFIG = {
    "page_title": "📊 Analyse Commentaires Toxiques",
    "page_icon": "💬",
    "layout": "wide",
    "refresh_interval": 5000,
    "max_comments_display": 100,
    "timeline_days": 7,
    "colors": {
        "positive": "#2ecc71",
        "negative": "#e74c3c",
        "neutral": "#95a5a6",
        "toxic": "#d62728",
        "non_toxic": "#1f77b4",
    }
}

# ===========================================================================
# TRAITEMENT TEXTE
# ===========================================================================

TEXT_CONFIG = {
    "min_word_length": 3,
    "max_keywords": 10,
    "sentiment_thresholds": {
        "positive": 0.05,
        "negative": -0.05,
    }
}

# ===========================================================================
# FONCTIONS UTILITAIRES
# ===========================================================================

def get_topic(stage: str) -> str:
    """Retourne le nom du topic Kafka pour une étape donnée"""
    return KAFKA_TOPICS.get(stage, "data.raw.stream")

def print_architecture():
    """Affiche l'architecture du pipeline"""
    print("""
╔══════════════════════════════════════════════════════════════════════╗
║           ARCHITECTURE PIPELINE STREAMING MULTI-TOPICS               ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  Facebook API                                                        ║
║       ↓                                                              ║
║  Producer Python                                                     ║
║       ↓                                                              ║
║  Kafka: data.raw.stream (JSON brut)                                  ║
║       ↓                                                              ║
║  Spark Streaming (Normalizer)                                        ║
║       ↓                                                              ║
║  Kafka: data.cleaned.stream (texte nettoyé)                          ║
║       ↓                                                              ║
║  Spark Streaming (Feature Builder)                                   ║
║       ↓                                                              ║
║  Kafka: data.features.stream (features NLP)                          ║
║       ↓                                                              ║
║  Spark Streaming (Model Serving)                                     ║
║       ↓                                                              ║
║  Kafka: data.predictions.stream (prédictions)                        ║
║       ↓                                                              ║
║  PostgreSQL                                                          ║
║       ↓                                                              ║
║  Streamlit Dashboard                                                 ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
    """)
    
    print(f"\n📌 Kafka Bootstrap: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"📌 Topics: {list(KAFKA_TOPICS.values())}")
    print(f"📌 PostgreSQL: {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}")

if __name__ == "__main__":
    print_architecture()
