"""
============================================================================
Configuration Centralisée - Pipeline Streaming Temps Réel
============================================================================
Toutes les configurations du projet en un seul fichier
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
        "EAATQPWLZCNMkBQJxk3j3znlpYPsz93ofg6ez3EjZCzKQ3375hvnn6jkCPsqyl55cCNVzdKNQjVDDNyCPzLbZCcxdlldRNF9jw9qgxwZBzmoqfevnvq1cXnpJ1zM3y7CK8c5ZAFg9uPzOHE1FPBHZBToQ3k4CuCSQY2oQH77UZAFUctDq5pFiYUC3P1x6MzUiVNUHlesjGZCZCUWjT4JgOO1kHG4R8QaDCr10dMwPOlcAqWwZDZD"  # Remplacer par votre token
    ),
    "page_id": os.getenv("FACEBOOK_PAGE_ID", "143748515489111"),
    "api_version": "v17.0",
    "base_url": "https://graph.facebook.com",
    "polling_interval": 30,  # secondes
    "max_comments_per_request": 100,
}

# ===========================================================================
# APACHE KAFKA (KRaft Mode)
# ===========================================================================

KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "topic": "facebook_comments",
    "group_id": "facebook_comment_processor",
    "auto_offset_reset": "earliest",
    "enable_auto_commit": True,
    
    # Configuration KRaft
    "kraft": {
        "node_id": 1,
        "controller_quorum_voters": "1@localhost:9093",
        "listeners": "PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093",
        "log_dirs": str(KAFKA_DIR / "kraft-combined-logs"),
        "num_partitions": 3,
        "replication_factor": 1,
    },
    
    # Producer
    "producer": {
        "acks": "all",
        "retries": 3,
        "batch_size": 16384,
        "linger_ms": 1,
        "buffer_memory": 33554432,
    },
    
    # Consumer
    "consumer": {
        "fetch_min_bytes": 1,
        "fetch_max_wait_ms": 500,
        "max_poll_records": 500,
    }
}

# ===========================================================================
# POSTGRESQL
# ===========================================================================

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB", "toxic_coments_db"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "majid2020"),
}

# URL JDBC pour Spark
POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"

# URL pour psycopg2
POSTGRES_URL = f"postgresql://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"

# ===========================================================================
# PYSPARK
# ===========================================================================

SPARK_CONFIG = {
    "app_name": "FacebookCommentAnalysis",
    "master": "local[*]",
    "driver_memory": "4g",
    "executor_memory": "4g",
    "checkpoint_location": str(CHECKPOINT_DIR),
    
    # Packages Maven
    "packages": [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.postgresql:postgresql:42.7.0",
    ],
    
    # Streaming
    "streaming": {
        "trigger_interval": "10 seconds",
        "output_mode": "append",
        "starting_offsets": "latest",
    }
}

# ===========================================================================
# MODÈLE ML
# ===========================================================================

MODEL_CONFIG = {
    "toxic_model_path": str(MODEL_DIR / "toxic_model.h5"),
    "tokenizer_path": str(MODEL_DIR / "tokenizer.pkl"),
    "glove_path": str(DATASET_DIR / "glove.6B" / "glove.6B.100d.txt"),
    
    # Paramètres
    "max_sequence_length": 200,
    "toxicity_threshold": 0.5,
    "embedding_dim": 100,
    
    # Classes de toxicité
    "toxicity_labels": [
        "toxic",
        "severe_toxic", 
        "obscene",
        "threat",
        "insult",
        "identity_hate"
    ]
}

# ===========================================================================
# STREAMLIT DASHBOARD
# ===========================================================================

DASHBOARD_CONFIG = {
    "page_title": "📊 Analyse Commentaires Facebook",
    "page_icon": "💬",
    "layout": "wide",
    "refresh_interval": 5000,  # millisecondes
    
    # Limites d'affichage
    "max_comments_display": 100,
    "timeline_days": 7,
    "top_keywords_limit": 20,
    
    # Couleurs
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

TEXT_PROCESSING_CONFIG = {
    "min_word_length": 3,
    "max_keywords": 10,
    "languages": ["english", "french"],
    
    # Patterns à supprimer
    "url_pattern": r"http\S+|www\S+",
    "mention_pattern": r"@\w+",
    "hashtag_pattern": r"#\w+",
    "number_pattern": r"\d+",
    
    # Seuils sentiment VADER
    "sentiment_thresholds": {
        "positive": 0.05,
        "negative": -0.05,
    }
}

# ===========================================================================
# LOGGING
# ===========================================================================

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "standard",
            "stream": "ext://sys.stdout"
        },
        "file": {
            "class": "logging.FileHandler",
            "level": "DEBUG",
            "formatter": "standard",
            "filename": str(LOGS_DIR / "pipeline.log"),
            "mode": "a"
        }
    },
    "root": {
        "level": "INFO",
        "handlers": ["console", "file"]
    }
}

# ===========================================================================
# UTILITAIRES
# ===========================================================================

def get_postgres_connection_string():
    """Retourne la chaîne de connexion PostgreSQL"""
    return POSTGRES_URL

def get_kafka_producer_config():
    """Retourne la configuration du producteur Kafka"""
    return {
        "bootstrap_servers": KAFKA_CONFIG["bootstrap_servers"],
        "value_serializer": lambda v: __import__('json').dumps(v).encode('utf-8'),
        **KAFKA_CONFIG["producer"]
    }

def get_kafka_consumer_config():
    """Retourne la configuration du consommateur Kafka"""
    return {
        "bootstrap_servers": KAFKA_CONFIG["bootstrap_servers"],
        "group_id": KAFKA_CONFIG["group_id"],
        "auto_offset_reset": KAFKA_CONFIG["auto_offset_reset"],
        "enable_auto_commit": KAFKA_CONFIG["enable_auto_commit"],
        "value_deserializer": lambda m: __import__('json').loads(m.decode('utf-8')),
        **KAFKA_CONFIG["consumer"]
    }

def print_config():
    """Affiche la configuration actuelle"""
    print("=" * 60)
    print("CONFIGURATION DU PIPELINE")
    print("=" * 60)
    print(f"\n📌 Facebook API:")
    print(f"   Page ID: {FACEBOOK_CONFIG['page_id']}")
    print(f"   API Version: {FACEBOOK_CONFIG['api_version']}")
    print(f"\n📌 Kafka:")
    print(f"   Servers: {KAFKA_CONFIG['bootstrap_servers']}")
    print(f"   Topic: {KAFKA_CONFIG['topic']}")
    print(f"\n📌 PostgreSQL:")
    print(f"   Host: {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}")
    print(f"   Database: {POSTGRES_CONFIG['database']}")
    print(f"\n📌 Spark:")
    print(f"   Master: {SPARK_CONFIG['master']}")
    print(f"   Trigger: {SPARK_CONFIG['streaming']['trigger_interval']}")
    print(f"\n📌 Model:")
    print(f"   Path: {MODEL_CONFIG['toxic_model_path']}")
    print(f"   Threshold: {MODEL_CONFIG['toxicity_threshold']}")
    print("=" * 60)

if __name__ == "__main__":
    print_config()
