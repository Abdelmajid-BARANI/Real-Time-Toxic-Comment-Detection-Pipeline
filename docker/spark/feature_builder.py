"""
============================================================================
SPARK STREAMING - Feature Builder (Étape 3)
============================================================================
Pipeline: data.cleaned.stream → data.features.stream

Fonctionnalités:
- Lecture depuis data.cleaned.stream
- Extraction features NLP:
  * Longueur du commentaire
  * Nombre de mots
  * Score lexical
  * Densité de ponctuation
  * Indicateurs toxiques
- Écriture vers data.features.stream
"""

import os
import sys
import re
import logging
from datetime import datetime

# Configuration logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("FeatureBuilder")

# ===========================================================================
# IMPORTS SPARK
# ===========================================================================

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, udf, from_json, to_json, struct, current_timestamp,
        length, size, split, when, lit, array
    )
    from pyspark.sql.types import (
        StructType, StructField, StringType, FloatType,
        IntegerType, TimestampType, ArrayType
    )
except ImportError as e:
    logger.error(f"❌ PySpark non installé: {e}")
    sys.exit(1)

# ===========================================================================
# CONFIGURATION
# ===========================================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_CLEANED = os.getenv("KAFKA_TOPIC_CLEANED", "data.cleaned.stream")
TOPIC_FEATURES = os.getenv("KAFKA_TOPIC_FEATURES", "data.features.stream")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/app/checkpoints/feature_builder")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "5 seconds")

# Schéma du message nettoyé
CLEANED_SCHEMA = StructType([
    StructField("source", StringType(), True),
    StructField("post_id", StringType(), True),
    StructField("comment_id", StringType(), True),
    StructField("username", StringType(), True),
    StructField("original_message", StringType(), True),
    StructField("cleaned_message", StringType(), True),
    StructField("created_time", StringType(), True),
    StructField("processing_time", StringType(), True),
])

# ===========================================================================
# MOTS INDICATEURS DE TOXICITÉ
# ===========================================================================

TOXIC_WORDS = {
    # Insultes vulgaires (sévères)
    "fuck", "fucking", "fucker", "fucked", "fck", "fuk",
    "shit", "shitty", "bullshit", "bitch", "bitches",
    "asshole", "bastard", "dick", "pussy", "cunt", "cock",
    "whore", "slut", "hoe", "nigger", "nigga", "faggot", "fag",
    "retard", "retarded",
    # Violence
    "kill", "die", "murder", "rape", "death", "burn", "attack",
    # Insultes modérées
    "hate", "stupid", "idiot", "moron", "dumb", "ugly", "loser",
    "pathetic", "garbage", "trash", "worthless", "racist", "nazi",
    "terrorist", "scum", "disgusting", "offensive", "suck", "sucks",
    "damn", "crap", "ass", "jerk", "freak", "fool", "lame",
    "horrible", "terrible", "awful", "shut up", "go away"
}

POSITIVE_WORDS = {
    "love", "great", "amazing", "wonderful", "beautiful", "awesome",
    "excellent", "fantastic", "good", "nice", "perfect", "best",
    "happy", "joy", "thank", "thanks", "appreciate", "brilliant"
}

# ===========================================================================
# FONCTIONS D'EXTRACTION DE FEATURES
# ===========================================================================

def extract_features(text: str) -> dict:
    """Extrait les features NLP d'un texte"""
    if not text:
        return {
            "char_count": 0,
            "word_count": 0,
            "avg_word_length": 0.0,
            "punctuation_ratio": 0.0,
            "uppercase_ratio": 0.0,
            "toxic_word_count": 0,
            "positive_word_count": 0,
            "exclamation_count": 0,
            "question_count": 0,
            "lexical_score": 0.0
        }
    
    # Comptages de base
    char_count = len(text)
    words = text.lower().split()
    word_count = len(words)
    
    # Longueur moyenne des mots
    avg_word_length = sum(len(w) for w in words) / max(word_count, 1)
    
    # Ratio de ponctuation
    punct_count = sum(1 for c in text if c in ".,!?;:\"'")
    punctuation_ratio = punct_count / max(char_count, 1)
    
    # Ratio majuscules (sur texte original)
    upper_count = sum(1 for c in text if c.isupper())
    uppercase_ratio = upper_count / max(char_count, 1)
    
    # Comptage mots toxiques
    toxic_word_count = sum(1 for w in words if w in TOXIC_WORDS)
    
    # Comptage mots positifs
    positive_word_count = sum(1 for w in words if w in POSITIVE_WORDS)
    
    # Ponctuation expressive
    exclamation_count = text.count('!')
    question_count = text.count('?')
    
    # Score lexical (indicateur de toxicité basé sur le vocabulaire)
    lexical_score = (toxic_word_count * 2 - positive_word_count) / max(word_count, 1)
    lexical_score = max(-1.0, min(1.0, lexical_score))  # Normaliser entre -1 et 1
    
    return {
        "char_count": char_count,
        "word_count": word_count,
        "avg_word_length": round(avg_word_length, 2),
        "punctuation_ratio": round(punctuation_ratio, 4),
        "uppercase_ratio": round(uppercase_ratio, 4),
        "toxic_word_count": toxic_word_count,
        "positive_word_count": positive_word_count,
        "exclamation_count": exclamation_count,
        "question_count": question_count,
        "lexical_score": round(lexical_score, 4)
    }

# UDFs pour chaque feature
@udf(IntegerType())
def char_count_udf(text):
    return len(text) if text else 0

@udf(IntegerType())
def word_count_udf(text):
    return len(text.split()) if text else 0

@udf(FloatType())
def avg_word_length_udf(text):
    if not text:
        return 0.0
    words = text.split()
    if not words:
        return 0.0
    return sum(len(w) for w in words) / len(words)

@udf(FloatType())
def lexical_score_udf(text):
    if not text:
        return 0.0
    words = text.lower().split()
    if not words:
        return 0.0
    toxic = sum(1 for w in words if w in TOXIC_WORDS)
    positive = sum(1 for w in words if w in POSITIVE_WORDS)
    score = (toxic * 2 - positive) / len(words)
    return max(-1.0, min(1.0, score))

@udf(IntegerType())
def toxic_word_count_udf(text):
    if not text:
        return 0
    words = text.lower().split()
    return sum(1 for w in words if w in TOXIC_WORDS)

@udf(IntegerType())
def positive_word_count_udf(text):
    if not text:
        return 0
    words = text.lower().split()
    return sum(1 for w in words if w in POSITIVE_WORDS)

@udf(FloatType())
def uppercase_ratio_udf(text):
    if not text:
        return 0.0
    upper = sum(1 for c in text if c.isupper())
    return upper / len(text)

# ===========================================================================
# CRÉATION SPARK SESSION
# ===========================================================================

def create_spark_session() -> SparkSession:
    """Crée la session Spark avec configuration Kafka"""
    
    logger.info("🚀 Initialisation Spark Session - Feature Builder")
    
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    ]
    
    spark = SparkSession.builder \
        .appName("FeatureBuilder") \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.driver.memory", "2g") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("✅ Spark Session créée")
    
    return spark

# ===========================================================================
# PIPELINE STREAMING
# ===========================================================================

def run_feature_builder():
    """Execute le pipeline d'extraction de features"""
    
    spark = create_spark_session()
    
    logger.info(f"📥 Lecture depuis: {TOPIC_CLEANED}")
    logger.info(f"📤 Écriture vers: {TOPIC_FEATURES}")
    
    # Lecture depuis Kafka (data.cleaned.stream)
    cleaned_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC_CLEANED) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parsing JSON
    parsed_df = cleaned_stream \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp") \
        .select(
            col("key").alias("comment_id_key"),
            from_json(col("value"), CLEANED_SCHEMA).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ) \
        .select("comment_id_key", "data.*", "kafka_timestamp")
    
    # Extraction des features
    features_df = parsed_df \
        .withColumn("char_count", char_count_udf(col("cleaned_message"))) \
        .withColumn("word_count", word_count_udf(col("cleaned_message"))) \
        .withColumn("avg_word_length", avg_word_length_udf(col("cleaned_message"))) \
        .withColumn("lexical_score", lexical_score_udf(col("original_message"))) \
        .withColumn("toxic_word_count", toxic_word_count_udf(col("cleaned_message"))) \
        .withColumn("positive_word_count", positive_word_count_udf(col("cleaned_message"))) \
        .withColumn("uppercase_ratio", uppercase_ratio_udf(col("original_message"))) \
        .withColumn("feature_extraction_time", current_timestamp()) \
        .select(
            col("source"),
            col("post_id"),
            col("comment_id"),
            col("username"),
            col("original_message"),
            col("cleaned_message"),
            col("created_time"),
            col("char_count"),
            col("word_count"),
            col("avg_word_length"),
            col("lexical_score"),
            col("toxic_word_count"),
            col("positive_word_count"),
            col("uppercase_ratio"),
            col("feature_extraction_time")
        )
    
    # Format de sortie pour Kafka
    output_df = features_df \
        .select(
            col("comment_id").alias("key"),
            to_json(struct(
                "source", "post_id", "comment_id", "username",
                "original_message", "cleaned_message", "created_time",
                "char_count", "word_count", "avg_word_length",
                "lexical_score", "toxic_word_count", "positive_word_count",
                "uppercase_ratio", "feature_extraction_time"
            )).alias("value")
        )
    
    # Écriture vers Kafka (data.features.stream)
    query = output_df \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", TOPIC_FEATURES) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .trigger(processingTime=TRIGGER_INTERVAL) \
        .start()
    
    logger.info("✅ Feature Builder démarré")
    logger.info(f"   {TOPIC_CLEANED} → {TOPIC_FEATURES}")
    
    query.awaitTermination()

# ===========================================================================
# MAIN
# ===========================================================================

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("  SPARK STREAMING FEATURE BUILDER")
    logger.info("  data.cleaned.stream → data.features.stream")
    logger.info("=" * 60)
    
    run_feature_builder()
