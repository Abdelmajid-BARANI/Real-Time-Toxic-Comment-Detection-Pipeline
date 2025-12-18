"""
============================================================================
SPARK STREAMING - Normalizer (Étape 2)
============================================================================
Pipeline: data.raw.stream → data.cleaned.stream

Fonctionnalités:
- Lecture depuis data.raw.stream
- Nettoyage du texte (lowercase, emojis, stopwords)
- Validation du schéma
- Écriture vers data.cleaned.stream
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
logger = logging.getLogger("StreamNormalizer")

# ===========================================================================
# IMPORTS SPARK
# ===========================================================================

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, udf, from_json, to_json, struct, current_timestamp,
        lower, regexp_replace, trim, length, when
    )
    from pyspark.sql.types import (
        StructType, StructField, StringType, TimestampType
    )
except ImportError as e:
    logger.error(f"❌ PySpark non installé: {e}")
    sys.exit(1)

# ===========================================================================
# CONFIGURATION
# ===========================================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "data.raw.stream")
TOPIC_CLEANED = os.getenv("KAFKA_TOPIC_CLEANED", "data.cleaned.stream")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/app/checkpoints/normalizer")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "5 seconds")

# Schéma du message brut
RAW_SCHEMA = StructType([
    StructField("source", StringType(), True),
    StructField("post_id", StringType(), True),
    StructField("comment_id", StringType(), True),
    StructField("user", StringType(), True),
    StructField("message", StringType(), True),
    StructField("created_time", StringType(), True),
])

# ===========================================================================
# FONCTIONS DE NETTOYAGE
# ===========================================================================

def clean_text(text: str) -> str:
    """Nettoie le texte des commentaires"""
    if not text:
        return ""
    
    # Minuscules
    text = text.lower()
    
    # Suppression URLs
    text = re.sub(r"http\S+|www\S+|https\S+", "", text)
    
    # Suppression mentions @
    text = re.sub(r"@\w+", "", text)
    
    # Suppression emojis
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"
        "\U0001F300-\U0001F5FF"
        "\U0001F680-\U0001F6FF"
        "\U0001F1E0-\U0001F1FF"
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "]+",
        flags=re.UNICODE
    )
    text = emoji_pattern.sub('', text)
    
    # Suppression caractères spéciaux (garder ponctuation de base)
    text = re.sub(r"[^\w\s.,!?']", " ", text)
    
    # Normalisation des espaces
    text = re.sub(r"\s+", " ", text).strip()
    
    return text

# Enregistrer comme UDF Spark
clean_text_udf = udf(clean_text, StringType())

# ===========================================================================
# CRÉATION SPARK SESSION
# ===========================================================================

def create_spark_session() -> SparkSession:
    """Crée la session Spark avec configuration Kafka"""
    
    logger.info("🚀 Initialisation Spark Session - Normalizer")
    
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    ]
    
    spark = SparkSession.builder \
        .appName("StreamNormalizer") \
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

def run_normalizer():
    """Execute le pipeline de normalisation"""
    
    spark = create_spark_session()
    
    logger.info(f"📥 Lecture depuis: {TOPIC_RAW}")
    logger.info(f"📤 Écriture vers: {TOPIC_CLEANED}")
    
    # Lecture depuis Kafka (data.raw.stream)
    raw_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC_RAW) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parsing JSON
    parsed_df = raw_stream \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp") \
        .select(
            col("key").alias("comment_id_key"),
            from_json(col("value"), RAW_SCHEMA).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ) \
        .select("comment_id_key", "data.*", "kafka_timestamp")
    
    # Nettoyage et transformation
    cleaned_df = parsed_df \
        .withColumn("cleaned_message", clean_text_udf(col("message"))) \
        .withColumn("processing_time", current_timestamp()) \
        .filter(length(col("cleaned_message")) > 0) \
        .select(
            col("source"),
            col("post_id"),
            col("comment_id"),
            col("user").alias("username"),
            col("message").alias("original_message"),
            col("cleaned_message"),
            col("created_time"),
            col("processing_time")
        )
    
    # Format de sortie pour Kafka
    output_df = cleaned_df \
        .select(
            col("comment_id").alias("key"),
            to_json(struct(
                "source", "post_id", "comment_id", "username",
                "original_message", "cleaned_message", 
                "created_time", "processing_time"
            )).alias("value")
        )
    
    # Écriture vers Kafka (data.cleaned.stream)
    query = output_df \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", TOPIC_CLEANED) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .trigger(processingTime=TRIGGER_INTERVAL) \
        .start()
    
    logger.info("✅ Stream Normalizer démarré")
    logger.info(f"   {TOPIC_RAW} → {TOPIC_CLEANED}")
    
    query.awaitTermination()

# ===========================================================================
# MAIN
# ===========================================================================

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("  SPARK STREAMING NORMALIZER")
    logger.info("  data.raw.stream → data.cleaned.stream")
    logger.info("=" * 60)
    
    run_normalizer()
