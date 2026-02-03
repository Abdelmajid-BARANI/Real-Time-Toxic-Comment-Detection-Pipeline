"""
============================================================================
SPARK STREAMING - Model Serving (Étape 4)
============================================================================
Pipeline: data.features.stream → data.predictions.stream → PostgreSQL

Fonctionnalités:
- Lecture depuis data.features.stream
- Prédiction toxicité avec modèle ML
- Analyse de sentiment
- Écriture vers data.predictions.stream
- Sauvegarde dans PostgreSQL
"""

import os
import sys
import logging
from datetime import datetime

# Configuration logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("ModelServing")

# ===========================================================================
# IMPORTS SPARK
# ===========================================================================

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, udf, from_json, to_json, struct, current_timestamp,
        when, lit
    )
    from pyspark.sql.types import (
        StructType, StructField, StringType, FloatType,
        IntegerType, BooleanType, TimestampType
    )
except ImportError as e:
    logger.error(f"❌ PySpark non installé: {e}")
    sys.exit(1)

# ===========================================================================
# CONFIGURATION
# ===========================================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_FEATURES = os.getenv("KAFKA_TOPIC_FEATURES", "data.features.stream")
TOPIC_PREDICTIONS = os.getenv("KAFKA_TOPIC_PREDICTIONS", "data.predictions.stream")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/app/checkpoints/model_serving")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "5 seconds")

# PostgreSQL
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "toxic_coments_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "majid2020")
POSTGRES_TABLE = "facebook_comments_analysis"
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Modèle - Seuil abaissé à 0.3 pour détecter les insultes modérées
TOXICITY_THRESHOLD = float(os.getenv("TOXICITY_THRESHOLD", "0.3"))

# Schéma du message avec features
FEATURES_SCHEMA = StructType([
    StructField("source", StringType(), True),
    StructField("post_id", StringType(), True),
    StructField("comment_id", StringType(), True),
    StructField("username", StringType(), True),
    StructField("original_message", StringType(), True),
    StructField("cleaned_message", StringType(), True),
    StructField("created_time", StringType(), True),
    StructField("char_count", IntegerType(), True),
    StructField("word_count", IntegerType(), True),
    StructField("avg_word_length", FloatType(), True),
    StructField("lexical_score", FloatType(), True),
    StructField("toxic_word_count", IntegerType(), True),
    StructField("positive_word_count", IntegerType(), True),
    StructField("uppercase_ratio", FloatType(), True),
    StructField("feature_extraction_time", StringType(), True),
])

# ===========================================================================
# MODÈLE DE PRÉDICTION
# ===========================================================================

# Variables globales pour les modèles
toxic_model = None
tokenizer_dl = None
TF_AVAILABLE = False

try:
    import numpy as np
    from tensorflow.keras.models import load_model
    from tensorflow.keras.preprocessing.sequence import pad_sequences
    import joblib
    
    MODEL_PATH = os.getenv("MODEL_PATH", "/app/model/toxic_model.h5")
    TOKENIZER_PATH = os.getenv("TOKENIZER_PATH", "/app/model/tokenizer.pkl")
    MAX_SEQUENCE_LENGTH = 200
    
    if os.path.exists(MODEL_PATH) and os.path.exists(TOKENIZER_PATH):
        toxic_model = load_model(MODEL_PATH)
        tokenizer_dl = joblib.load(TOKENIZER_PATH)
        TF_AVAILABLE = True
        logger.info("✅ Modèle TensorFlow chargé")
    else:
        logger.warning(f"⚠️ Modèle non trouvé: {MODEL_PATH}")
except ImportError as e:
    logger.warning(f"⚠️ TensorFlow non disponible: {e}")

# ===========================================================================
# FONCTIONS DE PRÉDICTION
# ===========================================================================

def predict_toxicity_ml(text: str) -> float:
    """Prédit le score de toxicité avec le modèle ML"""
    global toxic_model, tokenizer_dl
    
    if not TF_AVAILABLE or toxic_model is None or tokenizer_dl is None:
        return predict_toxicity_rules(text)
    
    try:
        import numpy as np
        from tensorflow.keras.preprocessing.sequence import pad_sequences
        
        sequences = tokenizer_dl.texts_to_sequences([text])
        padded = pad_sequences(sequences, maxlen=MAX_SEQUENCE_LENGTH)
        prediction = toxic_model.predict(padded, verbose=0)
        
        # Moyenne des scores de toxicité (multi-label)
        toxicity_score = float(np.mean(prediction[0]))
        return round(toxicity_score, 4)
    except Exception as e:
        logger.warning(f"Erreur prédiction ML: {e}")
        return predict_toxicity_rules(text)

def predict_toxicity_rules(text: str) -> float:
    """Prédiction basée sur des règles (fallback)"""
    if not text:
        return 0.0
    
    text_lower = text.lower()
    words = text_lower.split()
    
    # Mots toxiques avec différents niveaux de sévérité
    severe_toxic = {
        "fuck", "fucking", "fucker", "fucked", "shit", "shitty", "bitch",
        "asshole", "bastard", "dick", "pussy", "cunt", "whore", "slut",
        "nigger", "faggot", "retard", "kill", "die", "murder", "rape",
        "nazi", "terrorist", "ass", "gay"
    }
    
    moderate_toxic = {
        "hate", "stupid", "idiot", "moron", "dumb", "ugly", "loser",
        "pathetic", "garbage", "trash", "worthless", "racist", "scum",
        "disgusting", "suck", "sucks", "damn", "crap", "ass", "jerk",
        "freak", "fool", "lame", "horrible", "terrible", "awful"
    }
    
    # Compter les occurrences
    severe_count = sum(1 for w in words if w in severe_toxic)
    moderate_count = sum(1 for w in words if w in moderate_toxic)
    
    # Vérifier aussi dans le texte complet (pour les mots collés)
    for word in severe_toxic:
        if word in text_lower and word not in ' '.join(words):
            severe_count += 1
    
    word_count = len(words)
    if word_count == 0:
        return 0.0
    
    # Score basé sur la sévérité
    base_score = min(severe_count * 0.5 + moderate_count * 0.25, 0.95)
    
    # Bonus pour majuscules excessives (cri)
    if text != text_lower:
        upper_ratio = sum(1 for c in text if c.isupper()) / len(text)
        if upper_ratio > 0.5:
            base_score += 0.1
    
    # Bonus pour ponctuation excessive
    exclamation_count = text.count('!')
    if exclamation_count > 2:
        base_score += 0.05
    
    return min(round(base_score, 4), 1.0)

def analyze_sentiment(text: str, lexical_score: float) -> tuple:
    """Analyse le sentiment du texte"""
    if not text:
        return ("neutral", 0.5)
    
    # Utiliser le lexical_score comme indicateur
    # Un lexical_score élevé = plus de mots toxiques = sentiment négatif
    if lexical_score > 0.05:
        sentiment = "negative"
        score = max(0.1, 0.5 - lexical_score)
    elif lexical_score < -0.05:
        sentiment = "positive"
        score = min(0.9, 0.5 + abs(lexical_score))
    else:
        sentiment = "neutral"
        score = 0.5
    
    return (sentiment, round(max(0.0, min(1.0, score)), 4))

# UDFs Spark
@udf(FloatType())
def toxicity_score_udf(text):
    return predict_toxicity_rules(text) if not TF_AVAILABLE else predict_toxicity_ml(text)

@udf(StringType())
def toxicity_label_udf(score):
    if score is None:
        return "non_toxique"
    return "toxique" if score >= TOXICITY_THRESHOLD else "non_toxique"

@udf(BooleanType())
def is_toxic_udf(score):
    if score is None:
        return False
    return score >= TOXICITY_THRESHOLD

@udf(StringType())
def sentiment_udf(lexical_score):
    """Analyse le sentiment basé sur le lexical_score"""
    if lexical_score is None:
        return "neutral"
    # Un lexical_score > 0 signifie plus de mots toxiques que positifs
    if lexical_score > 0.05:
        return "negative"
    elif lexical_score < -0.05:
        return "positive"
    return "neutral"

@udf(FloatType())
def sentiment_score_udf(lexical_score):
    if lexical_score is None:
        return 0.5
    if lexical_score > 0.05:
        return max(0.1, 0.5 - lexical_score)
    elif lexical_score < -0.05:
        return min(0.9, 0.5 + abs(lexical_score))
    return 0.5

# ===========================================================================
# CRÉATION SPARK SESSION
# ===========================================================================

def create_spark_session() -> SparkSession:
    """Crée la session Spark avec configuration Kafka et PostgreSQL"""
    
    logger.info("🚀 Initialisation Spark Session - Model Serving")
    
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.postgresql:postgresql:42.7.0",
    ]
    
    spark = SparkSession.builder \
        .appName("ModelServing") \
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
# ÉCRITURE POSTGRESQL
# ===========================================================================

def write_to_postgres(df, epoch_id):
    """Écrit un batch vers PostgreSQL avec gestion des doublons"""
    if df.count() == 0:
        return
    
    logger.info(f"📝 Écriture PostgreSQL - Batch {epoch_id}: {df.count()} lignes")
    
    try:
        # Utiliser une requête temporaire pour UPSERT
        df.createOrReplaceTempView(f"temp_batch_{epoch_id}")
        
        # Connexion PostgreSQL
        import psycopg2
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cursor = conn.cursor()
        
        # Insérer ligne par ligne avec ON CONFLICT DO UPDATE
        for row in df.collect():
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
                row.post_id, row.comment_id, row.username, row.comment, row.cleaned_comment,
                float(row.toxicity_score), row.label, bool(row.is_toxic), row.sentiment,
                float(row.sentiment_score), row.word_count, row.char_count,
                row.created_time, row.ingestion_time
            ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"✅ Batch {epoch_id} écrit avec succès")
    except Exception as e:
        logger.error(f"❌ Erreur écriture PostgreSQL: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()

# ===========================================================================
# PIPELINE STREAMING
# ===========================================================================

def run_model_serving():
    """Execute le pipeline de prédiction"""
    
    spark = create_spark_session()
    
    logger.info(f"📥 Lecture depuis: {TOPIC_FEATURES}")
    logger.info(f"📤 Écriture vers: {TOPIC_PREDICTIONS}")
    logger.info(f"💾 Sauvegarde PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    
    # Lecture depuis Kafka (data.features.stream)
    features_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC_FEATURES) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parsing JSON
    parsed_df = features_stream \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp") \
        .select(
            col("key").alias("comment_id_key"),
            from_json(col("value"), FEATURES_SCHEMA).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ) \
        .select("comment_id_key", "data.*", "kafka_timestamp")
    
    # Prédictions
    predictions_df = parsed_df \
        .withColumn("toxicity_score", toxicity_score_udf(col("cleaned_message"))) \
        .withColumn("label", toxicity_label_udf(col("toxicity_score"))) \
        .withColumn("is_toxic", is_toxic_udf(col("toxicity_score"))) \
        .withColumn("sentiment", sentiment_udf(col("lexical_score"))) \
        .withColumn("sentiment_score", sentiment_score_udf(col("lexical_score"))) \
        .withColumn("prediction_time", current_timestamp())
    
    # Sélection des colonnes pour PostgreSQL
    postgres_df = predictions_df.select(
        col("post_id"),
        col("comment_id"),
        col("username"),
        col("original_message").alias("comment"),
        col("cleaned_message").alias("cleaned_comment"),
        col("toxicity_score"),
        col("label"),
        col("is_toxic"),
        col("sentiment"),
        col("sentiment_score"),
        col("word_count"),
        col("char_count"),
        col("created_time").cast(TimestampType()),
        current_timestamp().alias("ingestion_time")
    )
    
    # Format pour Kafka (data.predictions.stream)
    kafka_output_df = predictions_df.select(
        col("comment_id").alias("key"),
        to_json(struct(
            "comment_id", "toxicity_score", "label", "is_toxic",
            "sentiment", "sentiment_score", "prediction_time"
        )).alias("value")
    )
    
    # Écriture vers Kafka
    kafka_query = kafka_output_df \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", TOPIC_PREDICTIONS) \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/kafka") \
        .trigger(processingTime=TRIGGER_INTERVAL) \
        .start()
    
    # Écriture vers PostgreSQL
    postgres_query = postgres_df \
        .writeStream \
        .foreachBatch(write_to_postgres) \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/postgres") \
        .trigger(processingTime=TRIGGER_INTERVAL) \
        .start()
    
    logger.info("✅ Model Serving démarré")
    logger.info(f"   {TOPIC_FEATURES} → {TOPIC_PREDICTIONS}")
    logger.info(f"   {TOPIC_FEATURES} → PostgreSQL")
    
    # Attendre les deux queries
    spark.streams.awaitAnyTermination()

# ===========================================================================
# MAIN
# ===========================================================================

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("  SPARK STREAMING MODEL SERVING")
    logger.info("  data.features.stream → data.predictions.stream")
    logger.info("  data.features.stream → PostgreSQL")
    logger.info("=" * 60)
    
    run_model_serving()
