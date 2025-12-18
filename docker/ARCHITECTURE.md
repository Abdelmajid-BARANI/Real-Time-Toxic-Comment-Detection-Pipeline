# ============================================================================
# 🏗️ ARCHITECTURE - Pipeline Big Data Streaming
# Analyse Temps Réel des Commentaires Toxiques
# ============================================================================

## Vue d'ensemble

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    PLATEFORME BIG DATA STREAMING                            │
│              Analyse Temps Réel des Commentaires Toxiques                   │
└─────────────────────────────────────────────────────────────────────────────┘

                              ┌─────────────────┐
                              │   FACEBOOK      │
                              │   GRAPH API     │
                              └────────┬────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         COUCHE INGESTION                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                     producer-facebook                                │   │
│  │  • Collecte commentaires Facebook                                    │   │
│  │  • Sérialisation JSON                                                │   │
│  │  • Publication vers Kafka                                            │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        APACHE KAFKA (KRaft)                                 │
│  ┌───────────────┐ ┌───────────────┐ ┌───────────────┐ ┌───────────────┐   │
│  │data.raw.stream│→│data.cleaned.  │→│data.features. │→│data.predictions│  │
│  │               │ │    stream     │ │    stream     │ │    .stream    │   │
│  │ Commentaires  │ │ Texte nettoyé │ │ Features NLP  │ │  Prédictions  │   │
│  │    bruts      │ │               │ │               │ │   toxicité    │   │
│  └───────────────┘ └───────────────┘ └───────────────┘ └───────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
         │                    │                    │                │
         ▼                    ▼                    ▼                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     SPARK STRUCTURED STREAMING                               │
│                                                                              │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐           │
│  │ spark-normalizer │  │  spark-features  │  │   spark-model    │           │
│  │                  │  │                  │  │                  │           │
│  │ • Lowercase      │  │ • Longueur texte │  │ • Modèle ML      │           │
│  │ • Suppression    │  │ • Nombre mots    │  │ • Score toxicité │           │
│  │   emojis/URLs    │  │ • Score lexical  │  │ • Sentiment      │           │
│  │ • Stopwords      │  │ • Indicateurs    │  │ • Classification │           │
│  │ • Validation     │  │   toxiques       │  │                  │           │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘           │
└─────────────────────────────────────────────────────────────────────────────┘
                                                             │
                                                             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          COUCHE STOCKAGE                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        PostgreSQL                                    │   │
│  │                                                                      │   │
│  │  facebook_comments_analysis                                          │   │
│  │  ├── id, post_id, comment_id                                        │   │
│  │  ├── username, comment, cleaned_comment                             │   │
│  │  ├── toxicity_score, label, is_toxic                                │   │
│  │  ├── sentiment, sentiment_score                                     │   │
│  │  └── created_time, ingestion_time                                   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        COUCHE VISUALISATION                                 │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                     Streamlit Dashboard                              │   │
│  │                                                                      │   │
│  │  📊 Métriques temps réel    │  📈 Timeline                          │   │
│  │  🥧 Distribution toxicité   │  💬 Commentaires récents              │   │
│  │  🎯 Filtres & Export        │  🔄 Auto-refresh                      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 📌 Topics Kafka

| # | Topic | Description | Format |
|---|-------|-------------|--------|
| 1 | `data.raw.stream` | Commentaires Facebook bruts | JSON |
| 2 | `data.cleaned.stream` | Texte nettoyé et normalisé | JSON |
| 3 | `data.features.stream` | Features NLP extraits | JSON |
| 4 | `data.predictions.stream` | Prédictions de toxicité | JSON |
| 5 | `data.labels.stream` | Labels réels (optionnel) | JSON |

## 🔄 Flux de données détaillé

### Étape 1: Ingestion (Producer)
```json
// data.raw.stream
{
  "source": "facebook",
  "post_id": "123456789",
  "comment_id": "987654321",
  "user": "John Doe",
  "message": "This is a comment!",
  "created_time": "2025-01-01T10:00:00Z"
}
```

### Étape 2: Normalisation (Spark)
```json
// data.cleaned.stream
{
  "source": "facebook",
  "post_id": "123456789",
  "comment_id": "987654321",
  "username": "John Doe",
  "original_message": "This is a comment!",
  "cleaned_message": "this is a comment",
  "created_time": "2025-01-01T10:00:00Z",
  "processing_time": "2025-01-01T10:00:01Z"
}
```

### Étape 3: Feature Engineering (Spark)
```json
// data.features.stream
{
  "source": "facebook",
  "post_id": "123456789",
  "comment_id": "987654321",
  "username": "John Doe",
  "original_message": "This is a comment!",
  "cleaned_message": "this is a comment",
  "char_count": 19,
  "word_count": 4,
  "avg_word_length": 4.0,
  "lexical_score": -0.1,
  "toxic_word_count": 0,
  "positive_word_count": 0,
  "uppercase_ratio": 0.05,
  "feature_extraction_time": "2025-01-01T10:00:02Z"
}
```

### Étape 4: Prédiction (Spark + ML)
```json
// data.predictions.stream
{
  "comment_id": "987654321",
  "toxicity_score": 0.12,
  "label": "non_toxique",
  "is_toxic": false,
  "sentiment": "neutral",
  "sentiment_score": 0.52,
  "prediction_time": "2025-01-01T10:00:03Z"
}
```

## 🐳 Services Docker

```
┌─────────────────────────────────────────────────────────────────┐
│                    DOCKER COMPOSE                                │
│                                                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │    kafka    │  │   postgres  │  │    producer-facebook    │  │
│  │   :9092     │  │    :5432    │  │                         │  │
│  │   :9093     │  │             │  │                         │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
│                                                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │spark-normal │  │spark-feature│  │     spark-model         │  │
│  │   izer      │  │    s        │  │                         │  │
│  │             │  │             │  │                         │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
│                                                                  │
│  ┌─────────────┐  ┌─────────────┐                               │
│  │  streamlit  │  │  kafka-ui   │                               │
│  │   :8501     │  │   :8080     │                               │
│  └─────────────┘  └─────────────┘                               │
│                                                                  │
│  Network: toxic-pipeline-network                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 🔧 Stack Technique

| Composant | Technologie | Version |
|-----------|-------------|---------|
| Message Broker | Apache Kafka (KRaft) | 3.6 |
| Stream Processing | PySpark Structured Streaming | 3.5.0 |
| Base de données | PostgreSQL | 15 |
| Dashboard | Streamlit | 1.29 |
| Container | Docker | 24+ |
| Orchestration | Docker Compose | 2.0+ |
| Langage | Python | 3.10 |

## 📊 Métriques de Performance

- **Latence end-to-end**: < 5 secondes
- **Throughput**: ~1000 messages/minute
- **Trigger interval**: 5 secondes
- **Partitions Kafka**: 3 par topic
- **Checkpoint**: Toutes les 5 secondes

## 🚀 Cas d'usage

1. **Modération automatique** - Filtrage des commentaires toxiques
2. **Analyse de sentiment** - Comprendre l'opinion des utilisateurs
3. **Alertes temps réel** - Notification sur pics de toxicité
4. **Reporting** - Tableaux de bord analytiques
5. **ML Pipeline** - Amélioration continue du modèle

---
*Architecture conçue pour un projet PFE Big Data / IA - ENSA*
