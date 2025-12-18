# 🏗️ Architecture de la Plateforme de Streaming Temps Réel

## Analyse Intelligente des Commentaires Facebook

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                     ARCHITECTURE END-TO-END                                   │
└──────────────────────────────────────────────────────────────────────────────┘

┌─────────────┐    ┌─────────────┐    ┌─────────────────┐    ┌──────────────┐    ┌───────────────┐
│   FACEBOOK  │    │    KAFKA    │    │     PYSPARK     │    │  POSTGRESQL  │    │   STREAMLIT   │
│  GRAPH API  │───▶│   (KRaft)   │───▶│   STREAMING     │───▶│   DATABASE   │───▶│   DASHBOARD   │
│  Producer   │    │   Topic     │    │   Consumer      │    │   Storage    │    │ Visualization │
└─────────────┘    └─────────────┘    └─────────────────┘    └──────────────┘    └───────────────┘
      │                   │                    │                    │                    │
      │                   │                    │                    │                    │
   ┌──▼───────┐      ┌────▼────┐         ┌────▼─────┐         ┌────▼─────┐        ┌─────▼────┐
   │ Posts &  │      │ Topic:  │         │ Nettoyage│         │ comments │        │ Graphiques│
   │ Comments │      │facebook_│         │ TF-IDF   │         │ analysis │        │ Métriques │
   │ JSON     │      │comments │         │ Toxicité │         │ keywords │        │ Filtres   │
   └──────────┘      └─────────┘         │ Sentiment│         │ stats    │        └──────────┘
                                         └──────────┘         └──────────┘

```

---

## 📊 Vue d'Ensemble du Pipeline

```
                                    FLUX DE DONNÉES
                                    ═══════════════

    [Facebook API]                                              [Dashboard]
         │                                                           ▲
         │ HTTP/REST                                                 │
         ▼                                                           │ SQL
    ┌─────────┐     JSON     ┌─────────┐    Batch    ┌────────┐    Query
    │Producer │─────────────▶│  Kafka  │────────────▶│ Spark  │─────────┐
    │ Python  │              │ Broker  │             │Streaming│         │
    └─────────┘              │(KRaft)  │             └────────┘         │
                             └─────────┘                  │              │
                                  │                       │ JDBC        │
                                  │                       ▼              │
                             ┌─────────┐           ┌──────────┐         │
                             │ Topic:  │           │PostgreSQL│◀────────┘
                             │facebook_│           │  Tables  │
                             │comments │           └──────────┘
                             └─────────┘
```

---

## 🔧 1. Facebook Graph API (Ingestion)

### Rôle
Collecte des posts et commentaires d'une page Facebook via l'API officielle.

### Fonctionnalités
- 🔑 Gestion sécurisée du token d'accès
- 📄 Pagination automatique pour récupérer tous les commentaires
- 🔄 Mode polling temps réel (intervalle configurable)
- 📋 Format JSON standardisé

### Format des Données
```json
{
    "post_id": "143748515489111_123456789",
    "comment_id": "987654321",
    "username": "John Doe",
    "message": "Contenu du commentaire",
    "created_time": "2025-12-18T10:30:00+0000"
}
```

### Fichier: `fetch_facebook_comments.py`
```python
# Configuration requise
ACCESS_TOKEN = "votre_token_facebook"
PAGE_ID = "votre_page_id"
```

---

## 🚀 2. Apache Kafka (KRaft Mode)

### Architecture KRaft (sans ZooKeeper)

```
┌─────────────────────────────────────────────────────────────┐
│                    KAFKA KRAFT CLUSTER                       │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │   Broker 1   │  │   Broker 2   │  │   Broker 3   │       │
│  │  Controller  │  │   Follower   │  │   Follower   │       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
│         │                 │                 │                │
│         └────────────────┬──────────────────┘                │
│                          │                                   │
│               ┌──────────▼──────────┐                        │
│               │  Topic: facebook_   │                        │
│               │      comments       │                        │
│               │  Partitions: 3      │                        │
│               │  Replication: 1     │                        │
│               └─────────────────────┘                        │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Avantages KRaft
- ✅ Pas de dépendance ZooKeeper
- ✅ Architecture simplifiée
- ✅ Meilleure performance
- ✅ Configuration locale facile

### Configuration
```properties
# server.properties (KRaft)
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093
```

---

## ⚡ 3. PySpark Structured Streaming

### Pipeline de Traitement

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         SPARK STREAMING PIPELINE                              │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   │
│  │  Kafka   │──▶│  Parse   │──▶│  Clean   │──▶│ Analyze  │──▶│  Write   │   │
│  │  Source  │   │   JSON   │   │   Text   │   │   ML     │   │ Postgres │   │
│  └──────────┘   └──────────┘   └──────────┘   └──────────┘   └──────────┘   │
│       │              │              │              │              │          │
│       │              │              │              │              │          │
│   ┌───▼───┐     ┌────▼────┐   ┌────▼────┐   ┌────▼─────┐   ┌────▼────┐     │
│   │ Read  │     │ Schema  │   │Stopwords│   │ Toxicity │   │  JDBC   │     │
│   │Stream │     │ Spark   │   │ URLs    │   │Sentiment │   │  Batch  │     │
│   └───────┘     └─────────┘   │ Special │   │ Keywords │   └─────────┘     │
│                               └─────────┘   └──────────┘                    │
│                                                                               │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Étapes de Traitement

#### 1. Nettoyage du Texte
```python
- Suppression URLs (http://, www.)
- Suppression mentions (@user)
- Suppression hashtags (#topic)
- Normalisation (minuscules)
- Tokenization
```

#### 2. Suppression des Stopwords
```python
- Mots vides anglais/français
- Ponctuation
- Caractères spéciaux
```

#### 3. Vectorisation
```python
- TF-IDF pour recherche
- Embeddings (GloVe 100d) pour ML
- Word2Vec optionnel
```

#### 4. Classification
```python
- Modèle Deep Learning (toxic_model.h5)
- 6 classes de toxicité:
  • toxic
  • severe_toxic
  • obscene
  • threat
  • insult
  • identity_hate
- Analyse sentiment VADER
```

### Configuration Spark
```python
spark = SparkSession.builder \
    .appName("FacebookCommentAnalysis") \
    .master("local[*]") \
    .config("spark.sql.streaming.checkpointLocation", "./checkpoints") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()
```

---

## 🗄️ 4. PostgreSQL (Stockage)

### Schéma de Base de Données

```sql
                    DIAGRAMME ENTITÉ-RELATION
    ┌─────────────────────────────────────────────────────────┐
    │                facebook_comments_analysis                │
    ├─────────────────────────────────────────────────────────┤
    │ PK │ id              │ SERIAL                           │
    ├────┼─────────────────┼──────────────────────────────────┤
    │    │ post_id         │ TEXT                             │
    │ UK │ comment_id      │ TEXT (UNIQUE)                    │
    │    │ username        │ TEXT                             │
    │    │ comment         │ TEXT                             │
    │    │ cleaned_comment │ TEXT                             │
    │    │ toxicity_score  │ FLOAT (0-1)                      │
    │    │ label           │ TEXT (toxique/non_toxique)       │
    │    │ sentiment       │ TEXT (positive/negative/neutral) │
    │    │ sentiment_score │ FLOAT (0-1)                      │
    │    │ created_time    │ TIMESTAMP                        │
    │    │ ingestion_time  │ TIMESTAMP                        │
    └────┴─────────────────┴──────────────────────────────────┘
```

### Tables Additionnelles

```
┌─────────────────────┐     ┌─────────────────────┐
│   trending_keywords │     │      statistics     │
├─────────────────────┤     ├─────────────────────┤
│ id (PK)             │     │ id (PK)             │
│ keyword             │     │ timestamp           │
│ count               │     │ total_comments      │
│ last_updated        │     │ toxic_count         │
└─────────────────────┘     │ positive_count      │
                            │ negative_count      │
                            └─────────────────────┘
```

### Configuration PostgreSQL
```
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=toxic_coments_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=majid2020
```

---

## 📊 5. Streamlit Dashboard

### Interface Utilisateur

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  📊 Analyse en Temps Réel - Commentaires Facebook                     🔄 5s │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐            │
│  │   TOTAL    │  │  TOXIQUES  │  │  POSITIFS  │  │  NÉGATIFS  │            │
│  │   1,234    │  │    156     │  │    678     │  │    234     │            │
│  │  +12/h     │  │   12.6%    │  │   54.9%    │  │   18.9%    │            │
│  └────────────┘  └────────────┘  └────────────┘  └────────────┘            │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                    📈 TIMELINE DES COMMENTAIRES                       │   │
│  │     ╭────────────────────────────────────────────────────╮           │   │
│  │  50 │    ╭─╮                     ╭──────╮                │           │   │
│  │     │   ╱   ╲                   ╱        ╲    Total      │           │   │
│  │  25 │──╱     ╲─────────────────╱          ╲──────────    │           │   │
│  │     │ ╱       ╲         ╱────╲╱            ╲  Toxiques   │           │   │
│  │   0 │╯         ╰───────╯                    ╰─────────── │           │   │
│  │     └────────────────────────────────────────────────────┘           │   │
│  │         8h    10h   12h   14h   16h   18h   20h   22h                 │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  ┌─────────────────────────────┐  ┌─────────────────────────────────────┐   │
│  │  🥧 DISTRIBUTION SENTIMENTS │  │  📊 TOP MOTS-CLÉS                   │   │
│  │                              │  │                                     │   │
│  │     ╭───────────╮           │  │  love      ████████████ 156         │   │
│  │    ╱   Positif   ╲          │  │  great     ██████████   134         │   │
│  │   │    54.9%      │         │  │  thanks    ████████     98          │   │
│  │   │    ╭─────╮    │         │  │  amazing   ██████       76          │   │
│  │   │   │Neutre│    │         │  │  hate      ████         45          │   │
│  │    ╲  │26.2% │   ╱          │  │  terrible  ███          32          │   │
│  │     ╲ ╰─────╯  ╱            │  │                                     │   │
│  │      ╲Négatif ╱             │  │                                     │   │
│  │       ╲18.9%╱               │  │                                     │   │
│  │        ╰───╯                │  │                                     │   │
│  └─────────────────────────────┘  └─────────────────────────────────────┘   │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  💬 COMMENTAIRES RÉCENTS                              📥 Export CSV  │   │
│  ├──────────────────────────────────────────────────────────────────────┤   │
│  │  User       │ Message           │ Sentiment │ Toxique │ Score       │   │
│  ├─────────────┼───────────────────┼───────────┼─────────┼─────────────┤   │
│  │  John Doe   │ Great post! I...  │ 😊 Positif│   ❌    │   0.05     │   │
│  │  Jane Smith │ This is terrib... │ 😞 Négatif│   ✅    │   0.85     │   │
│  │  Bob Wilson │ Nice content...   │ 😊 Positif│   ❌    │   0.10     │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Fonctionnalités
- ✅ Auto-refresh toutes les 5 secondes
- ✅ Filtrage par sentiment
- ✅ Filtrage par toxicité
- ✅ Export CSV
- ✅ Graphiques interactifs (Plotly)

---

## ⚙️ Stack Technique

| Composant | Technologie | Version |
|-----------|-------------|---------|
| Langage | Python | 3.10+ |
| API Facebook | Graph API | v17.0 |
| Message Broker | Apache Kafka | 3.5+ (KRaft) |
| Stream Processing | PySpark | 3.5.0 |
| Database | PostgreSQL | 15+ |
| Dashboard | Streamlit | 1.28+ |
| ML | TensorFlow | 2.15+ |
| NLP | NLTK, VADER | Latest |

---

## 🎯 Cas d'Usage

1. **Détection de Discours Haineux**
   - Modération automatique des commentaires toxiques
   - Alertes temps réel pour les modérateurs

2. **Analyse de Réputation**
   - Suivi du sentiment global
   - Identification des tendances

3. **Modération Automatique**
   - Filtrage des commentaires inappropriés
   - Classement par niveau de risque

4. **Projet Big Data / IA**
   - Pipeline complet de traitement
   - Architecture scalable

---

## 📁 Structure des Fichiers

```
Real-Time Toxic CommentV/
├── config.py                    # Configuration centralisée
├── fetch_facebook_comments.py   # Producteur Kafka (Facebook API)
├── spark_streaming.py           # PySpark Structured Streaming
├── process_kafka_local.py       # Consommateur Kafka simple
├── dashboard.py                 # Dashboard Streamlit
├── schema.sql                   # Schéma PostgreSQL
├── setup_kafka_kraft.py         # Installation Kafka KRaft
├── requirements.txt             # Dépendances Python
├── GUIDE_DEMARRAGE.md          # Guide d'installation
├── ARCHITECTURE.md             # Ce document
│
├── model/
│   ├── toxic_model.h5          # Modèle de toxicité
│   ├── tokenizer.pkl           # Tokenizer Keras
│   └── README.md
│
├── dataset/
│   └── glove.6B/               # Embeddings GloVe
│
└── checkpoints/                # Checkpoints Spark
```

---

## 🚀 Démarrage Rapide

```bash
# 1. Installer les dépendances
pip install -r requirements.txt

# 2. Configurer PostgreSQL
psql -U postgres -d toxic_coments_db -f schema.sql

# 3. Démarrer Kafka (KRaft)
python setup_kafka_kraft.py start

# 4. Lancer le producteur Facebook
python fetch_facebook_comments.py

# 5. Lancer le streaming Spark
python spark_streaming.py

# 6. Lancer le dashboard
streamlit run dashboard.py
```

---

## 📈 Métriques de Performance

| Métrique | Valeur Cible |
|----------|--------------|
| Latence ingestion | < 1s |
| Throughput Kafka | 10K msg/s |
| Batch Spark | 10s |
| Refresh Dashboard | 5s |
| Précision Toxicité | > 90% |

---

*Documentation générée le 18 Décembre 2025*
*Version 2.0 - Architecture Locale (sans Docker)*
