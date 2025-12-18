# ============================================================================
# Plateforme Big Data - Analyse Temps Réel des Commentaires Toxiques
# ============================================================================

## 🏗️ Architecture

```
╔══════════════════════════════════════════════════════════════════════╗
║           ARCHITECTURE PIPELINE STREAMING MULTI-TOPICS               ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  Facebook API                                                        ║
║       ↓                                                              ║
║  Producer Python (producer-facebook)                                 ║
║       ↓                                                              ║
║  Kafka: data.raw.stream                                              ║
║       ↓                                                              ║
║  Spark Streaming (spark-normalizer)                                  ║
║       ↓                                                              ║
║  Kafka: data.cleaned.stream                                          ║
║       ↓                                                              ║
║  Spark Streaming (spark-features)                                    ║
║       ↓                                                              ║
║  Kafka: data.features.stream                                         ║
║       ↓                                                              ║
║  Spark Streaming (spark-model)                                       ║
║       ↓                                                              ║
║  Kafka: data.predictions.stream → PostgreSQL                         ║
║       ↓                                                              ║
║  Streamlit Dashboard                                                 ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
```

## 📌 Topics Kafka

| Étape | Topic Kafka | Description |
|-------|-------------|-------------|
| Ingestion brute | `data.raw.stream` | Commentaires Facebook bruts (JSON) |
| Normalisation | `data.cleaned.stream` | Données nettoyées et structurées |
| Feature engineering | `data.features.stream` | Features NLP (longueur, score, etc.) |
| Prédiction IA | `data.predictions.stream` | Résultats toxicité / sentiment |
| (Optionnel) | `data.labels.stream` | Labels réels pour évaluation |

## 🐳 Services Docker

| Service | Port | Description |
|---------|------|-------------|
| `kafka` | 9092, 9093 | Apache Kafka (KRaft mode) |
| `kafka-ui` | 8080 | Interface web Kafka |
| `postgres` | 5432 | PostgreSQL |
| `producer-facebook` | - | Producer commentaires |
| `spark-normalizer` | - | Nettoyage texte |
| `spark-features` | - | Extraction features |
| `spark-model` | - | Prédiction toxicité |
| `streamlit` | 8501 | Dashboard |

## 🚀 Démarrage Rapide

### Prérequis
- Docker & Docker Compose
- 8 GB RAM minimum
- Ports 5432, 8080, 8501, 9092 libres

### Lancement

```powershell
# Naviguer vers le dossier docker
cd docker

# Démarrer tous les services
docker-compose up --build

# Ou en arrière-plan
docker-compose up --build -d
```

### Accès aux interfaces

- **Dashboard Streamlit**: http://localhost:8501
- **Kafka UI**: http://localhost:8080
- **PostgreSQL**: localhost:5432

### Arrêt

```powershell
docker-compose down

# Avec suppression des volumes
docker-compose down -v
```

## 🗄️ PostgreSQL

### Configuration

```
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=toxic_coments_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=majid2020
```

### Table principale

```sql
CREATE TABLE facebook_comments_analysis (
  id SERIAL PRIMARY KEY,
  post_id TEXT,
  comment_id TEXT UNIQUE,
  username TEXT,
  comment TEXT,
  toxicity_score FLOAT,
  label TEXT,
  is_toxic BOOLEAN,
  sentiment TEXT,
  sentiment_score FLOAT,
  created_time TIMESTAMP,
  ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## 📊 Format des Messages Kafka

### data.raw.stream (entrée)
```json
{
  "source": "facebook",
  "post_id": "123",
  "comment_id": "456",
  "user": "John Doe",
  "message": "This is stupid",
  "created_time": "2025-01-01T10:00:00Z"
}
```

### data.predictions.stream (sortie)
```json
{
  "comment_id": "456",
  "toxicity_score": 0.91,
  "label": "toxic",
  "is_toxic": true,
  "sentiment": "negative",
  "sentiment_score": 0.2,
  "timestamp": "2025-01-01T10:00:05Z"
}
```

## ⚙️ Configuration Avancée

### Mode Production (vraie API Facebook)

1. Obtenir un token Facebook Graph API
2. Modifier le `.env`:

```env
PRODUCER_MODE=facebook
FACEBOOK_ACCESS_TOKEN=your_real_token
FACEBOOK_PAGE_ID=your_page_id
```

### Personnaliser le seuil de toxicité

```yaml
# docker-compose.yml
spark-model:
  environment:
    TOXICITY_THRESHOLD: 0.6  # Par défaut: 0.5
```

## 📁 Structure du Projet

```
docker/
├── docker-compose.yml      # Orchestration
├── init.sql               # Schéma PostgreSQL
├── config.py              # Configuration centralisée
├── producer/
│   ├── Dockerfile
│   └── producer.py        # Ingestion Facebook
├── spark/
│   ├── Dockerfile
│   ├── stream_normalizer.py   # Étape 2
│   ├── feature_builder.py     # Étape 3
│   └── model_serving.py       # Étape 4
├── streamlit/
│   ├── Dockerfile
│   └── dashboard.py       # Dashboard temps réel
└── model/                 # Modèle ML (optionnel)
    ├── toxic_model.h5
    └── tokenizer.pkl
```

## 🧪 Mode Test

Par défaut, le producer génère des commentaires de test simulés.
Cela permet de tester le pipeline sans avoir besoin d'un token Facebook valide.

Commentaires de test inclus:
- Commentaires positifs
- Commentaires négatifs
- Commentaires toxiques
- Commentaires neutres

## 📈 Monitoring

### Kafka UI (http://localhost:8080)
- Visualiser les topics
- Voir les messages en temps réel
- Monitorer les consumers

### Streamlit Dashboard (http://localhost:8501)
- Métriques en temps réel
- Distribution toxicité/sentiment
- Timeline des commentaires
- Export CSV

## 🔧 Troubleshooting

### Kafka ne démarre pas
```powershell
# Vérifier les logs
docker logs kafka

# Redémarrer
docker-compose restart kafka
```

### PostgreSQL erreur connexion
```powershell
# Attendre que Postgres soit prêt
docker-compose exec postgres pg_isready -U postgres
```

### Spark streaming erreur
```powershell
# Supprimer les checkpoints
docker-compose down -v
docker-compose up --build
```

## 📚 Technologies

- **Python 3.10**
- **Apache Kafka 3.6** (KRaft mode)
- **PySpark 3.5.0**
- **PostgreSQL 15**
- **Streamlit 1.29**
- **Docker & Docker Compose**

## 👥 Auteurs

Projet PFE Big Data / IA - ENSA

---

📊 **Dashboard**: http://localhost:8501 | 🔍 **Kafka UI**: http://localhost:8080
