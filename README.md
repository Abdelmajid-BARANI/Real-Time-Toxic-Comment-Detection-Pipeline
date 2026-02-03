# 🛡️ Real-Time Toxic Comment Detection Pipeline

Un pipeline de détection en temps réel de commentaires toxiques utilisant Apache Kafka, Apache Spark et Machine Learning, avec visualisation via Streamlit Dashboard.

## 📋 Description

Ce projet implémente un système de détection de commentaires toxiques en temps réel capable d'analyser et de classifier automatiquement les commentaires provenant de Facebook. Le système utilise un modèle de deep learning pour identifier les contenus toxiques et offre une interface de visualisation interactive.

## 🏗️ Architecture

Le système est composé de plusieurs composants interconnectés:

- **Producer (Kafka)**: Récupère les commentaires depuis Facebook et les publie dans Kafka
- **Stream Normalizer (Spark)**: Normalise et nettoie les données en temps réel
- **Feature Builder (Spark)**: Extrait les features nécessaires pour le modèle ML
- **Model Serving (Spark)**: Applique le modèle de classification de toxicité
- **Dashboard (Streamlit)**: Interface web pour visualiser les résultats en temps réel

```
Facebook API → Producer → Kafka → Spark Streaming → ML Model → Dashboard
```

## 🚀 Technologies Utilisées

- **Apache Kafka**: Message broker pour le streaming de données
- **Apache Spark**: Traitement distribué des flux de données
- **TensorFlow/Keras**: Modèle de deep learning pour la classification
- **Streamlit**: Interface web interactive
- **Docker & Docker Compose**: Containerisation et orchestration
- **PostgreSQL**: Stockage des résultats
- **Python**: Langage principal du projet

## 📦 Prérequis

- Docker Desktop (Windows/Mac) ou Docker Engine (Linux)
- Docker Compose
- Python 3.8+
- 8GB RAM minimum recommandé

## 🔧 Installation

### 1. Cloner le repository

```bash
git clone https://github.com/Abdelmajid-BARANI/Real-Time-Toxic-Comment-Detection-Pipeline.git
cd Toxic-Comment-DetectionV3
```

### 2. Configuration

Créer un fichier `.env` dans le dossier `docker/` basé sur `.env.example`:

```bash
cd docker
cp .env.example .env
```

Modifier les variables d'environnement selon vos besoins (tokens Facebook, etc.)

### 3. Lancer le pipeline avec Docker

```bash
# Windows PowerShell
.\docker\start_pipeline.ps1

# Linux/Mac
cd docker
docker-compose up -d
```

## 📖 Utilisation

### Démarrage rapide

1. **Lancer le pipeline complet**:
   ```powershell
   .\docker\start_pipeline.ps1
   ```

2. **Accéder au dashboard**:
   Ouvrez votre navigateur sur `http://localhost:8501`

3. **Arrêter le pipeline**:
   ```powershell
   .\docker\stop_pipeline.ps1
   ```

### Mode développement

Pour exécuter les composants individuellement:

```bash
# Producer
python docker/producer/producer.py

# Dashboard
streamlit run dashboard.py

# Fetch Facebook comments
python fetch_facebook_comments.py
```

## 📁 Structure du Projet

```
Toxic-Comment-DetectionV3/
├── docker/                      # Configuration Docker
│   ├── producer/               # Service Kafka Producer
│   ├── spark/                  # Services Spark Streaming
│   │   ├── stream_normalizer.py
│   │   ├── feature_builder.py
│   │   ├── model_serving.py
│   │   └── toxic_model.h5
│   ├── streamlit/              # Dashboard Streamlit
│   ├── docker-compose.yml      # Orchestration des services
│   └── start_pipeline.ps1      # Script de démarrage
├── model/                      # Notebooks et modèles ML
│   ├── Classification_commentaires_toxiques.ipynb
│   └── toxic_model.h5
├── config.py                   # Configuration globale
├── dashboard.py                # Application Streamlit
├── fetch_facebook_comments.py  # Script de récupération Facebook
├── schema.sql                  # Schéma de base de données
└── requirements.txt            # Dépendances Python
```

## 🔍 Fonctionnalités

- ✅ Récupération automatique des commentaires Facebook en temps réel
- ✅ Détection multi-classe de toxicité (toxic, severe_toxic, obscene, threat, insult, identity_hate)
- ✅ Traitement distribué avec Apache Spark
- ✅ Visualisation interactive avec graphiques en temps réel
- ✅ Stockage persistant des résultats dans PostgreSQL
- ✅ Architecture scalable et containerisée

## 📊 Dashboard

Le dashboard Streamlit offre:
- Visualisation en temps réel des commentaires analysés
- Statistiques sur les types de toxicité détectés
- Graphiques interactifs (distribution, timeline, etc.)
- Filtrage et recherche de commentaires
- Export des données

## 🤖 Modèle ML

Le modèle de classification est un réseau de neurones profond (CNN/LSTM) entraîné sur un large dataset de commentaires toxiques. Il prédit 6 catégories de toxicité:
- Toxic
- Severe Toxic
- Obscene
- Threat
- Insult
- Identity Hate

## 🛠️ Dépannage

### Le container Spark ne démarre pas
```bash
docker logs spark-processor
# Vérifier les logs pour identifier l'erreur
```

### Problème de connexion Kafka
```bash
# Vérifier que Kafka est accessible
docker-compose ps
docker logs kafka
```

### Quick Fix Script
En cas de problème, utilisez le script de réparation rapide:
```bash
python docker/quick_fix.py
```

## 📝 Contribution

Les contributions sont les bienvenues! N'hésitez pas à:
1. Fork le projet
2. Créer une branche (`git checkout -b feature/AmazingFeature`)
3. Commit vos changements (`git commit -m 'Add some AmazingFeature'`)
4. Push vers la branche (`git push origin feature/AmazingFeature`)
5. Ouvrir une Pull Request

## 📄 Licence

Ce projet est sous licence MIT. Voir le fichier `LICENSE` pour plus de détails.

## 👥 Auteurs

- **Abdelmajid BARANI** - [GitHub](https://github.com/Abdelmajid-BARANI)

## 🙏 Remerciements

- Dataset: [Toxic Comment Classification Challenge](https://www.kaggle.com/c/jigsaw-toxic-comment-classification-challenge)
- Équipe ENSA - Projet Big Data

## 📧 Contact

Pour toute question ou suggestion, n'hésitez pas à ouvrir une issue sur GitHub.

---

⭐ Si ce projet vous a aidé, n'hésitez pas à lui donner une étoile sur GitHub!
