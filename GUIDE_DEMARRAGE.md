# 🚀 Guide de Démarrage - Pipeline Streaming Temps Réel

## Analyse Intelligente des Commentaires Facebook

Ce guide vous accompagne pas à pas pour installer et configurer la plateforme de streaming temps réel d'analyse de commentaires Facebook.

---

## 📋 Table des Matières

1. [Prérequis](#-prérequis)
2. [Installation des Dépendances](#-installation-des-dépendances)
3. [Configuration de PostgreSQL](#-configuration-de-postgresql)
4. [Installation de Kafka (KRaft)](#-installation-de-kafka-kraft)
5. [Configuration du Projet](#-configuration-du-projet)
6. [Lancement du Pipeline](#-lancement-du-pipeline)
7. [Résolution des Problèmes](#-résolution-des-problèmes)

---

## 📦 Prérequis

### Logiciels Requis

| Logiciel | Version Minimum | Lien de Téléchargement |
|----------|-----------------|------------------------|
| Python | 3.10+ | [python.org](https://www.python.org/downloads/) |
| Java JDK | 11+ | [adoptium.net](https://adoptium.net/) |
| PostgreSQL | 13+ | [postgresql.org](https://www.postgresql.org/download/) |
| Apache Kafka | 3.5+ | [kafka.apache.org](https://kafka.apache.org/downloads) |
| Apache Spark | 3.5+ | [spark.apache.org](https://spark.apache.org/downloads.html) |

### Vérification de l'Installation

```powershell
# Vérifier Python
python --version
# Python 3.10.x ou supérieur

# Vérifier Java
java -version
# openjdk version "11.x.x" ou supérieur

# Vérifier PostgreSQL
psql --version
# psql (PostgreSQL) 15.x ou supérieur
```

---

## 🐍 Installation des Dépendances

### 1. Créer un Environnement Virtuel (Recommandé)

```powershell
# Créer l'environnement
python -m venv venv

# Activer l'environnement (Windows PowerShell)
.\venv\Scripts\Activate.ps1

# Ou (Windows CMD)
.\venv\Scripts\activate.bat
```

### 2. Installer les Dépendances Python

```powershell
# Installer toutes les dépendances
pip install -r requirements.txt
```

### 3. Fichier `requirements.txt` Complet

```txt
# Web & API
requests>=2.31.0
urllib3>=2.0.0

# Kafka
kafka-python>=2.0.2

# Spark
pyspark>=3.5.0

# PostgreSQL
psycopg2-binary>=2.9.9

# Machine Learning
tensorflow>=2.15.0
keras>=3.0.0
numpy>=1.24.0
scikit-learn>=1.3.0
joblib>=1.3.0

# NLP
nltk>=3.8.1
vaderSentiment>=3.3.2

# Dashboard
streamlit>=1.28.0
streamlit-autorefresh>=1.0.1
plotly>=5.18.0
pandas>=2.0.0

# Utilitaires
python-dotenv>=1.0.0
```

### 4. Télécharger les Ressources NLTK

```python
import nltk
nltk.download('stopwords')
nltk.download('punkt')
nltk.download('wordnet')
```

---

## 🐘 Configuration de PostgreSQL

### 1. Créer la Base de Données

```powershell
# Connexion à PostgreSQL
psql -U postgres

# Créer la base de données
CREATE DATABASE toxic_coments_db;

# Vérifier la création
\l

# Se connecter à la base
\c toxic_coments_db
```

### 2. Exécuter le Schéma SQL

```powershell
# Depuis le répertoire du projet
psql -U postgres -d toxic_coments_db -f schema.sql
```

### 3. Vérifier les Tables

```sql
-- Lister les tables
\dt

-- Vérifier la structure de la table principale
\d facebook_comments_analysis

-- Vérifier les données de test
SELECT * FROM facebook_comments_analysis LIMIT 5;
```

### 4. Configuration de Connexion

Les paramètres de connexion par défaut sont :

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=toxic_coments_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=majid2020
```

---

## 📨 Installation de Kafka (KRaft)

### Option 1 : Script Automatique (Recommandé)

```powershell
# Lancer le script d'installation
python setup_kafka_kraft.py

# Le script va :
# 1. Vérifier Java
# 2. Télécharger Kafka
# 3. Configurer KRaft
# 4. Créer les scripts de démarrage
```

### Option 2 : Installation Manuelle

#### Étape 1 : Télécharger Kafka

```powershell
# Télécharger depuis https://kafka.apache.org/downloads
# Version : kafka_2.13-3.6.1.tgz

# Extraire dans le dossier kafka/
```

#### Étape 2 : Configurer KRaft

Modifier `kafka/config/kraft/server.properties` :

```properties
# Configuration KRaft (sans ZooKeeper)
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093
advertised.listeners=PLAINTEXT://localhost:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=C:/path/to/project/kafka/kraft-combined-logs
num.partitions=3
```

#### Étape 3 : Formater le Stockage

```powershell
# Générer un ID de cluster
$KAFKA_CLUSTER_ID = "MkU3OEVBNTcwNTJENDM2Qk"

# Formater le stockage
kafka/bin/windows/kafka-storage.bat format -t $KAFKA_CLUSTER_ID -c kafka/config/kraft/server.properties
```

### Démarrer Kafka

```powershell
# Démarrer le serveur Kafka
kafka\start_kafka.bat

# Ou avec le script Python
python setup_kafka_kraft.py start
```

### Créer le Topic

```powershell
# Créer le topic facebook_comments
kafka\create_topic.bat

# Ou manuellement
kafka\bin\windows\kafka-topics.bat --create --topic facebook_comments --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### Vérifier le Topic

```powershell
# Lister les topics
kafka\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092

# Décrire le topic
kafka\bin\windows\kafka-topics.bat --describe --topic facebook_comments --bootstrap-server localhost:9092
```

---

## ⚙️ Configuration du Projet

### 1. Configuration Facebook API

Modifier `fetch_facebook_comments.py` ou définir les variables d'environnement :

```python
# Obtenir un token d'accès depuis Facebook Developers
# https://developers.facebook.com/tools/explorer/

ACCESS_TOKEN = "votre_token_facebook"
PAGE_ID = "votre_page_id"
```

### 2. Configuration Centralisée

Toute la configuration est dans `config.py`. Vous pouvez aussi utiliser des variables d'environnement :

```powershell
# Variables d'environnement (PowerShell)
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:POSTGRES_HOST = "localhost"
$env:POSTGRES_DB = "toxic_coments_db"
$env:POSTGRES_USER = "postgres"
$env:POSTGRES_PASSWORD = "majid2020"
$env:FACEBOOK_ACCESS_TOKEN = "votre_token"
$env:FACEBOOK_PAGE_ID = "votre_page_id"
```

---

## 🎬 Lancement du Pipeline

### Architecture du Pipeline

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Terminal 1 │    │  Terminal 2 │    │  Terminal 3 │    │  Terminal 4 │    │  Terminal 5 │
│    Kafka    │    │  Facebook   │    │    Spark    │    │  PostgreSQL │    │  Streamlit  │
│   Server    │    │  Producer   │    │  Streaming  │    │    (Auto)   │    │  Dashboard  │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
      │                   │                  │                  │                  │
      │                   │                  │                  │                  │
      ▼                   ▼                  ▼                  ▼                  ▼
   Port 9092          API REST            Kafka ──►        Stockage           Port 8501
                     polling              PostgreSQL       résultats          Visu web
```

### Étape par Étape

#### 📍 Terminal 1 : Démarrer Kafka

```powershell
cd "c:\Users\abdel\OneDrive\Desktop\0ENSA\IDSCC5\Big data\Real-Time Toxic CommentV"

# Démarrer Kafka
kafka\start_kafka.bat
```

Attendez de voir :
```
[INFO] Kafka Server started
```

#### 📍 Terminal 2 : Démarrer le Producteur Facebook

```powershell
cd "c:\Users\abdel\OneDrive\Desktop\0ENSA\IDSCC5\Big data\Real-Time Toxic CommentV"

# Activer l'environnement virtuel
.\venv\Scripts\Activate.ps1

# Lancer le producteur en mode continu
python fetch_facebook_comments.py

# Ou pour une seule collecte
python fetch_facebook_comments.py --once
```

Vous verrez :
```
🚀 DÉMARRAGE DU PRODUCTEUR KAFKA (Mode Temps Réel)
📥 Récupération des posts de la page...
📤 [John Doe]: Great post! I love...
✅ 5 nouveaux commentaires envoyés
```

#### 📍 Terminal 3 : Démarrer Spark Streaming

```powershell
cd "c:\Users\abdel\OneDrive\Desktop\0ENSA\IDSCC5\Big data\Real-Time Toxic CommentV"

# Activer l'environnement virtuel
.\venv\Scripts\Activate.ps1

# Lancer le streaming Spark
python spark_streaming.py
```

Vous verrez :
```
🚀 DÉMARRAGE SPARK STRUCTURED STREAMING
✅ STREAMING ACTIF
📊 En attente de commentaires depuis Kafka...
📝 Batch 1: 5 commentaires à traiter
✅ Batch 1: 5 commentaires écrits dans PostgreSQL
```

#### 📍 Terminal 4 : Démarrer le Dashboard Streamlit

```powershell
cd "c:\Users\abdel\OneDrive\Desktop\0ENSA\IDSCC5\Big data\Real-Time Toxic CommentV"

# Activer l'environnement virtuel
.\venv\Scripts\Activate.ps1

# Lancer le dashboard
streamlit run dashboard.py
```

Le dashboard s'ouvre automatiquement dans votre navigateur à :
```
http://localhost:8501
```

---

## 🔧 Scripts de Démarrage Rapide

### Script PowerShell Complet

Créez un fichier `start_pipeline.ps1` :

```powershell
# start_pipeline.ps1 - Démarrage du pipeline complet

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "   DÉMARRAGE DU PIPELINE BIG DATA      " -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$ProjectPath = "c:\Users\abdel\OneDrive\Desktop\0ENSA\IDSCC5\Big data\Real-Time Toxic CommentV"

# 1. Démarrer Kafka
Write-Host "`n🚀 Démarrage de Kafka..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$ProjectPath'; .\kafka\start_kafka.bat"
Start-Sleep -Seconds 10

# 2. Démarrer le producteur Facebook
Write-Host "📱 Démarrage du producteur Facebook..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$ProjectPath'; .\venv\Scripts\Activate.ps1; python fetch_facebook_comments.py"
Start-Sleep -Seconds 5

# 3. Démarrer Spark Streaming
Write-Host "⚡ Démarrage de Spark Streaming..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$ProjectPath'; .\venv\Scripts\Activate.ps1; python spark_streaming.py"
Start-Sleep -Seconds 10

# 4. Démarrer le Dashboard
Write-Host "📊 Démarrage du Dashboard Streamlit..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$ProjectPath'; .\venv\Scripts\Activate.ps1; streamlit run dashboard.py"

Write-Host "`n========================================" -ForegroundColor Green
Write-Host "   ✅ PIPELINE DÉMARRÉ AVEC SUCCÈS     " -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host "`n📊 Dashboard: http://localhost:8501" -ForegroundColor White
```

### Script d'Arrêt

Créez un fichier `stop_pipeline.ps1` :

```powershell
# stop_pipeline.ps1 - Arrêt du pipeline

Write-Host "Arrêt du pipeline..." -ForegroundColor Yellow

# Arrêter les processus Python
Get-Process python -ErrorAction SilentlyContinue | Stop-Process -Force

# Arrêter Kafka
& .\kafka\stop_kafka.bat

Write-Host "✅ Pipeline arrêté" -ForegroundColor Green
```

---

## 🐛 Résolution des Problèmes

### ❌ Erreur : Kafka ne démarre pas

**Symptôme** : `Connection refused localhost:9092`

**Solutions** :
1. Vérifier que Java est installé : `java -version`
2. Vérifier que le port 9092 n'est pas utilisé : `netstat -ano | findstr 9092`
3. Supprimer les logs et reformater :
   ```powershell
   Remove-Item -Recurse kafka\kraft-combined-logs
   python setup_kafka_kraft.py
   ```

### ❌ Erreur : PostgreSQL connection refused

**Symptôme** : `psycopg2.OperationalError: could not connect to server`

**Solutions** :
1. Vérifier que PostgreSQL est démarré
2. Vérifier les paramètres de connexion dans `config.py`
3. Vérifier que la base de données existe :
   ```powershell
   psql -U postgres -c "\l"
   ```

### ❌ Erreur : Modèle de toxicité non chargé

**Symptôme** : `Model not found: model/toxic_model.h5`

**Solutions** :
1. Vérifier que le fichier existe dans `model/`
2. Entraîner un nouveau modèle avec le notebook
3. Le pipeline fonctionnera en mode dégradé (sans prédiction de toxicité)

### ❌ Erreur : Token Facebook invalide

**Symptôme** : `Error validating access token`

**Solutions** :
1. Générer un nouveau token sur [Facebook Developers](https://developers.facebook.com/tools/explorer/)
2. Vérifier les permissions du token (pages_read_engagement)
3. Mettre à jour le token dans `fetch_facebook_comments.py`

### ❌ Erreur : Spark packages not found

**Symptôme** : `ClassNotFoundException: org.apache.kafka.connect`

**Solutions** :
1. Vérifier la connectivité Internet (Maven télécharge les packages)
2. Définir un miroir Maven :
   ```python
   .config("spark.jars.repositories", "https://repo1.maven.org/maven2/")
   ```

---

## 📚 Ressources Supplémentaires

### Documentation Officielle
- [Apache Kafka](https://kafka.apache.org/documentation/)
- [PySpark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Streamlit](https://docs.streamlit.io/)
- [Facebook Graph API](https://developers.facebook.com/docs/graph-api/)

### Commandes Utiles

```powershell
# Voir les messages dans Kafka
kafka\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic facebook_comments --from-beginning

# Vérifier les données dans PostgreSQL
psql -U postgres -d toxic_coments_db -c "SELECT COUNT(*) FROM facebook_comments_analysis"

# Vérifier les statistiques
psql -U postgres -d toxic_coments_db -c "SELECT * FROM daily_summary LIMIT 5"
```

---

## 🎓 Support

Pour toute question ou problème :
1. Vérifier les logs dans `logs/pipeline.log`
2. Consulter la documentation dans `ARCHITECTURE.md`
3. Exécuter les tests de diagnostic :
   ```powershell
   python config.py  # Affiche la configuration
   ```

---

*Guide mis à jour le 18 Décembre 2025*
