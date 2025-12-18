-- ============================================================================
-- SCHÉMA POSTGRESQL - Plateforme Analyse Commentaires Toxiques
-- ============================================================================
-- Base de données: toxic_coments_db
-- Configuration:
--   POSTGRES_HOST=postgres
--   POSTGRES_PORT=5432
--   POSTGRES_DB=toxic_coments_db
--   POSTGRES_USER=postgres
--   POSTGRES_PASSWORD=majid2020
-- ============================================================================

-- Suppression des tables existantes
DROP TABLE IF EXISTS facebook_comments_analysis CASCADE;
DROP TABLE IF EXISTS pipeline_metrics CASCADE;

-- ============================================================================
-- TABLE PRINCIPALE: facebook_comments_analysis
-- ============================================================================

CREATE TABLE facebook_comments_analysis (
    id SERIAL PRIMARY KEY,
    post_id TEXT,
    comment_id TEXT UNIQUE,
    username TEXT DEFAULT 'Unknown',
    comment TEXT NOT NULL,
    cleaned_comment TEXT,
    
    -- Scores de toxicité
    toxicity_score FLOAT DEFAULT 0.0 CHECK (toxicity_score >= 0 AND toxicity_score <= 1),
    label TEXT DEFAULT 'non_toxique' CHECK (label IN ('toxique', 'non_toxique')),
    is_toxic BOOLEAN DEFAULT FALSE,
    
    -- Analyse de sentiment
    sentiment TEXT DEFAULT 'neutral' CHECK (sentiment IN ('positive', 'negative', 'neutral')),
    sentiment_score FLOAT DEFAULT 0.5 CHECK (sentiment_score >= 0 AND sentiment_score <= 1),
    
    -- Métadonnées features
    word_count INTEGER DEFAULT 0,
    char_count INTEGER DEFAULT 0,
    
    -- Timestamps
    created_time TIMESTAMP,
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index pour performance
CREATE INDEX idx_ingestion_time ON facebook_comments_analysis(ingestion_time DESC);
CREATE INDEX idx_is_toxic ON facebook_comments_analysis(is_toxic);
CREATE INDEX idx_sentiment ON facebook_comments_analysis(sentiment);
CREATE INDEX idx_toxicity_score ON facebook_comments_analysis(toxicity_score);

-- ============================================================================
-- TABLE MÉTRIQUES PIPELINE
-- ============================================================================

CREATE TABLE pipeline_metrics (
    id SERIAL PRIMARY KEY,
    metric_name TEXT NOT NULL,
    metric_value FLOAT NOT NULL,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- VUES ANALYTIQUES
-- ============================================================================

-- Vue résumé journalier
CREATE OR REPLACE VIEW daily_summary AS
SELECT 
    DATE(ingestion_time) as date,
    COUNT(*) as total_comments,
    SUM(CASE WHEN is_toxic THEN 1 ELSE 0 END) as toxic_comments,
    ROUND(AVG(toxicity_score)::numeric, 4) as avg_toxicity,
    SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) as positive,
    SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) as negative,
    SUM(CASE WHEN sentiment = 'neutral' THEN 1 ELSE 0 END) as neutral
FROM facebook_comments_analysis
GROUP BY DATE(ingestion_time)
ORDER BY date DESC;

-- Vue activité par heure
CREATE OR REPLACE VIEW hourly_activity AS
SELECT 
    DATE_TRUNC('hour', ingestion_time) as hour,
    COUNT(*) as total,
    SUM(CASE WHEN is_toxic THEN 1 ELSE 0 END) as toxic,
    ROUND(AVG(toxicity_score)::numeric, 4) as avg_toxicity
FROM facebook_comments_analysis
WHERE ingestion_time >= NOW() - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', ingestion_time)
ORDER BY hour DESC;

-- Vue commentaires récents toxiques
CREATE OR REPLACE VIEW recent_toxic_comments AS
SELECT 
    id, username, comment, toxicity_score, sentiment, ingestion_time
FROM facebook_comments_analysis
WHERE is_toxic = TRUE
ORDER BY ingestion_time DESC
LIMIT 100;

-- ============================================================================
-- DONNÉES DE TEST (optionnel)
-- ============================================================================

-- INSERT INTO facebook_comments_analysis 
-- (post_id, comment_id, username, comment, toxicity_score, label, is_toxic, sentiment, sentiment_score, word_count, char_count)
-- VALUES
-- ('test_post_1', 'test_1', 'Alice', 'This is a great post!', 0.1, 'non_toxique', false, 'positive', 0.8, 5, 22),
-- ('test_post_1', 'test_2', 'Bob', 'I hate this garbage content', 0.85, 'toxique', true, 'negative', 0.2, 5, 28),
-- ('test_post_1', 'test_3', 'Charlie', 'Interesting perspective', 0.15, 'non_toxique', false, 'neutral', 0.5, 2, 23);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;
