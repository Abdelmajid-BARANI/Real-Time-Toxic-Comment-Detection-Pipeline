-- ============================================================================
-- SCHÉMA POSTGRESQL - Analyse de Commentaires Facebook
-- Base de données: toxic_coments_db
-- ============================================================================

-- Configuration de connexion:
-- POSTGRES_HOST=localhost
-- POSTGRES_PORT=5432
-- POSTGRES_DB=toxic_coments_db
-- POSTGRES_USER=postgres
-- POSTGRES_PASSWORD=majid2020

-- ============================================================================
-- SUPPRESSION DES TABLES EXISTANTES
-- ============================================================================

DROP TABLE IF EXISTS trending_keywords CASCADE;
DROP TABLE IF EXISTS statistics CASCADE;
DROP TABLE IF EXISTS facebook_comments_analysis CASCADE;
DROP VIEW IF EXISTS daily_summary CASCADE;
DROP VIEW IF EXISTS hourly_activity CASCADE;
DROP VIEW IF EXISTS top_users CASCADE;
DROP VIEW IF EXISTS recent_comments CASCADE;

-- ============================================================================
-- TABLE PRINCIPALE: facebook_comments_analysis
-- ============================================================================
-- Stocke tous les commentaires Facebook avec leurs analyses de toxicité
-- et sentiment.

CREATE TABLE facebook_comments_analysis (
    -- Identifiants
    id SERIAL PRIMARY KEY,
    post_id TEXT NOT NULL,
    comment_id TEXT UNIQUE NOT NULL,
    
    -- Informations utilisateur
    username TEXT DEFAULT 'Unknown',
    
    -- Contenu
    comment TEXT NOT NULL,
    cleaned_comment TEXT,
    
    -- Scores de toxicité (0.0 à 1.0)
    toxicity_score FLOAT DEFAULT 0.0 CHECK (toxicity_score >= 0 AND toxicity_score <= 1),
    severe_toxic_score FLOAT DEFAULT 0.0 CHECK (severe_toxic_score >= 0 AND severe_toxic_score <= 1),
    obscene_score FLOAT DEFAULT 0.0 CHECK (obscene_score >= 0 AND obscene_score <= 1),
    threat_score FLOAT DEFAULT 0.0 CHECK (threat_score >= 0 AND threat_score <= 1),
    insult_score FLOAT DEFAULT 0.0 CHECK (insult_score >= 0 AND insult_score <= 1),
    identity_hate_score FLOAT DEFAULT 0.0 CHECK (identity_hate_score >= 0 AND identity_hate_score <= 1),
    
    -- Classification toxicité
    label TEXT DEFAULT 'non_toxique' CHECK (label IN ('toxique', 'non_toxique')),
    is_toxic BOOLEAN DEFAULT FALSE,
    
    -- Analyse de sentiment
    sentiment TEXT DEFAULT 'neutral' CHECK (sentiment IN ('positive', 'negative', 'neutral')),
    sentiment_score FLOAT DEFAULT 0.5 CHECK (sentiment_score >= 0 AND sentiment_score <= 1),
    
    -- Métadonnées
    word_count INTEGER DEFAULT 0,
    char_count INTEGER DEFAULT 0,
    keywords TEXT[],
    
    -- Timestamps
    created_time TIMESTAMP,
    ingestion_time TIMESTAMP DEFAULT NOW(),
    
    -- Flags supplémentaires
    requires_review BOOLEAN DEFAULT FALSE,
    is_processed BOOLEAN DEFAULT TRUE
);

-- ============================================================================
-- INDEX POUR PERFORMANCE
-- ============================================================================

-- Index sur les timestamps pour requêtes temporelles
CREATE INDEX idx_analysis_created_time ON facebook_comments_analysis(created_time DESC);
CREATE INDEX idx_analysis_ingestion_time ON facebook_comments_analysis(ingestion_time DESC);

-- Index sur les colonnes de filtrage
CREATE INDEX idx_analysis_is_toxic ON facebook_comments_analysis(is_toxic);
CREATE INDEX idx_analysis_label ON facebook_comments_analysis(label);
CREATE INDEX idx_analysis_sentiment ON facebook_comments_analysis(sentiment);
CREATE INDEX idx_analysis_username ON facebook_comments_analysis(username);
CREATE INDEX idx_analysis_post_id ON facebook_comments_analysis(post_id);

-- Index composites pour requêtes fréquentes
CREATE INDEX idx_analysis_toxic_sentiment ON facebook_comments_analysis(is_toxic, sentiment);
CREATE INDEX idx_analysis_date_toxic ON facebook_comments_analysis(created_time, is_toxic);
CREATE INDEX idx_analysis_toxicity_score ON facebook_comments_analysis(toxicity_score DESC);

-- Index GIN pour recherche full-text
CREATE INDEX idx_analysis_comment_gin ON facebook_comments_analysis 
    USING GIN(to_tsvector('english', comment));

-- ============================================================================
-- TABLE: STATISTICS
-- ============================================================================
-- Agrégations pré-calculées pour le dashboard

CREATE TABLE statistics (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT NOW(),
    
    -- Compteurs
    total_comments INTEGER DEFAULT 0,
    toxic_count INTEGER DEFAULT 0,
    non_toxic_count INTEGER DEFAULT 0,
    positive_count INTEGER DEFAULT 0,
    negative_count INTEGER DEFAULT 0,
    neutral_count INTEGER DEFAULT 0,
    
    -- Moyennes
    avg_toxicity_score FLOAT DEFAULT 0.0,
    avg_sentiment_score FLOAT DEFAULT 0.5,
    
    -- Pourcentages
    toxic_percentage FLOAT DEFAULT 0.0,
    positive_percentage FLOAT DEFAULT 0.0,
    negative_percentage FLOAT DEFAULT 0.0,
    
    -- Période
    period_start TIMESTAMP,
    period_end TIMESTAMP,
    period_type VARCHAR(20) DEFAULT 'realtime' 
        CHECK (period_type IN ('realtime', 'hourly', 'daily', 'weekly'))
);

CREATE INDEX idx_statistics_timestamp ON statistics(timestamp DESC);
CREATE INDEX idx_statistics_period ON statistics(period_type, period_start);

-- ============================================================================
-- TABLE: TRENDING_KEYWORDS
-- ============================================================================
-- Mots-clés les plus fréquents extraits des commentaires

CREATE TABLE trending_keywords (
    id SERIAL PRIMARY KEY,
    keyword VARCHAR(100) NOT NULL UNIQUE,
    count INTEGER DEFAULT 1,
    sentiment_avg FLOAT DEFAULT 0.5,
    toxicity_avg FLOAT DEFAULT 0.0,
    last_updated TIMESTAMP DEFAULT NOW(),
    first_seen TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_keywords_count ON trending_keywords(count DESC);
CREATE INDEX idx_keywords_updated ON trending_keywords(last_updated DESC);

-- ============================================================================
-- VUES POUR LE DASHBOARD
-- ============================================================================

-- Vue: Résumé quotidien
CREATE VIEW daily_summary AS
SELECT 
    DATE(created_time) AS date,
    COUNT(*) AS total_comments,
    SUM(CASE WHEN is_toxic THEN 1 ELSE 0 END) AS toxic_count,
    SUM(CASE WHEN NOT is_toxic THEN 1 ELSE 0 END) AS non_toxic_count,
    SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) AS positive_count,
    SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) AS negative_count,
    SUM(CASE WHEN sentiment = 'neutral' THEN 1 ELSE 0 END) AS neutral_count,
    ROUND(AVG(toxicity_score)::numeric, 4) AS avg_toxicity,
    ROUND(AVG(sentiment_score)::numeric, 4) AS avg_sentiment,
    ROUND((SUM(CASE WHEN is_toxic THEN 1 ELSE 0 END)::float / 
           NULLIF(COUNT(*), 0) * 100)::numeric, 2) AS toxic_percentage
FROM facebook_comments_analysis
WHERE created_time IS NOT NULL
GROUP BY DATE(created_time)
ORDER BY date DESC;

-- Vue: Activité horaire (24 dernières heures)
CREATE VIEW hourly_activity AS
SELECT 
    DATE_TRUNC('hour', ingestion_time) AS hour,
    COUNT(*) AS comment_count,
    SUM(CASE WHEN is_toxic THEN 1 ELSE 0 END) AS toxic_count,
    ROUND(AVG(toxicity_score)::numeric, 4) AS avg_toxicity,
    ROUND(AVG(sentiment_score)::numeric, 4) AS avg_sentiment
FROM facebook_comments_analysis
WHERE ingestion_time >= NOW() - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', ingestion_time)
ORDER BY hour DESC;

-- Vue: Top utilisateurs par activité
CREATE VIEW top_users AS
SELECT 
    username,
    COUNT(*) AS comment_count,
    SUM(CASE WHEN is_toxic THEN 1 ELSE 0 END) AS toxic_count,
    ROUND((SUM(CASE WHEN is_toxic THEN 1 ELSE 0 END)::float / 
           NULLIF(COUNT(*), 0) * 100)::numeric, 2) AS toxic_percentage,
    ROUND(AVG(sentiment_score)::numeric, 4) AS avg_sentiment,
    MAX(ingestion_time) AS last_comment
FROM facebook_comments_analysis
WHERE username IS NOT NULL AND username != 'Unknown'
GROUP BY username
ORDER BY comment_count DESC
LIMIT 50;

-- Vue: Commentaires récents
CREATE VIEW recent_comments AS
SELECT 
    id,
    comment_id,
    post_id,
    username,
    LEFT(comment, 100) AS comment_preview,
    label,
    ROUND(toxicity_score::numeric, 4) AS toxicity_score,
    sentiment,
    ROUND(sentiment_score::numeric, 4) AS sentiment_score,
    created_time,
    ingestion_time
FROM facebook_comments_analysis
ORDER BY ingestion_time DESC
LIMIT 100;

-- ============================================================================
-- FONCTIONS UTILITAIRES
-- ============================================================================

-- Fonction: Calculer et insérer les statistiques temps réel
CREATE OR REPLACE FUNCTION calculate_realtime_stats()
RETURNS void AS $$
BEGIN
    INSERT INTO statistics (
        total_comments,
        toxic_count,
        non_toxic_count,
        positive_count,
        negative_count,
        neutral_count,
        avg_toxicity_score,
        avg_sentiment_score,
        toxic_percentage,
        positive_percentage,
        negative_percentage,
        period_type,
        period_start,
        period_end
    )
    SELECT 
        COUNT(*),
        SUM(CASE WHEN is_toxic THEN 1 ELSE 0 END),
        SUM(CASE WHEN NOT is_toxic THEN 1 ELSE 0 END),
        SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END),
        SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END),
        SUM(CASE WHEN sentiment = 'neutral' THEN 1 ELSE 0 END),
        COALESCE(AVG(toxicity_score), 0),
        COALESCE(AVG(sentiment_score), 0.5),
        COALESCE((SUM(CASE WHEN is_toxic THEN 1 ELSE 0 END)::float / 
                  NULLIF(COUNT(*), 0) * 100), 0),
        COALESCE((SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END)::float / 
                  NULLIF(COUNT(*), 0) * 100), 0),
        COALESCE((SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END)::float / 
                  NULLIF(COUNT(*), 0) * 100), 0),
        'realtime',
        NOW() - INTERVAL '5 minutes',
        NOW()
    FROM facebook_comments_analysis
    WHERE ingestion_time >= NOW() - INTERVAL '5 minutes';
END;
$$ LANGUAGE plpgsql;

-- Fonction: Mettre à jour les mots-clés tendance
CREATE OR REPLACE FUNCTION update_trending_keywords(
    p_keywords TEXT[],
    p_sentiment FLOAT DEFAULT 0.5,
    p_toxicity FLOAT DEFAULT 0.0
)
RETURNS void AS $$
DECLARE
    keyword TEXT;
BEGIN
    FOREACH keyword IN ARRAY p_keywords
    LOOP
        INSERT INTO trending_keywords (keyword, count, sentiment_avg, toxicity_avg)
        VALUES (keyword, 1, p_sentiment, p_toxicity)
        ON CONFLICT (keyword) DO UPDATE SET
            count = trending_keywords.count + 1,
            sentiment_avg = (trending_keywords.sentiment_avg * trending_keywords.count + p_sentiment) / 
                           (trending_keywords.count + 1),
            toxicity_avg = (trending_keywords.toxicity_avg * trending_keywords.count + p_toxicity) / 
                          (trending_keywords.count + 1),
            last_updated = NOW();
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Fonction: Nettoyer les anciennes statistiques
CREATE OR REPLACE FUNCTION cleanup_old_statistics(days_to_keep INTEGER DEFAULT 30)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM statistics
    WHERE timestamp < NOW() - (days_to_keep || ' days')::INTERVAL
    AND period_type = 'realtime';
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- TRIGGERS
-- ============================================================================

-- Trigger: Définir automatiquement le label basé sur le score de toxicité
CREATE OR REPLACE FUNCTION set_toxicity_label()
RETURNS TRIGGER AS $$
BEGIN
    -- Définir is_toxic et label basé sur toxicity_score
    IF NEW.toxicity_score >= 0.5 THEN
        NEW.is_toxic := TRUE;
        NEW.label := 'toxique';
        NEW.requires_review := TRUE;
    ELSE
        NEW.is_toxic := FALSE;
        NEW.label := 'non_toxique';
    END IF;
    
    -- Définir le sentiment basé sur sentiment_score
    IF NEW.sentiment IS NULL OR NEW.sentiment = '' THEN
        IF NEW.sentiment_score >= 0.6 THEN
            NEW.sentiment := 'positive';
        ELSIF NEW.sentiment_score <= 0.4 THEN
            NEW.sentiment := 'negative';
        ELSE
            NEW.sentiment := 'neutral';
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_set_toxicity_label
BEFORE INSERT OR UPDATE ON facebook_comments_analysis
FOR EACH ROW
EXECUTE FUNCTION set_toxicity_label();

-- ============================================================================
-- DONNÉES DE TEST
-- ============================================================================

INSERT INTO facebook_comments_analysis 
    (post_id, comment_id, username, comment, toxicity_score, sentiment_score, created_time)
VALUES
    ('post_001', 'comment_001', 'Alice Martin', 'Great content! I really love this post!', 0.05, 0.92, NOW() - INTERVAL '2 hours'),
    ('post_001', 'comment_002', 'Bob Johnson', 'This is absolutely terrible and offensive!', 0.85, 0.15, NOW() - INTERVAL '1 hour'),
    ('post_002', 'comment_003', 'Charlie Brown', 'Nice work, thanks for sharing this information.', 0.08, 0.78, NOW() - INTERVAL '30 minutes'),
    ('post_002', 'comment_004', 'Diana Wilson', 'I disagree with this post.', 0.22, 0.35, NOW() - INTERVAL '15 minutes'),
    ('post_003', 'comment_005', 'Eve Thompson', 'Amazing! Keep up the excellent work!', 0.03, 0.95, NOW());

-- Calculer les statistiques initiales
SELECT calculate_realtime_stats();

-- ============================================================================
-- REQUÊTES UTILES POUR LE DASHBOARD STREAMLIT
-- ============================================================================

-- Statistiques globales
-- SELECT * FROM statistics ORDER BY timestamp DESC LIMIT 1;

-- Résumé des 7 derniers jours
-- SELECT * FROM daily_summary WHERE date >= CURRENT_DATE - 7;

-- Activité des 24 dernières heures
-- SELECT * FROM hourly_activity;

-- Commentaires toxiques récents
-- SELECT * FROM facebook_comments_analysis 
-- WHERE is_toxic = TRUE ORDER BY ingestion_time DESC LIMIT 20;

-- Distribution des sentiments
-- SELECT sentiment, COUNT(*) as count 
-- FROM facebook_comments_analysis GROUP BY sentiment;

-- Top 10 mots-clés
-- SELECT keyword, count FROM trending_keywords ORDER BY count DESC LIMIT 10;

-- Recherche full-text
-- SELECT * FROM facebook_comments_analysis 
-- WHERE to_tsvector('english', comment) @@ to_tsquery('english', 'keyword');

-- ============================================================================
-- MAINTENANCE
-- ============================================================================

-- Rafraîchir les statistiques matérialisées
-- REFRESH MATERIALIZED VIEW IF EXISTS ...;

-- Vacuum et analyse
-- VACUUM ANALYZE facebook_comments_analysis;
-- VACUUM ANALYZE statistics;
-- VACUUM ANALYZE trending_keywords;

-- Statistiques de taille
-- SELECT pg_size_pretty(pg_total_relation_size('facebook_comments_analysis'));

-- ============================================================================
-- COMMENTAIRES DE DOCUMENTATION
-- ============================================================================

COMMENT ON TABLE facebook_comments_analysis IS 
    'Table principale stockant les commentaires Facebook avec analyses de toxicité et sentiment';

COMMENT ON TABLE statistics IS 
    'Statistiques agrégées calculées périodiquement pour le dashboard';

COMMENT ON TABLE trending_keywords IS 
    'Mots-clés tendance extraits des commentaires avec métriques associées';

COMMENT ON VIEW daily_summary IS 
    'Résumé quotidien des commentaires avec métriques agrégées';

COMMENT ON VIEW hourly_activity IS 
    'Activité horaire des dernières 24 heures';

COMMENT ON VIEW top_users IS 
    'Top 50 utilisateurs par nombre de commentaires';

COMMENT ON VIEW recent_comments IS 
    '100 commentaires les plus récents avec aperçu';

-- ============================================================================
-- FIN DU SCHÉMA
-- ============================================================================
