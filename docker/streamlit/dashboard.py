"""
============================================================================
STREAMLIT DASHBOARD - Analyse Temps Réel des Commentaires Toxiques
============================================================================
Dashboard connecté à PostgreSQL pour visualisation temps réel
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
from datetime import datetime, timedelta
import os
import time

# ===========================================================================
# CONFIGURATION PAGE
# ===========================================================================

st.set_page_config(
    page_title="📊 Analyse Commentaires Toxiques",
    page_icon="💬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ===========================================================================
# STYLES CSS
# ===========================================================================

st.markdown("""
<style>
    .main-header {
        font-size: 2.5em;
        font-weight: bold;
        background: linear-gradient(90deg, #667eea, #764ba2);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 20px;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 10px;
        padding: 20px;
        color: white;
        text-align: center;
    }
    .toxic-comment {
        background: #ffebee;
        border-left: 5px solid #e53935;
        padding: 15px;
        margin: 10px 0;
        border-radius: 0 10px 10px 0;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    }
    .toxic-comment strong {
        color: #c62828;
        font-size: 1.1em;
    }
    .toxic-comment em {
        color: #d32f2f;
        font-style: normal;
    }
    .toxic-comment small {
        color: #757575;
    }
    .safe-comment {
        background: #e8f5e9;
        border-left: 5px solid #43a047;
        padding: 15px;
        margin: 10px 0;
        border-radius: 0 10px 10px 0;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    }
    .safe-comment strong {
        color: #2e7d32;
        font-size: 1.1em;
    }
    .safe-comment em {
        color: #388e3c;
        font-style: normal;
    }
    .safe-comment small {
        color: #757575;
    }
    .pipeline-box {
        background: #f8f9fa;
        border: 2px solid #e9ecef;
        border-radius: 10px;
        padding: 15px;
        margin: 10px 0;
    }
</style>
""", unsafe_allow_html=True)

# ===========================================================================
# DATABASE CONNECTION
# ===========================================================================

DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': int(os.getenv('POSTGRES_PORT', '5432')),
    'database': os.getenv('POSTGRES_DB', 'toxic_coments_db'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'majid2020')
}

def get_connection():
    """Établit une nouvelle connexion à PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        st.error(f"❌ Connexion PostgreSQL échouée: {e}")
        return None

def execute_query(query: str, params: tuple = None) -> pd.DataFrame:
    """Exécute une requête SQL et retourne un DataFrame"""
    conn = None
    try:
        conn = get_connection()
        if conn is None:
            return pd.DataFrame()
        return pd.read_sql(query, conn, params=params)
    except Exception as e:
        st.error(f"❌ Erreur requête: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

# ===========================================================================
# DATA FUNCTIONS
# ===========================================================================

@st.cache_data(ttl=5)
def get_statistics():
    """Récupère les statistiques globales"""
    query = """
    SELECT 
        COUNT(*) as total,
        COALESCE(SUM(CASE WHEN is_toxic THEN 1 ELSE 0 END), 0) as toxic,
        COALESCE(SUM(CASE WHEN NOT is_toxic THEN 1 ELSE 0 END), 0) as non_toxic,
        COALESCE(SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END), 0) as positive,
        COALESCE(SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END), 0) as negative,
        COALESCE(SUM(CASE WHEN sentiment = 'neutral' THEN 1 ELSE 0 END), 0) as neutral,
        COALESCE(ROUND(AVG(toxicity_score)::numeric, 3), 0) as avg_toxicity,
        COALESCE(ROUND(AVG(sentiment_score)::numeric, 3), 0.5) as avg_sentiment
    FROM facebook_comments_analysis
    """
    return execute_query(query)

@st.cache_data(ttl=5)
def get_recent_comments(limit: int = 50, toxic_only: bool = False):
    """Récupère les commentaires récents"""
    query = """
    SELECT 
        id, username, comment, label, 
        ROUND(toxicity_score::numeric, 3) as toxicity_score,
        sentiment, is_toxic, ingestion_time
    FROM facebook_comments_analysis
    WHERE 1=1
    """
    if toxic_only:
        query += " AND is_toxic = TRUE"
    query += " ORDER BY ingestion_time DESC LIMIT %s"
    return execute_query(query, (limit,))

@st.cache_data(ttl=10)
def get_timeline_data(hours: int = 24):
    """Récupère les données de timeline"""
    query = """
    SELECT 
        DATE_TRUNC('hour', ingestion_time) as hour,
        COUNT(*) as total,
        SUM(CASE WHEN is_toxic THEN 1 ELSE 0 END) as toxic,
        SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) as positive,
        SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) as negative
    FROM facebook_comments_analysis
    WHERE ingestion_time >= NOW() - INTERVAL '%s hours'
    GROUP BY DATE_TRUNC('hour', ingestion_time)
    ORDER BY hour
    """
    return execute_query(query, (hours,))

@st.cache_data(ttl=10)
def get_toxicity_distribution():
    """Distribution des scores de toxicité"""
    query = """
    SELECT 
        CASE 
            WHEN toxicity_score < 0.2 THEN '0-20%'
            WHEN toxicity_score < 0.4 THEN '20-40%'
            WHEN toxicity_score < 0.6 THEN '40-60%'
            WHEN toxicity_score < 0.8 THEN '60-80%'
            ELSE '80-100%'
        END as range,
        COUNT(*) as count
    FROM facebook_comments_analysis
    GROUP BY range
    ORDER BY range
    """
    return execute_query(query)

# ===========================================================================
# MAIN DASHBOARD
# ===========================================================================

def main():
    # Header
    st.markdown('<h1 class="main-header">📊 Analyse des Commentaires Toxiques</h1>', unsafe_allow_html=True)
    
    # Sidebar
    st.sidebar.title("⚙️ Configuration")
    auto_refresh = st.sidebar.checkbox("🔄 Auto-refresh", value=True)
    refresh_interval = st.sidebar.slider("Intervalle (sec)", 5, 60, 10)
    show_toxic_only = st.sidebar.checkbox("🚨 Toxiques uniquement", value=False)
    
    # Pipeline Architecture
    with st.sidebar.expander("🏗️ Architecture Pipeline"):
        st.markdown("""
        ```
        Facebook API
            ↓
        data.raw.stream
            ↓
        data.cleaned.stream
            ↓
        data.features.stream
            ↓
        data.predictions.stream
            ↓
        PostgreSQL
            ↓
        Dashboard
        ```
        """)
    
    # Main content
    stats = get_statistics()
    
    if stats.empty or stats['total'].iloc[0] == 0:
        st.warning("⏳ En attente de données...")
        st.info("Le pipeline streaming est en cours d'exécution. Les données apparaîtront ici dès qu'elles seront disponibles.")
        
        # Show connection status
        conn = get_connection()
        if conn:
            st.success("✅ Connecté à PostgreSQL")
        else:
            st.error("❌ Déconnecté de PostgreSQL")
        
        if auto_refresh:
            time.sleep(refresh_interval)
            st.rerun()
        return
    
    # ===========================================================================
    # MÉTRIQUES PRINCIPALES
    # ===========================================================================
    
    st.subheader("📈 Métriques en Temps Réel")
    
    col1, col2, col3, col4 = st.columns(4)
    
    total = int(stats['total'].iloc[0])
    toxic = int(stats['toxic'].iloc[0])
    non_toxic = int(stats['non_toxic'].iloc[0])
    toxic_percent = (toxic / total * 100) if total > 0 else 0
    
    col1.metric("💬 Total Commentaires", f"{total:,}")
    col2.metric("🚨 Toxiques", f"{toxic:,}", f"{toxic_percent:.1f}%")
    col3.metric("✅ Non Toxiques", f"{non_toxic:,}")
    col4.metric("📊 Score Moyen", f"{stats['avg_toxicity'].iloc[0]:.2%}")
    
    # ===========================================================================
    # GRAPHIQUES
    # ===========================================================================
    
    st.subheader("📊 Visualisations")
    
    chart_col1, chart_col2 = st.columns(2)
    
    with chart_col1:
        # Pie chart toxicité
        fig_pie = px.pie(
            values=[toxic, non_toxic],
            names=['Toxique', 'Non Toxique'],
            title='Distribution Toxicité',
            color_discrete_sequence=['#e74c3c', '#2ecc71']
        )
        st.plotly_chart(fig_pie, use_container_width=True)
    
    with chart_col2:
        # Pie chart sentiment
        positive = int(stats['positive'].iloc[0])
        negative = int(stats['negative'].iloc[0])
        neutral = int(stats['neutral'].iloc[0])
        
        fig_sentiment = px.pie(
            values=[positive, negative, neutral],
            names=['Positif', 'Négatif', 'Neutre'],
            title='Distribution Sentiment',
            color_discrete_sequence=['#2ecc71', '#e74c3c', '#95a5a6']
        )
        st.plotly_chart(fig_sentiment, use_container_width=True)
    
    # Timeline
    timeline_data = get_timeline_data(24)
    if not timeline_data.empty:
        st.subheader("📈 Timeline (24h)")
        
        fig_timeline = go.Figure()
        fig_timeline.add_trace(go.Scatter(
            x=timeline_data['hour'],
            y=timeline_data['total'],
            name='Total',
            line=dict(color='#3498db', width=2)
        ))
        fig_timeline.add_trace(go.Scatter(
            x=timeline_data['hour'],
            y=timeline_data['toxic'],
            name='Toxiques',
            line=dict(color='#e74c3c', width=2)
        ))
        fig_timeline.update_layout(
            title='Volume de commentaires par heure',
            xaxis_title='Heure',
            yaxis_title='Nombre de commentaires'
        )
        st.plotly_chart(fig_timeline, use_container_width=True)
    
    # Distribution toxicité
    dist_data = get_toxicity_distribution()
    if not dist_data.empty:
        st.subheader("📊 Distribution des Scores de Toxicité")
        fig_dist = px.bar(
            dist_data,
            x='range',
            y='count',
            title='Répartition par niveau de toxicité',
            color='count',
            color_continuous_scale='Reds'
        )
        st.plotly_chart(fig_dist, use_container_width=True)
    
    # ===========================================================================
    # COMMENTAIRES RÉCENTS
    # ===========================================================================
    
    st.markdown('<h2 style="color: #FF6B6B; border-bottom: 3px solid #FF6B6B; padding-bottom: 10px;">💬 Commentaires Récents</h2>', unsafe_allow_html=True)
    
    comments = get_recent_comments(50, show_toxic_only)
    
    if not comments.empty:
        for _, row in comments.iterrows():
            is_toxic = row['is_toxic']
            css_class = "toxic-comment" if is_toxic else "safe-comment"
            icon = "🚨" if is_toxic else "✅"
            
            st.markdown(f"""
            <div class="{css_class}">
                <strong>{icon} {row['username']}</strong> - 
                Score: {row['toxicity_score']:.2%} | 
                Sentiment: {row['sentiment']}<br>
                <em>{row['comment'][:200]}...</em><br>
                <small>{row['ingestion_time']}</small>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("Aucun commentaire trouvé")
    
    # ===========================================================================
    # EXPORT
    # ===========================================================================
    
    st.subheader("📥 Export")
    
    if st.button("📥 Télécharger CSV"):
        all_comments = get_recent_comments(1000, False)
        csv = all_comments.to_csv(index=False)
        st.download_button(
            label="Télécharger",
            data=csv,
            file_name=f"toxic_comments_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #888;">
        📊 Dashboard temps réel | Pipeline: Facebook → Kafka → Spark → PostgreSQL → Streamlit
    </div>
    """, unsafe_allow_html=True)
    
    # Auto-refresh
    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()

if __name__ == "__main__":
    main()
