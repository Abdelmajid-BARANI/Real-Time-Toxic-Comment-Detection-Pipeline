"""
============================================================================
STREAMLIT DASHBOARD - Analyse en Temps Réel des Commentaires Facebook
============================================================================
Dashboard interactif pour visualiser les données analysées en temps réel.

Fonctionnalités:
- Métriques globales en temps réel
- Graphiques interactifs (timeline, distribution)
- Filtrage par sentiment et toxicité
- Export des données en CSV
- Auto-refresh configurable
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import os

# ===========================================================================
# CONFIGURATION DE LA PAGE
# ===========================================================================

st.set_page_config(
    page_title="📊 Analyse Commentaires Facebook",
    page_icon="💬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ===========================================================================
# STYLES CSS PERSONNALISÉS
# ===========================================================================

st.markdown("""
<style>
    /* Métriques */
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 10px;
        padding: 20px;
        color: white;
        text-align: center;
        margin: 10px 0;
    }
    
    /* Commentaires toxiques */
    .toxic-comment {
        background-color: #ffebee;
        border-left: 4px solid #f44336;
        padding: 10px;
        margin: 5px 0;
        border-radius: 0 5px 5px 0;
    }
    
    /* Commentaires positifs */
    .positive-comment {
        background-color: #e8f5e9;
        border-left: 4px solid #4caf50;
        padding: 10px;
        margin: 5px 0;
        border-radius: 0 5px 5px 0;
    }
    
    /* Header */
    .main-header {
        font-size: 2.5em;
        font-weight: bold;
        background: linear-gradient(90deg, #667eea, #764ba2);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 20px;
    }
    
    /* Footer */
    .footer {
        text-align: center;
        color: #888;
        padding: 20px;
        font-size: 0.9em;
    }
</style>
""", unsafe_allow_html=True)

# ===========================================================================
# CONFIGURATION BASE DE DONNÉES
# ===========================================================================

DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', '5432')),
    'database': os.getenv('POSTGRES_DB', 'toxic_coments_db'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'majid2020')
}

# ===========================================================================
# CONNEXION BASE DE DONNÉES
# ===========================================================================

@st.cache_resource
def get_database_connection():
    """Crée une connexion à la base de données avec cache"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        st.error(f"❌ Erreur de connexion à PostgreSQL: {e}")
        return None

def execute_query(query: str, params: tuple = None) -> pd.DataFrame:
    """Exécute une requête et retourne un DataFrame"""
    conn = get_database_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        return pd.read_sql(query, conn, params=params)
    except Exception as e:
        st.error(f"❌ Erreur de requête: {e}")
        return pd.DataFrame()

# ===========================================================================
# FONCTIONS DE DONNÉES
# ===========================================================================

@st.cache_data(ttl=5)
def get_global_statistics():
    """Récupère les statistiques globales"""
    query = """
    SELECT 
        COUNT(*) as total,
        COALESCE(SUM(CASE WHEN is_toxic THEN 1 ELSE 0 END), 0) as toxic,
        COALESCE(SUM(CASE WHEN NOT is_toxic THEN 1 ELSE 0 END), 0) as non_toxic,
        COALESCE(SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END), 0) as positive,
        COALESCE(SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END), 0) as negative,
        COALESCE(SUM(CASE WHEN sentiment = 'neutral' THEN 1 ELSE 0 END), 0) as neutral,
        COALESCE(ROUND(AVG(sentiment_score)::numeric, 3), 0.5) as avg_sentiment,
        COALESCE(ROUND(AVG(toxicity_score)::numeric, 3), 0) as avg_toxicity
    FROM facebook_comments_analysis
    """
    return execute_query(query)

@st.cache_data(ttl=5)
def get_recent_comments(limit: int = 50, sentiment_filter: list = None, toxic_only: bool = False):
    """Récupère les commentaires récents avec filtres"""
    query = """
    SELECT 
        id,
        username,
        comment,
        label,
        ROUND(toxicity_score::numeric, 3) as toxicity_score,
        sentiment,
        ROUND(sentiment_score::numeric, 3) as sentiment_score,
        is_toxic,
        created_time,
        ingestion_time
    FROM facebook_comments_analysis
    WHERE 1=1
    """
    
    params = []
    
    if sentiment_filter:
        placeholders = ','.join(['%s'] * len(sentiment_filter))
        query += f" AND sentiment IN ({placeholders})"
        params.extend(sentiment_filter)
    
    if toxic_only:
        query += " AND is_toxic = TRUE"
    
    query += " ORDER BY ingestion_time DESC LIMIT %s"
    params.append(limit)
    
    return execute_query(query, tuple(params))

@st.cache_data(ttl=10)
def get_timeline_data(days: int = 7):
    """Récupère les données de timeline"""
    query = """
    SELECT 
        DATE_TRUNC('hour', ingestion_time) as hour,
        COUNT(*) as total_count,
        SUM(CASE WHEN is_toxic THEN 1 ELSE 0 END) as toxic_count,
        SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) as positive_count,
        SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) as negative_count
    FROM facebook_comments_analysis
    WHERE ingestion_time >= NOW() - INTERVAL '%s days'
    GROUP BY DATE_TRUNC('hour', ingestion_time)
    ORDER BY hour
    """
    return execute_query(query, (days,))

@st.cache_data(ttl=10)
def get_daily_summary(days: int = 30):
    """Récupère le résumé quotidien"""
    query = """
    SELECT 
        DATE(ingestion_time) as date,
        COUNT(*) as total_comments,
        SUM(CASE WHEN is_toxic THEN 1 ELSE 0 END) as toxic_count,
        SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) as positive_count,
        SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) as negative_count,
        ROUND(AVG(toxicity_score)::numeric, 3) as avg_toxicity,
        ROUND(AVG(sentiment_score)::numeric, 3) as avg_sentiment
    FROM facebook_comments_analysis
    WHERE ingestion_time >= NOW() - INTERVAL '%s days'
    GROUP BY DATE(ingestion_time)
    ORDER BY date DESC
    """
    return execute_query(query, (days,))

@st.cache_data(ttl=30)
def get_top_keywords(limit: int = 20):
    """Récupère les mots-clés les plus fréquents"""
    query = """
    SELECT keyword, count 
    FROM trending_keywords 
    ORDER BY count DESC 
    LIMIT %s
    """
    return execute_query(query, (limit,))

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
    GROUP BY 1
    ORDER BY 1
    """
    return execute_query(query)

@st.cache_data(ttl=10)
def get_top_users(limit: int = 10):
    """Top utilisateurs par activité"""
    query = """
    SELECT 
        username,
        COUNT(*) as comment_count,
        SUM(CASE WHEN is_toxic THEN 1 ELSE 0 END) as toxic_count,
        ROUND(AVG(sentiment_score)::numeric, 3) as avg_sentiment
    FROM facebook_comments_analysis
    WHERE username IS NOT NULL AND username != 'Unknown'
    GROUP BY username
    ORDER BY comment_count DESC
    LIMIT %s
    """
    return execute_query(query, (limit,))

# ===========================================================================
# INTERFACE UTILISATEUR
# ===========================================================================

def main():
    """Fonction principale du dashboard"""
    
    # Header
    st.markdown('<h1 class="main-header">💬 Analyse Temps Réel - Commentaires Facebook</h1>', unsafe_allow_html=True)
    
    # Auto-refresh
    try:
        from streamlit_autorefresh import st_autorefresh
        refresh_rate = st.sidebar.selectbox(
            "🔄 Auto-refresh",
            options=[5, 10, 30, 60],
            format_func=lambda x: f"{x} secondes"
        )
        count = st_autorefresh(interval=refresh_rate * 1000, key="datarefresh")
    except ImportError:
        st.sidebar.info("💡 Installez `streamlit-autorefresh` pour l'auto-refresh")
        if st.sidebar.button("🔄 Rafraîchir"):
            st.cache_data.clear()
            st.rerun()
    
    # Sidebar - Filtres
    with st.sidebar:
        st.header("⚙️ Filtres")
        
        # Période
        st.subheader("📅 Période")
        period_options = {
            "Dernières 24h": 1,
            "7 derniers jours": 7,
            "30 derniers jours": 30,
            "Tout": 365
        }
        selected_period = st.selectbox("Afficher", list(period_options.keys()), index=1)
        days = period_options[selected_period]
        
        # Sentiment
        st.subheader("😊 Sentiment")
        sentiment_filter = st.multiselect(
            "Filtrer par sentiment",
            ["positive", "negative", "neutral"],
            default=[]
        )
        
        # Toxicité
        st.subheader("🚨 Toxicité")
        show_toxic_only = st.checkbox("Commentaires toxiques uniquement")
        
        # Nombre de résultats
        st.subheader("📊 Affichage")
        comments_limit = st.slider("Nombre de commentaires", 10, 200, 50)
        
        st.markdown("---")
        
        # Informations de connexion
        st.subheader("🔌 Connexion")
        conn = get_database_connection()
        if conn:
            st.success("✅ PostgreSQL connecté")
        else:
            st.error("❌ PostgreSQL déconnecté")
        
        st.caption(f"🕐 Dernière MAJ: {datetime.now().strftime('%H:%M:%S')}")
    
    # ===========================================================================
    # MÉTRIQUES PRINCIPALES
    # ===========================================================================
    
    st.markdown("---")
    
    stats = get_global_statistics()
    
    if not stats.empty:
        row = stats.iloc[0]
        total = int(row['total']) if row['total'] else 0
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric(
                label="📝 Total Commentaires",
                value=f"{total:,}",
                delta="Temps réel"
            )
        
        with col2:
            toxic = int(row['toxic']) if row['toxic'] else 0
            toxic_pct = (toxic / total * 100) if total > 0 else 0
            st.metric(
                label="🚨 Toxiques",
                value=f"{toxic:,}",
                delta=f"{toxic_pct:.1f}%",
                delta_color="inverse"
            )
        
        with col3:
            positive = int(row['positive']) if row['positive'] else 0
            positive_pct = (positive / total * 100) if total > 0 else 0
            st.metric(
                label="😊 Positifs",
                value=f"{positive:,}",
                delta=f"{positive_pct:.1f}%"
            )
        
        with col4:
            negative = int(row['negative']) if row['negative'] else 0
            negative_pct = (negative / total * 100) if total > 0 else 0
            st.metric(
                label="😞 Négatifs",
                value=f"{negative:,}",
                delta=f"{negative_pct:.1f}%",
                delta_color="inverse"
            )
        
        with col5:
            avg_sentiment = float(row['avg_sentiment']) if row['avg_sentiment'] else 0.5
            st.metric(
                label="📊 Sentiment Moyen",
                value=f"{avg_sentiment:.2f}",
                delta="Score 0-1"
            )
    
    # ===========================================================================
    # GRAPHIQUES PRINCIPAUX
    # ===========================================================================
    
    st.markdown("---")
    
    # Timeline
    st.subheader("📈 Timeline des Commentaires")
    
    timeline_df = get_timeline_data(days)
    
    if not timeline_df.empty:
        fig_timeline = go.Figure()
        
        fig_timeline.add_trace(go.Scatter(
            x=timeline_df['hour'],
            y=timeline_df['total_count'],
            name="Total",
            mode='lines+markers',
            line=dict(color='#1f77b4', width=2),
            fill='tozeroy',
            fillcolor='rgba(31, 119, 180, 0.1)'
        ))
        
        fig_timeline.add_trace(go.Scatter(
            x=timeline_df['hour'],
            y=timeline_df['toxic_count'],
            name="Toxiques",
            mode='lines+markers',
            line=dict(color='#d62728', width=2)
        ))
        
        fig_timeline.add_trace(go.Scatter(
            x=timeline_df['hour'],
            y=timeline_df['positive_count'],
            name="Positifs",
            mode='lines',
            line=dict(color='#2ecc71', width=1.5, dash='dash')
        ))
        
        fig_timeline.update_layout(
            xaxis_title="Date/Heure",
            yaxis_title="Nombre de commentaires",
            hovermode='x unified',
            height=400,
            legend=dict(orientation="h", yanchor="bottom", y=1.02)
        )
        
        st.plotly_chart(fig_timeline, use_container_width=True)
    else:
        st.info("📊 Aucune donnée de timeline disponible")
    
    # ===========================================================================
    # GRAPHIQUES EN COLONNES
    # ===========================================================================
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🥧 Distribution des Sentiments")
        
        if not stats.empty:
            sentiment_data = pd.DataFrame({
                'Sentiment': ['😊 Positif', '😞 Négatif', '😐 Neutre'],
                'Count': [
                    int(row['positive']) if row['positive'] else 0,
                    int(row['negative']) if row['negative'] else 0,
                    int(row['neutral']) if row['neutral'] else 0
                ]
            })
            
            fig_pie = px.pie(
                sentiment_data,
                values='Count',
                names='Sentiment',
                color='Sentiment',
                color_discrete_map={
                    '😊 Positif': '#2ecc71',
                    '😞 Négatif': '#e74c3c',
                    '😐 Neutre': '#95a5a6'
                },
                hole=0.4
            )
            
            fig_pie.update_traces(textposition='inside', textinfo='percent+label')
            fig_pie.update_layout(height=350, showlegend=False)
            
            st.plotly_chart(fig_pie, use_container_width=True)
    
    with col2:
        st.subheader("📊 Distribution de Toxicité")
        
        toxicity_dist = get_toxicity_distribution()
        
        if not toxicity_dist.empty:
            fig_toxicity = px.bar(
                toxicity_dist,
                x='range',
                y='count',
                color='count',
                color_continuous_scale='Reds',
                labels={'range': 'Score de toxicité', 'count': 'Nombre'}
            )
            
            fig_toxicity.update_layout(
                height=350,
                showlegend=False,
                xaxis_title="Score de toxicité",
                yaxis_title="Nombre de commentaires"
            )
            
            st.plotly_chart(fig_toxicity, use_container_width=True)
        else:
            st.info("📊 Aucune donnée de toxicité disponible")
    
    # ===========================================================================
    # TOP MOTS-CLÉS ET UTILISATEURS
    # ===========================================================================
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🔤 Top Mots-Clés")
        
        keywords_df = get_top_keywords(15)
        
        if not keywords_df.empty:
            fig_keywords = px.bar(
                keywords_df,
                x='count',
                y='keyword',
                orientation='h',
                color='count',
                color_continuous_scale='Viridis'
            )
            
            fig_keywords.update_layout(
                height=400,
                showlegend=False,
                xaxis_title="Fréquence",
                yaxis_title="Mot-clé",
                yaxis={'categoryorder': 'total ascending'}
            )
            
            st.plotly_chart(fig_keywords, use_container_width=True)
        else:
            st.info("📊 Aucun mot-clé disponible")
    
    with col2:
        st.subheader("👥 Top Utilisateurs")
        
        users_df = get_top_users(10)
        
        if not users_df.empty:
            fig_users = go.Figure()
            
            fig_users.add_trace(go.Bar(
                x=users_df['username'],
                y=users_df['comment_count'],
                name='Commentaires',
                marker_color='#3498db'
            ))
            
            fig_users.add_trace(go.Bar(
                x=users_df['username'],
                y=users_df['toxic_count'],
                name='Toxiques',
                marker_color='#e74c3c'
            ))
            
            fig_users.update_layout(
                height=400,
                barmode='group',
                xaxis_title="Utilisateur",
                yaxis_title="Nombre de commentaires",
                legend=dict(orientation="h", yanchor="bottom", y=1.02)
            )
            
            st.plotly_chart(fig_users, use_container_width=True)
        else:
            st.info("📊 Aucun utilisateur disponible")
    
    # ===========================================================================
    # RÉSUMÉ QUOTIDIEN
    # ===========================================================================
    
    st.markdown("---")
    st.subheader("📅 Résumé Quotidien")
    
    daily_df = get_daily_summary(days)
    
    if not daily_df.empty:
        fig_daily = make_subplots(
            rows=2, cols=1,
            subplot_titles=("Volume de Commentaires", "Évolution des Scores"),
            vertical_spacing=0.12
        )
        
        # Volume
        fig_daily.add_trace(
            go.Bar(
                x=daily_df['date'],
                y=daily_df['total_comments'],
                name="Total",
                marker_color='#3498db'
            ),
            row=1, col=1
        )
        
        fig_daily.add_trace(
            go.Bar(
                x=daily_df['date'],
                y=daily_df['toxic_count'],
                name="Toxiques",
                marker_color='#e74c3c'
            ),
            row=1, col=1
        )
        
        # Scores
        fig_daily.add_trace(
            go.Scatter(
                x=daily_df['date'],
                y=daily_df['avg_sentiment'],
                name="Sentiment",
                mode='lines+markers',
                line=dict(color='#2ecc71', width=2)
            ),
            row=2, col=1
        )
        
        fig_daily.add_trace(
            go.Scatter(
                x=daily_df['date'],
                y=daily_df['avg_toxicity'],
                name="Toxicité",
                mode='lines+markers',
                line=dict(color='#e74c3c', width=2)
            ),
            row=2, col=1
        )
        
        fig_daily.update_layout(
            height=500,
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.08)
        )
        
        st.plotly_chart(fig_daily, use_container_width=True)
    else:
        st.info("📊 Aucune donnée quotidienne disponible")
    
    # ===========================================================================
    # TABLE DES COMMENTAIRES
    # ===========================================================================
    
    st.markdown("---")
    st.subheader("💬 Commentaires Récents")
    
    comments_df = get_recent_comments(
        limit=comments_limit,
        sentiment_filter=sentiment_filter if sentiment_filter else None,
        toxic_only=show_toxic_only
    )
    
    if not comments_df.empty:
        # Colonnes à afficher
        display_cols = ['username', 'comment', 'label', 'toxicity_score', 'sentiment', 'sentiment_score', 'ingestion_time']
        
        # Vérifier que les colonnes existent
        available_cols = [col for col in display_cols if col in comments_df.columns]
        if not available_cols:
            st.warning("⚠️ Structure de données incompatible")
        else:
            display_df = comments_df[available_cols].copy()
            
            # Renommer les colonnes
            col_names = {
                'username': 'Utilisateur',
                'comment': 'Commentaire', 
                'label': 'Label',
                'toxicity_score': 'Score Toxicité',
                'sentiment': 'Sentiment',
                'sentiment_score': 'Score Sentiment',
                'ingestion_time': 'Date'
            }
            display_df.columns = [col_names.get(c, c) for c in available_cols]
            
            st.dataframe(
                display_df,
                use_container_width=True,
                height=400
            )
        
        # Export CSV
        col1, col2 = st.columns([3, 1])
        
        with col2:
            csv = comments_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Télécharger CSV",
                data=csv,
                file_name=f"comments_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    else:
        st.info("📭 Aucun commentaire ne correspond aux filtres")
    
    # ===========================================================================
    # FOOTER
    # ===========================================================================
    
    st.markdown("---")
    st.markdown("""
    <div class="footer">
        <p>📊 <strong>Dashboard Big Data</strong> - Pipeline Temps Réel</p>
        <p>Facebook Graph API → Kafka (KRaft) → Spark Streaming → PostgreSQL → Streamlit</p>
        <p>🗓️ Décembre 2025</p>
    </div>
    """, unsafe_allow_html=True)

# ===========================================================================
# POINT D'ENTRÉE
# ===========================================================================

if __name__ == "__main__":
    main()
