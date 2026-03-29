"""
app.py

Streamlit dashboard for the Anime Sentiment Recommendation Engine.

Features:
  - Dropdown: select an anime
  - Table: top 10 recommendations with sentiment breakdown
  - Bar chart: sentiment distribution per genre
  - Metrics: review count, positive ratio, avg score

Run: streamlit run app.py
"""

import os

import pandas as pd
import psycopg2
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Anime Recommendations",
    page_icon="🎬",
    layout="wide",
)

MARTS_SCHEMA = os.getenv("DBT_MARTS_SCHEMA", "schemaAnime_marts")


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "anime_db"),
        user=os.getenv("POSTGRES_USER", "anime_user"),
        password=os.getenv("POSTGRES_PASSWORD", "anime_pass"),
    )


def run_query(query: str) -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql(query, conn)


# ---------------------------------------------------------------------------
# Data loaders (cached)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def load_anime_list() -> pd.DataFrame:
    return run_query(f"""
        SELECT anime_id, title, title_display, genre_primary, avg_score, release_year
        FROM {MARTS_SCHEMA}.dim_anime
        ORDER BY title_display
    """)


@st.cache_data(ttl=300)
def load_recommendations(anime_id: int) -> pd.DataFrame:
    return run_query(f"""
        SELECT
            rank,
            recommended_title_display   AS title,
            recommended_genre           AS genre,
            recommended_studio          AS studio,
            recommended_year            AS year,
            recommended_community_score AS community_score,
            recommended_review_count    AS reviews,
            recommended_positive_ratio  AS positive_ratio,
            recommended_avg_sentiment   AS avg_sentiment,
            recommendation_score
        FROM {MARTS_SCHEMA}.mart_recommendations
        WHERE source_anime_id = {int(anime_id)}
        ORDER BY rank
    """)


@st.cache_data(ttl=300)
def load_sentiment_by_genre() -> pd.DataFrame:
    return run_query(f"""
        SELECT
            a.genre_primary                                     AS genre,
            COUNT(*)                                            AS review_count,
            COUNT(*) FILTER (WHERE f.sentiment_label = 'positive')  AS positive,
            COUNT(*) FILTER (WHERE f.sentiment_label = 'neutral')   AS neutral,
            COUNT(*) FILTER (WHERE f.sentiment_label = 'negative')  AS negative
        FROM {MARTS_SCHEMA}.fact_reviews f
        JOIN {MARTS_SCHEMA}.dim_anime a ON f.anime_id = a.anime_id
        WHERE f.sentiment_label IS NOT NULL
          AND a.genre_primary IS NOT NULL
        GROUP BY a.genre_primary
        ORDER BY review_count DESC
    """)


@st.cache_data(ttl=300)
def load_anime_sentiment_stats(anime_id: int) -> pd.DataFrame:
    return run_query(f"""
        SELECT
            COUNT(*)                                                AS total_reviews,
            COUNT(*) FILTER (WHERE sentiment_label = 'positive')    AS positive,
            COUNT(*) FILTER (WHERE sentiment_label = 'neutral')     AS neutral,
            COUNT(*) FILTER (WHERE sentiment_label = 'negative')    AS negative,
            ROUND(AVG(score)::NUMERIC, 1)                           AS avg_score,
            ROUND(AVG(sentiment_score)::NUMERIC, 3)                 AS avg_sentiment
        FROM {MARTS_SCHEMA}.fact_reviews
        WHERE anime_id = {int(anime_id)}
          AND sentiment_label IS NOT NULL
    """)


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.title("Anime Sentiment Recommendations")
st.markdown("Discover anime based on **genre similarity** and **positive review sentiment**.")

# Sidebar — anime selector
anime_df = load_anime_list()

if anime_df.empty:
    st.warning("No anime data found. Run the pipeline first (`dbt run` after extraction and loading).")
    st.stop()

st.sidebar.header("Select an Anime")
selected_title = st.sidebar.selectbox(
    "Choose an anime:",
    anime_df["title_display"].tolist(),
)

selected_row = anime_df[anime_df["title_display"] == selected_title].iloc[0]
selected_id = int(selected_row["anime_id"])

# Sidebar info
st.sidebar.markdown("---")
st.sidebar.markdown(f"**ID:** {selected_id}")
st.sidebar.markdown(f"**Genre:** {selected_row['genre_primary']}")
st.sidebar.markdown(f"**Score:** {selected_row['avg_score']}")
st.sidebar.markdown(f"**Year:** {selected_row['release_year']}")

# ---------------------------------------------------------------------------
# Main content
# ---------------------------------------------------------------------------

# Sentiment stats for the selected anime
stats = load_anime_sentiment_stats(selected_id)

if not stats.empty and stats.iloc[0]["total_reviews"] > 0:
    row = stats.iloc[0]
    st.subheader(f"Sentiment Overview — {selected_title}")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Reviews", int(row["total_reviews"]))
    col2.metric("Positive", int(row["positive"]))
    col3.metric("Neutral", int(row["neutral"]))
    col4.metric("Negative", int(row["negative"]))

    col5, col6 = st.columns(2)
    col5.metric("Avg Review Score", row["avg_score"])
    col6.metric("Avg Sentiment Confidence", row["avg_sentiment"])
else:
    st.info(f"No sentiment data for **{selected_title}** yet. Run `python -m ml.sentiment` to score reviews.")

# ---------------------------------------------------------------------------
# Recommendations table
# ---------------------------------------------------------------------------

st.subheader("Top 10 Recommendations")

recs = load_recommendations(selected_id)

if recs.empty:
    st.info("No recommendations available for this anime. It may not have enough sentiment-scored reviews, or no genre-matched anime exist.")
else:
    # Format for display
    display_df = recs.copy()
    display_df["positive_ratio"] = (display_df["positive_ratio"] * 100).round(1).astype(str) + "%"
    display_df["recommendation_score"] = display_df["recommendation_score"].round(3)
    display_df = display_df.rename(columns={
        "rank": "Rank",
        "title": "Title",
        "genre": "Genre",
        "studio": "Studio",
        "year": "Year",
        "community_score": "Score",
        "reviews": "Reviews",
        "positive_ratio": "Positive %",
        "recommendation_score": "Rec. Score",
    })

    st.dataframe(
        display_df[["Rank", "Title", "Genre", "Studio", "Year", "Score", "Reviews", "Positive %", "Rec. Score"]],
        use_container_width=True,
        hide_index=True,
    )

# ---------------------------------------------------------------------------
# Sentiment by genre chart
# ---------------------------------------------------------------------------

st.subheader("Sentiment Distribution by Genre")

genre_df = load_sentiment_by_genre()

if genre_df.empty:
    st.info("No sentiment data available yet.")
else:
    chart_df = genre_df.set_index("genre")[["positive", "neutral", "negative"]]
    st.bar_chart(chart_df)

    with st.expander("Raw genre data"):
        st.dataframe(genre_df, use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------

st.markdown("---")
st.caption("Built with Streamlit | Data: AniList API | Sentiment: HuggingFace Transformers")
