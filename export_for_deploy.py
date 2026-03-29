"""
export_for_deploy.py

Exports mart tables from PostgreSQL to CSV files for Streamlit Cloud deployment.
Run this locally after the full pipeline has completed.

Usage: python export_for_deploy.py
"""

import os

import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

EXPORTS_DIR = os.path.join(os.path.dirname(__file__), "data", "exports")
MARTS_SCHEMA = os.getenv("DBT_MARTS_SCHEMA", '"schemaAnime_marts"')


def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "anime_db"),
        user=os.getenv("POSTGRES_USER", "anime_user"),
        password=os.getenv("POSTGRES_PASSWORD", "anime_pass"),
    )


def export_table(conn, query, filename):
    df = pd.read_sql(query, conn)
    path = os.path.join(EXPORTS_DIR, filename)
    df.to_csv(path, index=False)
    print(f"  Exported {len(df)} rows -> {filename}")
    return df


def main():
    os.makedirs(EXPORTS_DIR, exist_ok=True)
    conn = get_connection()

    print("Exporting mart tables to CSV...")

    export_table(conn, f"""
        SELECT anime_id, title, title_display, genre_primary, avg_score, release_year
        FROM {MARTS_SCHEMA}.dim_anime
        ORDER BY title_display
    """, "dim_anime.csv")

    export_table(conn, f"""
        SELECT *
        FROM {MARTS_SCHEMA}.mart_recommendations
        ORDER BY source_anime_id, rank
    """, "mart_recommendations.csv")

    export_table(conn, f"""
        SELECT
            a.genre_primary                                         AS genre,
            COUNT(*)                                                AS review_count,
            COUNT(*) FILTER (WHERE s.sentiment_label = 'positive')  AS positive,
            COUNT(*) FILTER (WHERE s.sentiment_label = 'neutral')   AS neutral,
            COUNT(*) FILTER (WHERE s.sentiment_label = 'negative')  AS negative
        FROM {MARTS_SCHEMA}.fact_reviews f
        JOIN {MARTS_SCHEMA}.mart_review_sentiment s ON f.review_id = s.review_id
        JOIN {MARTS_SCHEMA}.dim_anime a ON f.anime_id = a.anime_id
        WHERE a.genre_primary IS NOT NULL
        GROUP BY a.genre_primary
        ORDER BY review_count DESC
    """, "sentiment_by_genre.csv")

    export_table(conn, f"""
        SELECT
            f.anime_id,
            COUNT(*)                                                    AS total_reviews,
            COUNT(*) FILTER (WHERE s.sentiment_label = 'positive')      AS positive,
            COUNT(*) FILTER (WHERE s.sentiment_label = 'neutral')       AS neutral,
            COUNT(*) FILTER (WHERE s.sentiment_label = 'negative')      AS negative,
            ROUND(AVG(f.score)::NUMERIC, 1)                             AS avg_score,
            ROUND(AVG(s.sentiment_score)::NUMERIC, 3)                   AS avg_sentiment
        FROM {MARTS_SCHEMA}.fact_reviews f
        JOIN {MARTS_SCHEMA}.mart_review_sentiment s ON f.review_id = s.review_id
        GROUP BY f.anime_id
    """, "anime_sentiment_stats.csv")

    conn.close()
    print("Done! CSV files are in data/exports/")


if __name__ == "__main__":
    main()
