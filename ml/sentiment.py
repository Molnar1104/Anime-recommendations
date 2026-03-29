"""
ml/sentiment.py

Runs sentiment analysis on anime reviews stored in the marts schema
using a HuggingFace Transformers model.

Reads review_text from fact_reviews, infers sentiment (positive/neutral/negative),
and writes results to a SEPARATE table (mart_review_sentiment) that dbt does NOT manage.

This avoids the problem where dbt run rebuilds fact_reviews and wipes sentiment scores.

Design:
- Uses cardiffnlp/twitter-roberta-base-sentiment-latest
- Batch processing in configurable chunks to control memory usage
- Idempotent: TRUNCATE + INSERT on mart_review_sentiment each run
- Truncates long reviews to the model's max token length
"""

import logging
import os
from typing import Optional

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

MODEL_NAME = "cardiffnlp/twitter-roberta-base-sentiment-latest"
BATCH_SIZE = int(os.getenv("SENTIMENT_BATCH_SIZE", "32"))
MAX_TEXT_LENGTH = 512  # characters to send to the model (roughly maps to token limit)

# Label mapping — the model outputs LABEL_0/1/2, we map to readable names
LABEL_MAP = {
    "negative": "negative",
    "neutral": "neutral",
    "positive": "positive",
    # Some model versions output LABEL_0, LABEL_1, LABEL_2
    "LABEL_0": "negative",
    "LABEL_1": "neutral",
    "LABEL_2": "positive",
}

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "anime_db"),
        user=os.getenv("POSTGRES_USER", "anime_user"),
        password=os.getenv("POSTGRES_PASSWORD", "anime_pass"),
    )

# ---------------------------------------------------------------------------
# Sentiment pipeline
# ---------------------------------------------------------------------------

def load_pipeline():
    """Load the HuggingFace sentiment-analysis pipeline (lazy import)."""
    from transformers import pipeline

    logger.info("Loading model: %s", MODEL_NAME)
    return pipeline(
        "sentiment-analysis",
        model=MODEL_NAME,
        tokenizer=MODEL_NAME,
        truncation=True,
        max_length=128,
    )


def classify_texts(pipe, texts: list[str]) -> list[dict]:
    """Run sentiment inference on a list of texts.

    Returns list of {"label": str, "score": float} dicts.
    """
    # Truncate long texts before sending to the model
    truncated = [t[:MAX_TEXT_LENGTH] if t else "" for t in texts]
    results = pipe(truncated, batch_size=BATCH_SIZE)
    return [
        {
            "label": LABEL_MAP.get(r["label"], r["label"]),
            "score": round(r["score"], 4),
        }
        for r in results
    ]


def fetch_reviews(conn, limit: Optional[int] = None) -> list[tuple]:
    """Fetch reviews that need sentiment scoring.

    Returns list of (review_id, review_text) tuples.
    """
    schema = _marts_schema()
    query = f"""
        SELECT review_id, COALESCE(review_text, summary, '')
        FROM {schema}.fact_reviews
        ORDER BY review_id
    """
    if limit:
        query += f" LIMIT {int(limit)}"

    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


def _ensure_sentiment_table(cur):
    """Create the mart_review_sentiment table if it doesn't exist.

    This table is NOT managed by dbt — it's owned by the ML layer.
    """
    schema = _marts_schema()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.mart_review_sentiment (
            review_id       INTEGER PRIMARY KEY,
            sentiment_label VARCHAR NOT NULL,
            sentiment_score FLOAT NOT NULL,
            scored_at       TIMESTAMPTZ DEFAULT NOW()
        )
    """)


def write_sentiments(conn, results: list[tuple]):
    """Write sentiment results to mart_review_sentiment (separate table).

    Args:
        results: list of (review_id, sentiment_label, sentiment_score) tuples.
    """
    schema = _marts_schema()
    with conn.cursor() as cur:
        _ensure_sentiment_table(cur)

        # Idempotent: truncate and reinsert
        cur.execute(f"TRUNCATE TABLE {schema}.mart_review_sentiment")

        execute_values(
            cur,
            f"""
            INSERT INTO {schema}.mart_review_sentiment
                (review_id, sentiment_label, sentiment_score)
            VALUES %s
            """,
            results,
        )

        logger.info("Wrote %d rows to %s.mart_review_sentiment.", len(results), schema)


def _marts_schema() -> str:
    """Return the marts schema name.

    dbt appends the model schema to the target schema, e.g. 'schemaAnime_marts'.
    In CI the schema is just 'public_marts'. Read from env or default.
    """
    return os.getenv("DBT_MARTS_SCHEMA", '"schemaAnime_marts"')

# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def run(limit: Optional[int] = None):
    """Run the full sentiment scoring pipeline."""
    pipe = load_pipeline()

    conn = get_connection()
    try:
        reviews = fetch_reviews(conn, limit=limit)
        if not reviews:
            logger.info("No reviews found in fact_reviews. Skipping.")
            return

        logger.info("Scoring %d reviews...", len(reviews))

        # Process in chunks
        all_results = []
        for i in range(0, len(reviews), BATCH_SIZE):
            chunk = reviews[i : i + BATCH_SIZE]
            review_ids = [r[0] for r in chunk]
            texts = [r[1] for r in chunk]

            sentiments = classify_texts(pipe, texts)

            for rid, s in zip(review_ids, sentiments):
                all_results.append((rid, s["label"], s["score"]))

            logger.info(
                "  Processed %d / %d reviews.",
                min(i + BATCH_SIZE, len(reviews)),
                len(reviews),
            )

        with conn:
            write_sentiments(conn, all_results)

        logger.info("Sentiment scoring complete.")
    finally:
        conn.close()


if __name__ == "__main__":
    run()
