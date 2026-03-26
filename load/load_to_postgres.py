"""
load/load_to_postgres.py

Loads raw JSON files from data/raw/ into PostgreSQL raw schema tables.

Design principles:
- Idempotent: TRUNCATE + INSERT on every run (safe to re-run)
- Creates raw schema and tables if they don't exist
- Reads the most recent JSON file per entity
- Uses logging, not print statements
"""

import json
import logging
import os
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

RAW_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL — raw schema tables
# ---------------------------------------------------------------------------

DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.anime (
    anime_id        INTEGER PRIMARY KEY,
    title_romaji    TEXT,
    title_english   TEXT,
    genres          TEXT[],
    studio          TEXT,
    episodes        INTEGER,
    avg_score       NUMERIC(5, 2),
    year            INTEGER,
    status          TEXT,
    description     TEXT,
    loaded_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.reviews (
    review_id       INTEGER PRIMARY KEY,
    anime_id        INTEGER,
    user_id         INTEGER,
    username        TEXT,
    user_created_at BIGINT,
    score           INTEGER,
    summary         TEXT,
    review_text     TEXT,
    created_at      BIGINT,
    loaded_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.users (
    user_id     INTEGER PRIMARY KEY,
    username    TEXT,
    join_date   BIGINT,
    loaded_at   TIMESTAMPTZ DEFAULT NOW()
);
"""

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
# Helpers
# ---------------------------------------------------------------------------

def _latest_raw_file(entity: str) -> Path:
    """Return the most recently modified JSON file for the given entity."""
    folder = RAW_DATA_DIR / entity
    files = sorted(folder.glob("*.json"), reverse=True)
    if not files:
        raise FileNotFoundError(f"No raw JSON files found in {folder}. Run extraction first.")
    return files[0]


def _load_json(entity: str) -> list:
    path = _latest_raw_file(entity)
    logger.info("Reading %s", path)
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)

# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def load_anime(cur, records: list) -> int:
    cur.execute("TRUNCATE TABLE raw.anime RESTART IDENTITY CASCADE;")
    rows = [
        (
            r["anime_id"],
            r.get("title_romaji"),
            r.get("title_english"),
            r.get("genres") or [],
            r.get("studio"),
            r.get("episodes"),
            r.get("avg_score"),
            r.get("year"),
            r.get("status"),
            r.get("description"),
        )
        for r in records
    ]
    execute_values(
        cur,
        """
        INSERT INTO raw.anime
            (anime_id, title_romaji, title_english, genres, studio, episodes,
             avg_score, year, status, description)
        VALUES %s
        ON CONFLICT (anime_id) DO NOTHING
        """,
        rows,
    )
    return len(rows)


def load_reviews(cur, records: list) -> int:
    cur.execute("TRUNCATE TABLE raw.reviews RESTART IDENTITY CASCADE;")
    rows = [
        (
            r["review_id"],
            r.get("anime_id"),
            r.get("user_id"),
            r.get("username"),
            r.get("user_created_at"),
            r.get("score"),
            r.get("summary"),
            r.get("review_text"),
            r.get("created_at"),
        )
        for r in records
    ]
    execute_values(
        cur,
        """
        INSERT INTO raw.reviews
            (review_id, anime_id, user_id, username, user_created_at,
             score, summary, review_text, created_at)
        VALUES %s
        ON CONFLICT (review_id) DO NOTHING
        """,
        rows,
    )
    return len(rows)


def load_users(cur, records: list) -> int:
    cur.execute("TRUNCATE TABLE raw.users RESTART IDENTITY CASCADE;")
    rows = [
        (
            r["user_id"],
            r.get("username"),
            r.get("join_date"),
        )
        for r in records
        if r.get("user_id") is not None
    ]
    execute_values(
        cur,
        """
        INSERT INTO raw.users (user_id, username, join_date)
        VALUES %s
        ON CONFLICT (user_id) DO NOTHING
        """,
        rows,
    )
    return len(rows)

# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def run():
    logger.info("Connecting to PostgreSQL...")
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                logger.info("Creating raw schema and tables if not exists...")
                cur.execute(DDL)

                anime_records = _load_json("anime")
                n = load_anime(cur, anime_records)
                logger.info("Loaded %d rows into raw.anime.", n)

                review_records = _load_json("reviews")
                n = load_reviews(cur, review_records)
                logger.info("Loaded %d rows into raw.reviews.", n)

                user_records = _load_json("users")
                n = load_users(cur, user_records)
                logger.info("Loaded %d rows into raw.users.", n)

        logger.info("Load complete.")
    finally:
        conn.close()


if __name__ == "__main__":
    run()
