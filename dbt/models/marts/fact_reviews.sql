/*
  fact_reviews.sql
  ────────────────
  Central fact table of the star schema.
  One row per review — joins to all three dimension tables.

  Foreign keys:
    anime_id  → dim_anime.anime_id
    user_id   → dim_user.user_id
    date_id   → dim_date.date_id

  Columns reserved for the ML layer (Phase 2):
    sentiment_score  FLOAT   — filled in by ml/sentiment.py
    sentiment_label  VARCHAR — 'positive' | 'neutral' | 'negative'
  Both are NULL until sentiment.py runs; schema is defined here so
  the mart table exists and dbt tests can reference it immediately.
*/

WITH reviews AS (
    SELECT * FROM {{ ref('stg_reviews') }}
),

dates AS (
    SELECT date_id, full_date FROM {{ ref('dim_date') }}
)

SELECT
    -- Keys
    r.review_id,
    r.anime_id,
    r.user_id,
    d.date_id,

    -- Measures
    r.score,

    -- Text payload (needed by ML layer)
    r.summary,
    r.review_text,

    -- ML placeholders (Phase 2 — sentiment.py writes here)
    CAST(NULL AS FLOAT)   AS sentiment_score,
    CAST(NULL AS VARCHAR) AS sentiment_label

FROM reviews r
LEFT JOIN dates d
    ON r.review_date = d.full_date
