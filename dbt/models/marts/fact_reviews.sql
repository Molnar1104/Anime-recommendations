/*
  fact_reviews.sql
  ────────────────
  Central fact table of the star schema.
  One row per review — joins to all three dimension tables.

  Foreign keys:
    anime_id  → dim_anime.anime_id
    user_id   → dim_user.user_id
    date_id   → dim_date.date_id

  Sentiment data lives in a SEPARATE table (mart_review_sentiment)
  managed by ml/sentiment.py — not here. This prevents dbt run from
  wiping sentiment scores on every rebuild.
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
    r.review_text

FROM reviews r
LEFT JOIN dates d
    ON r.review_date = d.full_date
