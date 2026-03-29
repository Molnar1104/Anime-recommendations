/*
  stg_reviews.sql
  ───────────────
  Cleans and casts raw.reviews:
    • Deduplicates: keeps the most-recent loaded row per review_id
    • Converts Unix timestamps to DATE
    • Filters out unusable rows (NULL review_id, score out of 1–100 range,
      empty review_text)
    • Standardises text fields
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'reviews') }}
),

deduped AS (
    -- If the same review_id appears more than once, keep only the latest load
    SELECT DISTINCT ON (review_id) *
    FROM source
    ORDER BY review_id, loaded_at DESC
),

cleaned AS (
    SELECT
        review_id::INT                                              AS review_id,
        anime_id::INT                                               AS anime_id,
        user_id::INT                                                AS user_id,
        TRIM(username)                                              AS username,
        score::INT                                                  AS score,
        TRIM(summary)                                               AS summary,
        TRIM(review_text)                                           AS review_text,
        TO_TIMESTAMP(created_at)::DATE                              AS review_date,
        TO_TIMESTAMP(user_created_at)::DATE                         AS user_created_date
    FROM deduped
    WHERE review_id   IS NOT NULL
      AND review_text IS NOT NULL
      AND TRIM(review_text) <> ''
      AND score BETWEEN 1 AND 100
)

SELECT * FROM cleaned
