/*
  stg_anime.sql
  ─────────────
  Cleans and casts raw.anime:
    • Renames columns to snake_case standard
    • Casts types explicitly
    • Drops rows where anime_id or title_romaji is NULL (unusable records)
    • Trims whitespace from text fields
    • Clips avg_score to 0–100 range to handle any API noise
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'anime') }}
),

cleaned AS (
    SELECT
        anime_id::INT                                   AS anime_id,
        TRIM(title_romaji)                              AS title_romaji,
        TRIM(title_english)                             AS title_english,
        genres                                          AS genres,          -- TEXT[]
        TRIM(studio)                                    AS studio,
        episodes::INT                                   AS episodes,
        GREATEST(0, LEAST(100, avg_score::NUMERIC(5,2)))
                                                        AS avg_score,
        year::INT                                       AS release_year,
        UPPER(TRIM(status))                             AS status,          -- normalise e.g. 'FINISHED'
        TRIM(description)                               AS description
    FROM source
    WHERE anime_id   IS NOT NULL
      AND title_romaji IS NOT NULL
)

SELECT * FROM cleaned
