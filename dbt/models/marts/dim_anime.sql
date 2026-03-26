/*
  dim_anime.sql
  ─────────────
  Anime dimension table.
  One row per unique anime — used as the lookup side of fact_reviews.

  Source: stg_anime (already cleaned and cast)
  Added: genre_primary — first element of the genres array, useful for
         simple grouping without unnesting (full genres array is kept too).
*/

WITH source AS (
    SELECT * FROM {{ ref('stg_anime') }}
)

SELECT
    anime_id,
    title_romaji                            AS title,
    COALESCE(title_english, title_romaji)   AS title_display,   -- fallback to romaji if no English title
    genres,                                                      -- full TEXT[] for detailed analysis
    genres[1]                               AS genre_primary,    -- first genre for simple bucketing
    studio,
    episodes,
    avg_score,
    release_year,
    status,
    description
FROM source
