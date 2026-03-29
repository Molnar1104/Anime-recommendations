/*
  mart_recommendations.sql
  ────────────────────────
  Sentiment-based anime recommendation engine.

  For every anime, finds other anime that share at least one genre AND
  have a high ratio of positive reviews. Results are ranked by a composite
  score combining positive sentiment ratio and average review score.

  Usage:
    SELECT * FROM mart_recommendations
    WHERE source_anime_id = <your_anime_id>
    ORDER BY rank
    LIMIT 10;

  Logic:
    1. For each anime, compute sentiment stats (positive_ratio, avg_sentiment_score)
    2. Explode genres into rows so we can match on ANY shared genre
    3. Self-join: for each anime pair that shares a genre, score the target
    4. Rank targets per source anime by composite score
    5. Keep top 10 recommendations per anime
*/

WITH review_sentiment AS (
    /*
      Join fact_reviews with the ML-managed mart_review_sentiment table.
      mart_review_sentiment is NOT a dbt model — it's created and populated
      by ml/sentiment.py so that dbt run doesn't wipe sentiment scores.
    */
    SELECT
        f.anime_id,
        COUNT(*)                                                    AS review_count,
        COUNT(*) FILTER (WHERE s.sentiment_label = 'positive')      AS positive_count,
        COUNT(*) FILTER (WHERE s.sentiment_label = 'negative')      AS negative_count,
        COUNT(*) FILTER (WHERE s.sentiment_label = 'neutral')       AS neutral_count,
        ROUND(
            COUNT(*) FILTER (WHERE s.sentiment_label = 'positive')::NUMERIC
            / NULLIF(COUNT(*), 0),
            4
        )                                                           AS positive_ratio,
        ROUND(AVG(s.sentiment_score)::NUMERIC, 4)                   AS avg_sentiment_score,
        ROUND(AVG(f.score)::NUMERIC, 2)                             AS avg_review_score
    FROM {{ ref('fact_reviews') }} f
    INNER JOIN {{ source('ml', 'mart_review_sentiment') }} s
        ON f.review_id = s.review_id
    GROUP BY f.anime_id
),

anime_with_stats AS (
    -- Join anime metadata with sentiment stats
    SELECT
        a.anime_id,
        a.title,
        a.title_display,
        a.genres,
        a.genre_primary,
        a.avg_score         AS community_score,
        a.release_year,
        a.studio,
        s.review_count,
        s.positive_count,
        s.positive_ratio,
        s.avg_sentiment_score,
        s.avg_review_score
    FROM {{ ref('dim_anime') }} a
    INNER JOIN review_sentiment s ON a.anime_id = s.anime_id
),

-- Unnest genres so we can match on ANY shared genre between two anime
anime_genres AS (
    SELECT
        anime_id,
        UNNEST(genres) AS genre
    FROM anime_with_stats
),

-- Find all anime pairs that share at least one genre
genre_matches AS (
    SELECT DISTINCT
        src.anime_id   AS source_anime_id,
        tgt.anime_id   AS target_anime_id
    FROM anime_genres src
    INNER JOIN anime_genres tgt
        ON src.genre = tgt.genre
       AND src.anime_id != tgt.anime_id
),

scored AS (
    SELECT
        gm.source_anime_id,

        -- Source anime info (for convenience)
        src.title           AS source_title,
        src.genre_primary   AS source_genre,

        -- Target (recommended) anime info
        tgt.anime_id        AS recommended_anime_id,
        tgt.title           AS recommended_title,
        tgt.title_display   AS recommended_title_display,
        tgt.genre_primary   AS recommended_genre,
        tgt.studio          AS recommended_studio,
        tgt.release_year    AS recommended_year,
        tgt.community_score AS recommended_community_score,
        tgt.review_count    AS recommended_review_count,
        tgt.positive_ratio  AS recommended_positive_ratio,
        tgt.avg_sentiment_score AS recommended_avg_sentiment,
        tgt.avg_review_score    AS recommended_avg_review_score,

        -- Composite recommendation score:
        --   60% positive sentiment ratio + 40% normalised community score
        ROUND(
            (0.6 * tgt.positive_ratio)
            + (0.4 * (tgt.community_score / 100.0)),
            4
        )                   AS recommendation_score

    FROM genre_matches gm
    INNER JOIN anime_with_stats src ON gm.source_anime_id = src.anime_id
    INNER JOIN anime_with_stats tgt ON gm.target_anime_id = tgt.anime_id
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY source_anime_id
            ORDER BY recommendation_score DESC, recommended_review_count DESC
        ) AS rank
    FROM scored
)

SELECT * FROM ranked
WHERE rank <= 10
ORDER BY source_anime_id, rank
