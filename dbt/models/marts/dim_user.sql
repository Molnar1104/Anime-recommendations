/*
  dim_user.sql
  ────────────
  User dimension table.
  One row per unique user — used as the lookup side of fact_reviews.

  Source: stg_users (already cleaned and cast)
  Added: account_age_days — derived metric useful for segmenting
         veteran vs new reviewers in the recommendation logic.
*/

WITH source AS (
    SELECT * FROM {{ ref('stg_users') }}
)

SELECT
    user_id,
    username,
    join_date,
    CURRENT_DATE - join_date   AS account_age_days
FROM source
