/*
  assert_no_negative_scores.sql
  ──────────────────────────────
  Custom singular test: returns any row where score < 1 or score > 10.
  dbt treats a non-empty result as a test failure.
*/

SELECT
    review_id,
    score
FROM {{ ref('stg_reviews') }}
WHERE score < 1
   OR score > 10
