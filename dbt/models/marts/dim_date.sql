/*
  dim_date.sql
  ────────────
  Date dimension table.
  Covers every calendar date spanned by reviews in stg_reviews,
  plus a small buffer (7 days either side) so new reviews never fall
  outside the dimension.

  All columns are derived from the date itself — no source table needed.
  Uses generate_series to build the spine, then calculates every attribute.
*/

WITH date_spine AS (
    SELECT
        CAST(
            generate_series(
                (SELECT MIN(review_date) - INTERVAL '7 days' FROM {{ ref('stg_reviews') }}),
                (SELECT MAX(review_date) + INTERVAL '7 days' FROM {{ ref('stg_reviews') }}),
                INTERVAL '1 day'
            )
        AS DATE) AS full_date
)

SELECT
    -- Surrogate key: integer YYYYMMDD — compact, human-readable, sort-safe
    TO_CHAR(full_date, 'YYYYMMDD')::INT     AS date_id,

    full_date,

    EXTRACT(DAY   FROM full_date)::INT      AS day,
    EXTRACT(MONTH FROM full_date)::INT      AS month,
    EXTRACT(YEAR  FROM full_date)::INT      AS year,
    EXTRACT(QUARTER FROM full_date)::INT    AS quarter,

    -- ISO week number (1–53)
    EXTRACT(WEEK FROM full_date)::INT       AS week_of_year,

    -- Full month name, e.g. 'January'
    TO_CHAR(full_date, 'Month')             AS month_name,

    -- Day of week: 0 = Sunday … 6 = Saturday (PostgreSQL EXTRACT DOW)
    EXTRACT(DOW FROM full_date)::INT        AS day_of_week,
    TO_CHAR(full_date, 'Day')               AS day_name,

    -- Weekend flag: Saturday (6) or Sunday (0)
    CASE
        WHEN EXTRACT(DOW FROM full_date) IN (0, 6) THEN TRUE
        ELSE FALSE
    END                                     AS is_weekend

FROM date_spine
