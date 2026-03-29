/*
  stg_users.sql
  ─────────────
  Cleans and casts raw.users:
    • Deduplicates on user_id
    • Converts Unix join_date timestamp to DATE
    • Lowercases and trims usernames for consistent joins
    • Drops rows where user_id or username is NULL
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'users') }}
),

deduped AS (
    SELECT DISTINCT ON (user_id) *
    FROM source
    ORDER BY user_id, loaded_at DESC
),

cleaned AS (
    SELECT
        user_id::INT                            AS user_id,
        LOWER(TRIM(username))                   AS username,
        TO_TIMESTAMP(join_date)::DATE           AS join_date
    FROM deduped
    WHERE user_id  IS NOT NULL
      AND username IS NOT NULL
      AND TRIM(username) <> ''
)

SELECT * FROM cleaned
