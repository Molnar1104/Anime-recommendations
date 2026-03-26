# Anime Sentiment Data Engineering Project

## Project Overview
A end-to-end data engineering pipeline that extracts anime and review data from the
MyAnimeList (MAL) / AniList API, models it into a star schema data warehouse, orchestrates
transformations with dbt, and adds an ML sentiment layer for anime recommendations.

The architecture mirrors the Azure production stack (Data Lake → Databricks → Synapse → dbt)
using free local equivalents so it can run on any machine.

## Architecture

```
MAL / AniList API
      ↓
  [EXTRACT]
  Python + requests
  Raw JSON → local /data/raw/ folder (simulates Data Lake)
      ↓
  [LOAD]
  Python loads raw data into PostgreSQL raw schema (simulates Synapse raw layer)
      ↓
  [TRANSFORM - Layer 1: Staging]
  dbt staging models → clean, rename, deduplicate
      ↓
  [TRANSFORM - Layer 2: Marts]
  dbt mart models → star schema (fact + dimension tables)
      ↓
  [ORCHESTRATE]
  Apache Airflow DAG ties all steps together on a schedule
      ↓
  [ML LAYER]
  HuggingFace Transformers → sentiment scores written back to warehouse
      ↓
  [OUTPUT]
  Recommendation query + optional Streamlit dashboard
```

## Tech Stack

| Component | Tool | Azure Equivalent |
|-----------|------|-----------------|
| Raw storage | Local /data/raw/ JSON files | Azure Data Lake |
| Database / warehouse | PostgreSQL (via Docker) | Azure Synapse |
| Transformation | dbt Core | dbt on Synapse |
| Orchestration | Apache Airflow | Azure Data Factory |
| Processing | Pandas / PySpark | Azure Databricks |
| ML | HuggingFace Transformers | Azure ML |
| Containerisation | Docker + Docker Compose | Azure Container |
| Version control | Git + GitHub Actions CI | Azure DevOps |

## Star Schema Design

```
fact_reviews
  - review_id (PK)
  - anime_id (FK → dim_anime)
  - user_id (FK → dim_user)
  - date_id (FK → dim_date)
  - score (INT)
  - sentiment_score (FLOAT, added in ML layer)
  - sentiment_label (VARCHAR, added in ML layer)
  - review_text (TEXT)

dim_anime
  - anime_id (PK)
  - title
  - genre
  - studio
  - episode_count
  - avg_score
  - year

dim_user
  - user_id (PK)
  - username
  - join_date
  - country

dim_date
  - date_id (PK)
  - full_date
  - day, month, year, quarter
  - is_weekend
```

## Project Structure

```
anime-de-project/
├── dags/
│   └── anime_pipeline_dag.py       # Airflow DAG definition
├── data/
│   └── raw/                        # raw JSON files land here (gitignored)
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_anime.sql
│   │   │   ├── stg_reviews.sql
│   │   │   └── stg_users.sql
│   │   └── marts/
│   │       ├── fact_reviews.sql
│   │       ├── dim_anime.sql
│   │       ├── dim_user.sql
│   │       └── dim_date.sql
│   ├── tests/
│   │   └── assert_no_negative_scores.sql
│   ├── dbt_project.yml
│   └── schema.yml                  # column tests (unique, not_null, relationships)
├── extract/
│   ├── __init__.py
│   └── fetch_anime.py              # API extraction logic
├── load/
│   ├── __init__.py
│   └── load_to_postgres.py         # loads raw JSON into PostgreSQL raw schema
├── ml/
│   ├── __init__.py
│   └── sentiment.py                # HuggingFace sentiment pipeline
├── tests/
│   └── test_extract.py             # unit tests for extraction functions
├── docker-compose.yml              # spins up PostgreSQL + Airflow
├── Dockerfile
├── requirements.txt
├── .env.example                    # API keys, DB connection strings (never commit .env)
├── .github/
│   └── workflows/
│       └── ci.yml                  # GitHub Actions: runs dbt test + pytest on push
└── README.md
```

## Phase 1 — Data Engineering Pipeline (Week 1-2)

### Step 1: Project setup
- Initialise git repo
- Create docker-compose.yml with PostgreSQL service
- Install dbt-postgres, apache-airflow, requests, pandas, psycopg2
- Create .env file for DB credentials and API keys (add to .gitignore immediately)

### Step 2: Extraction
- Register on MAL API (https://myanimelist.net/apiconfig) or use AniList GraphQL API
  (AniList is easier — no API key needed for read operations)
- Write fetch_anime.py:
  - Fetch top N anime metadata (title, genre, studio, episodes, score)
  - Fetch reviews for each anime
  - Fetch basic user data
  - Save each as raw JSON to /data/raw/{entity}/{timestamp}.json
  - Make fetch functions idempotent (check if file already exists before fetching)
  - Use logging not print statements
  - Add retry logic with exponential backoff

### Step 3: Load raw to PostgreSQL
- Write load_to_postgres.py:
  - Create a raw schema in PostgreSQL
  - Load raw JSON files into raw.anime, raw.reviews, raw.users tables
  - Use TRUNCATE + INSERT to keep it idempotent

### Step 4: dbt staging models
- Initialise dbt project inside /dbt folder
- Write staging models (one per source table):
  - stg_anime.sql — clean column names, cast types, drop nulls
  - stg_reviews.sql — deduplicate, standardise dates
  - stg_users.sql — clean usernames, parse join dates
- Add schema.yml with generic tests:
  - unique and not_null on all primary keys
  - relationships test between fact and dimensions
- Run: dbt run && dbt test

### Step 5: dbt mart models (star schema)
- Write fact_reviews.sql joining stg_reviews with stg_anime and stg_users
- Write dim_anime.sql, dim_user.sql, dim_date.sql
- Use {{ ref() }} for all cross-model references — never hardcode table names
- Run: dbt run && dbt test

### Step 6: Airflow orchestration
- Write anime_pipeline_dag.py with tasks in order:
  1. fetch_anime_task (PythonOperator)
  2. load_raw_task (PythonOperator)
  3. dbt_run_task (BashOperator: dbt run)
  4. dbt_test_task (BashOperator: dbt test)
- Schedule: @daily
- Add retry=3 and on_failure_callback logging to each task

### Step 7: CI with GitHub Actions
- Write ci.yml:
  - Trigger on push to main and pull_request
  - Steps: install deps → spin up test DB → run pytest → run dbt test
- This means every code change is automatically validated

---

## Phase 2 — ML Sentiment Layer (Week 3-4)

### Step 8: Sentiment extraction
- Write ml/sentiment.py:
  - Load HuggingFace pipeline: sentiment-analysis
    (recommended model: cardiffnlp/twitter-roberta-base-sentiment-latest)
  - For each review in fact_reviews, run sentiment inference
  - Output: sentiment_label (positive/negative/neutral) + sentiment_score (float)
  - Write results back to a new column in fact_reviews
    or a separate mart table mart_review_sentiment
- Batch process in chunks — don't load all reviews into memory at once
- Add as a new Airflow task after dbt_test_task

### Step 9: Recommendation logic
- Write a SQL query (as a dbt model) that:
  - Takes an input anime
  - Finds other anime with similar genre AND high positive sentiment ratio
  - Returns top 10 recommendations ranked by sentiment_score
- This query IS the recommendation engine — no separate ML model needed

### Step 10: Optional Streamlit dashboard
- Simple UI with:
  - Dropdown: select an anime
  - Output: top 10 recommendations with sentiment breakdown
  - Chart: sentiment distribution per genre (bar chart)
- Run: streamlit run app.py

---

## dbt Model Example (for reference)

```sql
-- models/staging/stg_reviews.sql
WITH source AS (
    SELECT * FROM raw.reviews
),
cleaned AS (
    SELECT
        review_id::INT           AS review_id,
        anime_id::INT            AS anime_id,
        user_id::INT             AS user_id,
        score::INT               AS score,
        review_text              AS review_text,
        created_at::DATE         AS review_date
    FROM source
    WHERE review_text IS NOT NULL
      AND score BETWEEN 1 AND 10
)
SELECT * FROM cleaned
```

```sql
-- models/marts/fact_reviews.sql
SELECT
    r.review_id,
    r.anime_id,
    r.user_id,
    d.date_id,
    r.score,
    r.review_text
FROM {{ ref('stg_reviews') }} r
LEFT JOIN {{ ref('dim_date') }} d ON r.review_date = d.full_date
```

---

## schema.yml Example (dbt tests)

```yaml
version: 2
models:
  - name: fact_reviews
    columns:
      - name: review_id
        tests:
          - unique
          - not_null
      - name: anime_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_anime')
              field: anime_id
      - name: score
        tests:
          - not_null
  - name: dim_anime
    columns:
      - name: anime_id
        tests:
          - unique
          - not_null
```

---

## Key Concepts This Project Demonstrates

- ELT pipeline (Extract → raw storage → transform in warehouse)
- Star schema data modelling (fact + dimension tables)
- dbt for SQL transformations with testing and documentation
- Apache Airflow for pipeline orchestration
- Idempotent pipeline design
- Docker for environment consistency
- CI/CD with GitHub Actions
- ML integrated as one step in a larger pipeline, not the whole project

---

## How to Present This in Interviews

**For Data Engineer roles:**
"I built an end-to-end ELT pipeline using the same architectural pattern as the Azure
stack — raw storage simulating a Data Lake, PostgreSQL as the warehouse, dbt for
SQL transformations with automated testing, and Airflow for orchestration. The ML
sentiment layer is one step in the pipeline, not the focus."

**For ML Engineer roles:**
"I built a sentiment-based recommendation system using HuggingFace Transformers on
top of a production-grade data pipeline, including a full star schema warehouse and
automated data quality tests."

---

## Notes for Claude Code

- Start with Phase 1 only. Do not build the ML layer until the pipeline is working end to end.
- Prioritise making the pipeline idempotent from the start — it is much harder to retrofit.
- Use DuckDB instead of PostgreSQL if you want zero setup (no Docker needed for the DB).
  DuckDB is a single file, runs in-process, and supports dbt natively.
- AniList API is recommended over MAL — GraphQL, no authentication needed for reads,
  well documented at https://anilist.gitbook.io/anilist-apiv2-docs
- Keep .env out of git from day one. Use .env.example with placeholder values instead.
