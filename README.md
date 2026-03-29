# Anime Sentiment Recommendation Engine

An end-to-end data engineering + ML pipeline that extracts anime and review data from the AniList API, models it into a star schema data warehouse with dbt, orchestrates with Apache Airflow, and adds a HuggingFace sentiment layer to power anime recommendations.

**[Live Dashboard](https://anime-recommendations-project.streamlit.app/)**

## Architecture

```
AniList GraphQL API
      |
  [EXTRACT] Python + requests
  Raw JSON --> data/raw/ (simulates Data Lake)
      |
  [LOAD] Python + psycopg2
  Raw JSON --> PostgreSQL raw schema (simulates Synapse)
      |
  [TRANSFORM] dbt Core
  Staging (clean, cast, dedup) --> Marts (star schema)
      |
  [ORCHESTRATE] Apache Airflow
  Daily DAG: extract --> load --> dbt run --> dbt test --> sentiment
      |
  [ML LAYER] HuggingFace Transformers
  cardiffnlp/twitter-roberta-base-sentiment-latest
  Sentiment scores --> mart_review_sentiment table
      |
  [RECOMMEND] dbt model
  Genre matching + sentiment-weighted composite scoring --> Top 10 per anime
      |
  [DASHBOARD] Streamlit
  Interactive UI with recommendations, sentiment metrics, and genre charts
```

## Tech Stack

| Component | Tool | Azure Equivalent |
|-----------|------|-----------------|
| Raw storage | Local JSON files | Azure Data Lake |
| Database | PostgreSQL | Azure Synapse |
| Transformation | dbt Core | dbt on Synapse |
| Orchestration | Apache Airflow | Azure Data Factory |
| ML | HuggingFace Transformers | Azure ML |
| Dashboard | Streamlit | Power BI |
| CI/CD | GitHub Actions | Azure DevOps |
| Containers | Docker Compose | Azure Container Apps |

## Quick Start

### Prerequisites
- Python 3.11+
- PostgreSQL (local install or Docker)

### 1. Clone and configure
```bash
git clone https://github.com/Molnar1104/Anime-recommendations.git
cd Anime-recommendations
cp .env.example .env
# Edit .env with your PostgreSQL credentials
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Set up database
```sql
-- Connect as postgres superuser
CREATE USER your_user WITH PASSWORD 'your_password';
CREATE DATABASE your_db OWNER your_user;
\c your_db
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
GRANT ALL ON SCHEMA raw, staging, marts TO your_user;
```

### 4. Run the full pipeline
```bash
# Extract data from AniList API
python -m extract.fetch_anime

# Load raw JSON into PostgreSQL
python -m load.load_to_postgres

# Build dbt models (staging + marts)
cd dbt && dbt run && cd ..

# Score reviews with sentiment analysis
python -m ml.sentiment

# Rebuild recommendations (now with sentiment data)
cd dbt && dbt run && dbt test && cd ..

# Launch dashboard
streamlit run app.py
```

## Star Schema

```
                    fact_reviews
                   /     |      \
            dim_anime  dim_user  dim_date
                         |
              mart_review_sentiment (ML-managed)
                         |
              mart_recommendations (dbt model)
```

- **fact_reviews** -- one row per review (score, text, FKs to dimensions)
- **dim_anime** -- anime metadata (title, genres, studio, episodes, avg_score)
- **dim_user** -- reviewer profiles (username, join_date, account_age)
- **dim_date** -- calendar dimension (day, month, quarter, is_weekend)
- **mart_review_sentiment** -- ML sentiment scores (managed by sentiment.py, not dbt)
- **mart_recommendations** -- top 10 recommendations per anime by genre + sentiment

## Project Structure

```
├── app.py                  # Streamlit dashboard (CSV or DB mode)
├── export_for_deploy.py    # Export mart tables to CSV for cloud deployment
├── dags/                   # Airflow DAG (daily pipeline orchestration)
├── data/
│   ├── raw/                # Raw JSON from AniList API (gitignored)
│   └── exports/            # CSV exports for Streamlit Cloud
├── dbt/
│   ├── models/
│   │   ├── staging/        # stg_anime, stg_reviews, stg_users
│   │   └── marts/          # fact_reviews, dim_*, mart_recommendations
│   ├── tests/              # Custom SQL tests
│   └── dbt_project.yml
├── extract/                # AniList GraphQL extraction
├── load/                   # PostgreSQL raw loader
├── ml/                     # HuggingFace sentiment pipeline
├── tests/                  # pytest unit tests
├── .github/workflows/      # GitHub Actions CI
├── docker-compose.yml      # PostgreSQL + Airflow services
└── requirements.txt
```

## Key Design Decisions

- **Idempotent pipeline** -- every step is safe to re-run (TRUNCATE+INSERT, file existence checks, dbt full rebuilds)
- **Separate sentiment table** -- ML scores live in `mart_review_sentiment` (not managed by dbt) so `dbt run` doesn't wipe them
- **Star schema** -- classic warehouse design with fact + dimension tables for flexible querying
- **CSV fallback** -- dashboard reads from CSV exports on Streamlit Cloud, PostgreSQL locally

## Future Improvements

- **More data** -- increase extraction from 50 to 500+ anime with 25+ reviews each for better recommendation coverage
- **Better recommendations** -- tune the 60/40 sentiment/community score weighting, add a minimum review count threshold, penalise anime with very few reviews
- **Collaborative filtering** -- add user-based recommendations (users who liked X also liked Y) alongside the current content-based approach
- **Incremental models** -- switch dbt mart materializations from full rebuild to incremental for performance at scale
- **Cloud deployment** -- migrate PostgreSQL to Supabase/Neon for a fully cloud-hosted pipeline
- **Airflow on Docker** -- run the full Airflow scheduler via Docker Compose for true automated daily runs
- **More ML models** -- experiment with aspect-based sentiment (what specifically did reviewers like/dislike) or review summarisation

## CI/CD

Every push/PR to master triggers GitHub Actions:
- Spins up PostgreSQL service container
- Runs `pytest` (unit tests for extraction, loading, DAG structure)
- Runs `dbt run` + `dbt test` (37 data quality tests)

## License

MIT
