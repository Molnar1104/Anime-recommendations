# Anime Sentiment Data Engineering Project

An end-to-end ELT pipeline that extracts anime and review data from the AniList API,
models it into a star schema data warehouse with dbt, orchestrates with Apache Airflow,
and adds an ML sentiment layer for anime recommendations.

## Architecture

```
AniList API → Python Extract → /data/raw/ (Data Lake)
           → PostgreSQL raw schema (Warehouse)
           → dbt staging + marts (Star Schema)
           → Airflow DAG (Orchestration)
           → HuggingFace Sentiment (ML Layer)
           → Recommendation query + Streamlit dashboard
```

## Quick Start

### Prerequisites
- Docker + Docker Compose
- Python 3.11+

### 1. Clone and configure
```bash
git clone <repo>
cd anime-de-project
cp .env.example .env
# Edit .env with your credentials
```

### 2. Start services
```bash
docker-compose up -d
```

### 3. Install Python dependencies
```bash
pip install -r requirements.txt
```

### 4. Run extraction
```bash
python -m extract.fetch_anime
```

### 5. Load to PostgreSQL
```bash
python -m load.load_to_postgres
```

### 6. Run dbt transformations
```bash
cd dbt
dbt run
dbt test
```

## Project Structure

```
├── dags/               # Airflow DAG definitions
├── data/raw/           # Raw JSON from API (gitignored)
├── dbt/                # dbt project (staging + marts)
├── extract/            # API extraction scripts
├── load/               # PostgreSQL loader
├── ml/                 # HuggingFace sentiment pipeline
├── tests/              # Python unit tests
└── docker-compose.yml  # PostgreSQL + Airflow services
```

## Star Schema

- `fact_reviews` — review scores, text, sentiment scores
- `dim_anime` — anime metadata (title, genre, studio, episodes)
- `dim_user` — user profiles
- `dim_date` — date dimension table
