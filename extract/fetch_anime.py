"""
extract/fetch_anime.py

Fetches anime metadata, reviews, and user data from the AniList GraphQL API.
Saves each dataset as raw JSON to /data/raw/{entity}/{timestamp}.json.

Design principles:
- Idempotent: skips fetch if a file for today already exists
- Uses logging, not print statements
- Retry with exponential backoff on transient errors
"""

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ANILIST_URL = "https://graphql.anilist.co"
RAW_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"
DEFAULT_TOP_N = 50  # number of anime to fetch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# GraphQL queries
# ---------------------------------------------------------------------------

ANIME_QUERY = """
query ($page: Int, $perPage: Int) {
  Page(page: $page, perPage: $perPage) {
    pageInfo {
      total
      currentPage
      lastPage
      hasNextPage
    }
    media(type: ANIME, sort: SCORE_DESC, status: FINISHED) {
      id
      title {
        romaji
        english
      }
      genres
      studios(isMain: true) {
        nodes {
          name
        }
      }
      episodes
      averageScore
      startDate {
        year
      }
      status
      description(asHtml: false)
    }
  }
}
"""

REVIEWS_QUERY = """
query ($mediaId: Int, $page: Int, $perPage: Int) {
  Page(page: $page, perPage: $perPage) {
    pageInfo {
      hasNextPage
    }
    reviews(mediaId: $mediaId, sort: CREATED_AT_DESC) {
      id
      mediaId
      user {
        id
        name
        createdAt
        options {
          profileColor
        }
      }
      score
      summary
      body
      createdAt
    }
  }
}
"""

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _post_with_retry(
    url: str,
    payload: dict,
    max_retries: int = 5,
    base_delay: float = 1.0,
) -> dict:
    """POST a GraphQL payload with exponential backoff on transient errors."""
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                timeout=30,
            )
            # AniList rate-limit: 429 or 500-range → retry
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", base_delay * attempt))
                logger.warning("Rate limited. Waiting %ss before retry %s/%s.", retry_after, attempt, max_retries)
                time.sleep(retry_after)
                continue
            if response.status_code >= 500:
                delay = base_delay * (2 ** (attempt - 1))
                logger.warning("Server error %s. Retrying in %.1fs (%s/%s).", response.status_code, delay, attempt, max_retries)
                time.sleep(delay)
                continue
            response.raise_for_status()
            data = response.json()
            if "errors" in data:
                raise ValueError(f"GraphQL errors: {data['errors']}")
            return data
        except requests.exceptions.ConnectionError as exc:
            delay = base_delay * (2 ** (attempt - 1))
            logger.warning("Connection error: %s. Retrying in %.1fs (%s/%s).", exc, delay, attempt, max_retries)
            time.sleep(delay)

    raise RuntimeError(f"All {max_retries} retries exhausted for {url}")


# ---------------------------------------------------------------------------
# Raw file helpers
# ---------------------------------------------------------------------------

def _today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _output_path(entity: str) -> Path:
    """Return the output path for today's raw file; create parent dirs."""
    folder = RAW_DATA_DIR / entity
    folder.mkdir(parents=True, exist_ok=True)
    return folder / f"{_today_str()}.json"


def _already_fetched(entity: str) -> bool:
    """Return True if today's raw file already exists (idempotency check)."""
    path = _output_path(entity)
    if path.exists():
        logger.info("Raw file already exists for '%s' today — skipping fetch.", entity)
        return True
    return False


def _save(entity: str, records: list) -> Path:
    path = _output_path(entity)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(records, fh, ensure_ascii=False, indent=2)
    logger.info("Saved %d records to %s", len(records), path)
    return path


# ---------------------------------------------------------------------------
# Fetch functions
# ---------------------------------------------------------------------------

def fetch_anime(top_n: int = DEFAULT_TOP_N) -> list:
    """
    Fetch top N anime ordered by score from AniList.
    Returns a list of anime dicts and saves raw JSON.
    """
    if _already_fetched("anime"):
        return _load_raw("anime")

    logger.info("Fetching top %d anime from AniList...", top_n)
    records = []
    page = 1
    per_page = min(top_n, 50)  # AniList max perPage is 50

    while len(records) < top_n:
        data = _post_with_retry(
            ANILIST_URL,
            {"query": ANIME_QUERY, "variables": {"page": page, "perPage": per_page}},
        )
        media_list = data["data"]["Page"]["media"]
        page_info = data["data"]["Page"]["pageInfo"]

        for item in media_list:
            records.append(_flatten_anime(item))
            if len(records) >= top_n:
                break

        logger.info("Fetched page %d/%d (%d anime so far).", page, page_info["lastPage"], len(records))

        if not page_info["hasNextPage"] or len(records) >= top_n:
            break
        page += 1
        time.sleep(0.5)  # be a polite API citizen

    _save("anime", records)
    return records


def fetch_reviews(anime_records: list, max_reviews_per_anime: int = 5) -> list:
    """
    Fetch up to max_reviews_per_anime reviews for each anime in anime_records.
    Returns a flat list of review dicts and saves raw JSON.
    """
    if _already_fetched("reviews"):
        return _load_raw("reviews")

    logger.info("Fetching reviews for %d anime...", len(anime_records))
    all_reviews = []

    for i, anime in enumerate(anime_records):
        anime_id = anime["anime_id"]
        logger.info("[%d/%d] Fetching reviews for anime_id=%s (%s).", i + 1, len(anime_records), anime_id, anime.get("title_english") or anime.get("title_romaji"))

        data = _post_with_retry(
            ANILIST_URL,
            {"query": REVIEWS_QUERY, "variables": {"mediaId": anime_id, "page": 1, "perPage": max_reviews_per_anime}},
        )
        reviews = data["data"]["Page"]["reviews"]
        for r in reviews:
            all_reviews.append(_flatten_review(r))

        time.sleep(0.5)

    _save("reviews", all_reviews)
    return all_reviews


def fetch_users(review_records: list) -> list:
    """
    Extract unique user data embedded in review records.
    AniList embeds user info in reviews, so no extra API calls needed.
    Returns a list of unique user dicts and saves raw JSON.
    """
    if _already_fetched("users"):
        return _load_raw("users")

    logger.info("Extracting unique users from %d reviews...", len(review_records))
    seen = {}
    for r in review_records:
        uid = r.get("user_id")
        if uid and uid not in seen:
            seen[uid] = {
                "user_id": uid,
                "username": r.get("username"),
                "join_date": r.get("user_created_at"),
            }

    users = list(seen.values())
    _save("users", users)
    return users


# ---------------------------------------------------------------------------
# Flatten helpers (raw API shape → flat dict)
# ---------------------------------------------------------------------------

def _flatten_anime(item: dict) -> dict:
    studios = item.get("studios", {}).get("nodes", [])
    studio_name = studios[0]["name"] if studios else None
    return {
        "anime_id": item["id"],
        "title_romaji": item["title"]["romaji"],
        "title_english": item["title"].get("english"),
        "genres": item.get("genres", []),
        "studio": studio_name,
        "episodes": item.get("episodes"),
        "avg_score": item.get("averageScore"),
        "year": (item.get("startDate") or {}).get("year"),
        "status": item.get("status"),
        "description": item.get("description"),
    }


def _flatten_review(r: dict) -> dict:
    user = r.get("user") or {}
    return {
        "review_id": r["id"],
        "anime_id": r["mediaId"],
        "user_id": user.get("id"),
        "username": user.get("name"),
        "user_created_at": user.get("createdAt"),
        "score": r.get("score"),
        "summary": r.get("summary"),
        "review_text": r.get("body"),
        "created_at": r.get("createdAt"),
    }


# ---------------------------------------------------------------------------
# Load helper (for idempotency re-use)
# ---------------------------------------------------------------------------

def _load_raw(entity: str) -> list:
    path = _output_path(entity)
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    anime = fetch_anime(top_n=DEFAULT_TOP_N)
    reviews = fetch_reviews(anime, max_reviews_per_anime=5)
    users = fetch_users(reviews)
    logger.info("Extraction complete. Anime: %d, Reviews: %d, Users: %d", len(anime), len(reviews), len(users))
