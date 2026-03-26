"""
tests/test_extract.py

Unit tests for extract/fetch_anime.py.
All network calls are mocked — no real API hits.
"""

import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from extract.fetch_anime import (
    _flatten_anime,
    _flatten_review,
    _post_with_retry,
    _today_str,
    fetch_anime,
    fetch_reviews,
    fetch_users,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

FAKE_ANIME_RESPONSE = {
    "data": {
        "Page": {
            "pageInfo": {"total": 1, "currentPage": 1, "lastPage": 1, "hasNextPage": False},
            "media": [
                {
                    "id": 1,
                    "title": {"romaji": "Fullmetal Alchemist", "english": "Fullmetal Alchemist"},
                    "genres": ["Action", "Adventure"],
                    "studios": {"nodes": [{"name": "Bones"}]},
                    "episodes": 51,
                    "averageScore": 83,
                    "startDate": {"year": 2003},
                    "status": "FINISHED",
                    "description": "Two brothers search for a philosopher's stone.",
                }
            ],
        }
    }
}

FAKE_REVIEWS_RESPONSE = {
    "data": {
        "Page": {
            "pageInfo": {"hasNextPage": False},
            "reviews": [
                {
                    "id": 101,
                    "mediaId": 1,
                    "user": {"id": 42, "name": "testuser", "createdAt": 1609459200},
                    "score": 9,
                    "summary": "Great show",
                    "body": "Really enjoyed this anime from start to finish.",
                    "createdAt": 1620000000,
                }
            ],
        }
    }
}


# ---------------------------------------------------------------------------
# _flatten_anime
# ---------------------------------------------------------------------------

def test_flatten_anime_full():
    raw = FAKE_ANIME_RESPONSE["data"]["Page"]["media"][0]
    result = _flatten_anime(raw)

    assert result["anime_id"] == 1
    assert result["title_romaji"] == "Fullmetal Alchemist"
    assert result["title_english"] == "Fullmetal Alchemist"
    assert result["genres"] == ["Action", "Adventure"]
    assert result["studio"] == "Bones"
    assert result["episodes"] == 51
    assert result["avg_score"] == 83
    assert result["year"] == 2003
    assert result["status"] == "FINISHED"


def test_flatten_anime_missing_studio():
    raw = FAKE_ANIME_RESPONSE["data"]["Page"]["media"][0].copy()
    raw["studios"] = {"nodes": []}
    result = _flatten_anime(raw)
    assert result["studio"] is None


def test_flatten_anime_missing_start_date():
    raw = FAKE_ANIME_RESPONSE["data"]["Page"]["media"][0].copy()
    raw["startDate"] = None
    result = _flatten_anime(raw)
    assert result["year"] is None


# ---------------------------------------------------------------------------
# _flatten_review
# ---------------------------------------------------------------------------

def test_flatten_review_full():
    raw = FAKE_REVIEWS_RESPONSE["data"]["Page"]["reviews"][0]
    result = _flatten_review(raw)

    assert result["review_id"] == 101
    assert result["anime_id"] == 1
    assert result["user_id"] == 42
    assert result["username"] == "testuser"
    assert result["score"] == 9
    assert result["review_text"] == "Really enjoyed this anime from start to finish."
    assert result["created_at"] == 1620000000


def test_flatten_review_no_user():
    raw = FAKE_REVIEWS_RESPONSE["data"]["Page"]["reviews"][0].copy()
    raw["user"] = None
    result = _flatten_review(raw)
    assert result["user_id"] is None
    assert result["username"] is None


# ---------------------------------------------------------------------------
# _post_with_retry
# ---------------------------------------------------------------------------

def test_post_with_retry_success():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": {"test": True}}
    mock_response.raise_for_status = MagicMock()

    with patch("requests.post", return_value=mock_response):
        result = _post_with_retry("https://example.com", {"query": "test"})

    assert result == {"data": {"test": True}}


def test_post_with_retry_retries_on_500():
    fail_response = MagicMock()
    fail_response.status_code = 500

    ok_response = MagicMock()
    ok_response.status_code = 200
    ok_response.json.return_value = {"data": {"ok": True}}
    ok_response.raise_for_status = MagicMock()

    with patch("requests.post", side_effect=[fail_response, ok_response]):
        with patch("time.sleep"):
            result = _post_with_retry("https://example.com", {}, base_delay=0.01)

    assert result["data"]["ok"] is True


def test_post_with_retry_raises_after_max_retries():
    fail_response = MagicMock()
    fail_response.status_code = 500

    with patch("requests.post", return_value=fail_response):
        with patch("time.sleep"):
            with pytest.raises(RuntimeError, match="retries exhausted"):
                _post_with_retry("https://example.com", {}, max_retries=3, base_delay=0.01)


def test_post_with_retry_raises_on_graphql_error():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"errors": [{"message": "Not found"}]}
    mock_response.raise_for_status = MagicMock()

    with patch("requests.post", return_value=mock_response):
        with pytest.raises(ValueError, match="GraphQL errors"):
            _post_with_retry("https://example.com", {})


# ---------------------------------------------------------------------------
# fetch_anime (integration-style with mocked HTTP + tmp dir)
# ---------------------------------------------------------------------------

def test_fetch_anime_saves_json(tmp_path):
    with patch("extract.fetch_anime.RAW_DATA_DIR", tmp_path):
        with patch("extract.fetch_anime._post_with_retry", return_value=FAKE_ANIME_RESPONSE):
            with patch("time.sleep"):
                records = fetch_anime(top_n=1)

    assert len(records) == 1
    assert records[0]["anime_id"] == 1

    saved_file = tmp_path / "anime" / f"{_today_str()}.json"
    assert saved_file.exists()
    with open(saved_file) as f:
        data = json.load(f)
    assert data[0]["title_romaji"] == "Fullmetal Alchemist"


def test_fetch_anime_idempotent(tmp_path):
    # Pre-create today's file
    folder = tmp_path / "anime"
    folder.mkdir()
    existing = [{"anime_id": 99, "title_romaji": "Cached"}]
    (folder / f"{_today_str()}.json").write_text(json.dumps(existing))

    with patch("extract.fetch_anime.RAW_DATA_DIR", tmp_path):
        with patch("extract.fetch_anime._post_with_retry") as mock_post:
            records = fetch_anime(top_n=50)

    mock_post.assert_not_called()
    assert records[0]["anime_id"] == 99


# ---------------------------------------------------------------------------
# fetch_users
# ---------------------------------------------------------------------------

def test_fetch_users_deduplicates(tmp_path):
    reviews = [
        {"user_id": 1, "username": "alice", "user_created_at": 100},
        {"user_id": 1, "username": "alice", "user_created_at": 100},
        {"user_id": 2, "username": "bob", "user_created_at": 200},
    ]
    with patch("extract.fetch_anime.RAW_DATA_DIR", tmp_path):
        users = fetch_users(reviews)

    assert len(users) == 2
    user_ids = {u["user_id"] for u in users}
    assert user_ids == {1, 2}
