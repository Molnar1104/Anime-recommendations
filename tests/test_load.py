"""
tests/test_load.py

Unit tests for load/load_to_postgres.py.
All DB calls are mocked — no real PostgreSQL connection needed.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from load.load_to_postgres import (
    _latest_raw_file,
    load_anime,
    load_reviews,
    load_users,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

ANIME_RECORDS = [
    {
        "anime_id": 1,
        "title_romaji": "Fullmetal Alchemist",
        "title_english": "Fullmetal Alchemist",
        "genres": ["Action", "Adventure"],
        "studio": "Bones",
        "episodes": 51,
        "avg_score": 83,
        "year": 2003,
        "status": "FINISHED",
        "description": "Two brothers.",
    }
]

REVIEW_RECORDS = [
    {
        "review_id": 101,
        "anime_id": 1,
        "user_id": 42,
        "username": "testuser",
        "user_created_at": 1609459200,
        "score": 9,
        "summary": "Great show",
        "review_text": "Loved it.",
        "created_at": 1620000000,
    }
]

USER_RECORDS = [
    {"user_id": 42, "username": "testuser", "join_date": 1609459200},
    {"user_id": 43, "username": "anotheruser", "join_date": 1620000000},
]


# ---------------------------------------------------------------------------
# _latest_raw_file
# ---------------------------------------------------------------------------

def test_latest_raw_file_returns_most_recent(tmp_path):
    entity_dir = tmp_path / "anime"
    entity_dir.mkdir()
    (entity_dir / "2026-03-24.json").write_text("[]")
    (entity_dir / "2026-03-26.json").write_text("[]")
    (entity_dir / "2026-03-25.json").write_text("[]")

    with patch("load.load_to_postgres.RAW_DATA_DIR", tmp_path):
        result = _latest_raw_file("anime")

    assert result.name == "2026-03-26.json"


def test_latest_raw_file_raises_when_empty(tmp_path):
    (tmp_path / "anime").mkdir()
    with patch("load.load_to_postgres.RAW_DATA_DIR", tmp_path):
        with pytest.raises(FileNotFoundError, match="No raw JSON files"):
            _latest_raw_file("anime")


# ---------------------------------------------------------------------------
# load_anime
# ---------------------------------------------------------------------------

def test_load_anime_truncates_and_inserts():
    cur = MagicMock()
    with patch("load.load_to_postgres.execute_values") as mock_ev:
        count = load_anime(cur, ANIME_RECORDS)

    cur.execute.assert_called_once_with("TRUNCATE TABLE raw.anime RESTART IDENTITY CASCADE;")
    assert mock_ev.called
    assert count == 1

    inserted_rows = mock_ev.call_args[0][2]
    assert inserted_rows[0][0] == 1           # anime_id
    assert inserted_rows[0][1] == "Fullmetal Alchemist"  # title_romaji
    assert inserted_rows[0][4] == "Bones"     # studio


def test_load_anime_empty_genres_defaults_to_list():
    record = ANIME_RECORDS[0].copy()
    record["genres"] = None
    cur = MagicMock()
    with patch("load.load_to_postgres.execute_values") as mock_ev:
        load_anime(cur, [record])
    inserted_rows = mock_ev.call_args[0][2]
    assert inserted_rows[0][3] == []  # genres defaults to []


# ---------------------------------------------------------------------------
# load_reviews
# ---------------------------------------------------------------------------

def test_load_reviews_truncates_and_inserts():
    cur = MagicMock()
    with patch("load.load_to_postgres.execute_values") as mock_ev:
        count = load_reviews(cur, REVIEW_RECORDS)

    cur.execute.assert_called_once_with("TRUNCATE TABLE raw.reviews RESTART IDENTITY CASCADE;")
    assert count == 1

    inserted_rows = mock_ev.call_args[0][2]
    assert inserted_rows[0][0] == 101   # review_id
    assert inserted_rows[0][5] == 9     # score


# ---------------------------------------------------------------------------
# load_users
# ---------------------------------------------------------------------------

def test_load_users_truncates_and_inserts():
    cur = MagicMock()
    with patch("load.load_to_postgres.execute_values") as mock_ev:
        count = load_users(cur, USER_RECORDS)

    cur.execute.assert_called_once_with("TRUNCATE TABLE raw.users RESTART IDENTITY CASCADE;")
    assert count == 2


def test_load_users_skips_null_user_ids():
    records = USER_RECORDS + [{"user_id": None, "username": "ghost", "join_date": None}]
    cur = MagicMock()
    with patch("load.load_to_postgres.execute_values") as mock_ev:
        count = load_users(cur, records)
    assert count == 2  # null user_id filtered out
