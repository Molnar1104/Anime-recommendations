"""
tests/test_sentiment.py

Unit tests for ml/sentiment.py.
All HuggingFace and DB calls are mocked — no model download or DB needed.
"""

from unittest.mock import MagicMock, patch, call

import pytest

from ml.sentiment import (
    LABEL_MAP,
    classify_texts,
    fetch_reviews,
    update_sentiments,
)


# ---------------------------------------------------------------------------
# classify_texts
# ---------------------------------------------------------------------------

def test_classify_texts_maps_labels():
    """Verify label mapping from model output to readable names."""
    mock_pipe = MagicMock()
    mock_pipe.return_value = [
        {"label": "positive", "score": 0.95},
        {"label": "negative", "score": 0.87},
        {"label": "neutral", "score": 0.72},
    ]

    results = classify_texts(mock_pipe, ["great", "terrible", "okay"])

    assert results[0] == {"label": "positive", "score": 0.95}
    assert results[1] == {"label": "negative", "score": 0.87}
    assert results[2] == {"label": "neutral", "score": 0.72}


def test_classify_texts_maps_label_indices():
    """Some model versions output LABEL_0/1/2 instead of named labels."""
    mock_pipe = MagicMock()
    mock_pipe.return_value = [
        {"label": "LABEL_2", "score": 0.91},
        {"label": "LABEL_0", "score": 0.88},
    ]

    results = classify_texts(mock_pipe, ["love it", "hate it"])

    assert results[0]["label"] == "positive"
    assert results[1]["label"] == "negative"


def test_classify_texts_truncates_long_input():
    """Long texts should be truncated before reaching the model."""
    mock_pipe = MagicMock()
    mock_pipe.return_value = [{"label": "neutral", "score": 0.5}]

    long_text = "x" * 1000
    classify_texts(mock_pipe, [long_text])

    # The text passed to the pipeline should be truncated to MAX_TEXT_LENGTH (512)
    actual_text = mock_pipe.call_args[0][0][0]
    assert len(actual_text) == 512


def test_classify_texts_handles_empty_strings():
    """Empty or None review text should not crash."""
    mock_pipe = MagicMock()
    mock_pipe.return_value = [
        {"label": "neutral", "score": 0.5},
        {"label": "neutral", "score": 0.5},
    ]

    results = classify_texts(mock_pipe, [None, ""])
    assert len(results) == 2


# ---------------------------------------------------------------------------
# fetch_reviews
# ---------------------------------------------------------------------------

def test_fetch_reviews_returns_tuples():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchall.return_value = [
        (1, "Great anime"),
        (2, "Not so good"),
    ]

    results = fetch_reviews(mock_conn)
    assert len(results) == 2
    assert results[0] == (1, "Great anime")


def test_fetch_reviews_with_limit():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchall.return_value = [(1, "Review")]

    fetch_reviews(mock_conn, limit=10)

    executed_sql = mock_cursor.execute.call_args[0][0]
    assert "LIMIT 10" in executed_sql


# ---------------------------------------------------------------------------
# update_sentiments
# ---------------------------------------------------------------------------

def test_update_sentiments_creates_temp_table_and_updates():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_cursor.rowcount = 2

    data = [
        (1, "positive", 0.95),
        (2, "negative", 0.87),
    ]

    with patch("ml.sentiment.execute_values") as mock_ev:
        update_sentiments(mock_conn, data)

    # Should create temp table, insert via execute_values, then UPDATE FROM
    calls = mock_cursor.execute.call_args_list
    assert any("CREATE TEMP TABLE" in str(c) for c in calls)
    assert any("UPDATE" in str(c) for c in calls)
    assert mock_ev.called


# ---------------------------------------------------------------------------
# LABEL_MAP
# ---------------------------------------------------------------------------

def test_label_map_covers_both_formats():
    """Ensure both named and indexed label formats are mapped."""
    assert LABEL_MAP["positive"] == "positive"
    assert LABEL_MAP["LABEL_0"] == "negative"
    assert LABEL_MAP["LABEL_1"] == "neutral"
    assert LABEL_MAP["LABEL_2"] == "positive"
