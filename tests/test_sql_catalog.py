from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest

from utilitarios.sql_catalog import normalize_sql_id, SqlCatalogEntry


@pytest.fixture
def mock_catalog():
    by_id = {
        "folder1/query1.sql": SqlCatalogEntry(
            sql_id="folder1/query1.sql", path=Path("folder1/query1.sql")
        ),
        "folder2/query2.sql": SqlCatalogEntry(
            sql_id="folder2/query2.sql", path=Path("folder2/query2.sql")
        ),
        "folder2/duplicate_name.sql": SqlCatalogEntry(
            sql_id="folder2/duplicate_name.sql", path=Path("folder2/duplicate_name.sql")
        ),
        "folder3/duplicate_name.sql": SqlCatalogEntry(
            sql_id="folder3/duplicate_name.sql", path=Path("folder3/duplicate_name.sql")
        ),
    }
    by_name = {
        "query1.sql": [by_id["folder1/query1.sql"]],
        "query2.sql": [by_id["folder2/query2.sql"]],
        "duplicate_name.sql": [
            by_id["folder2/duplicate_name.sql"],
            by_id["folder3/duplicate_name.sql"],
        ],
    }

    with patch("utilitarios.sql_catalog._index_entries", return_value=(by_id, by_name)):
        with patch(
            "utilitarios.sql_catalog.get_sql_id", return_value=None
        ) as mock_get_sql_id:
            yield mock_get_sql_id


def test_normalize_sql_id_none_or_empty(mock_catalog):
    assert normalize_sql_id(None) is None
    assert normalize_sql_id("") is None
    assert normalize_sql_id("   ") is None


def test_normalize_sql_id_direct_match(mock_catalog):
    assert normalize_sql_id("folder1/query1.sql") == "folder1/query1.sql"
    assert normalize_sql_id("./folder2/query2.sql") == "folder2/query2.sql"
    assert normalize_sql_id("FOLDER1/QUERY1.SQL") == "folder1/query1.sql"


def test_normalize_sql_id_get_sql_id_fallback(mock_catalog):
    mock_catalog.return_value = "folder1/query1.sql"
    assert (
        normalize_sql_id("/absolute/path/to/folder1/query1.sql") == "folder1/query1.sql"
    )
    mock_catalog.assert_called_once()


def test_normalize_sql_id_marker_fallback(mock_catalog):
    assert (
        normalize_sql_id("C:\\some\\path\\sql\\folder1\\query1.sql")
        == "folder1/query1.sql"
    )
    assert (
        normalize_sql_id("C:/some/path/consultas_fonte/folder2/query2.sql")
        == "folder2/query2.sql"
    )


def test_normalize_sql_id_marker_fallback_no_match(mock_catalog):
    assert normalize_sql_id("C:\\some\\path\\sql\\unknown_folder\\query1.sql") is None


def test_normalize_sql_id_candidate_name_match(mock_catalog):
    assert normalize_sql_id("some_random_path/query1.sql") == "folder1/query1.sql"
    assert normalize_sql_id("query2.sql") == "folder2/query2.sql"


def test_normalize_sql_id_candidate_name_multiple_matches(mock_catalog):
    assert normalize_sql_id("duplicate_name.sql") is None


def test_normalize_sql_id_candidate_suffix_match():
    by_id = {
        "folder/sub1/query.sql": SqlCatalogEntry(
            sql_id="folder/sub1/query.sql", path=Path("folder/sub1/query.sql")
        ),
        "folder/sub2/query.sql": SqlCatalogEntry(
            sql_id="folder/sub2/query.sql", path=Path("folder/sub2/query.sql")
        ),
    }
    by_name = {
        "query.sql": [by_id["folder/sub1/query.sql"], by_id["folder/sub2/query.sql"]],
    }
    with patch("utilitarios.sql_catalog._index_entries", return_value=(by_id, by_name)):
        with patch("utilitarios.sql_catalog.get_sql_id", return_value=None):
            assert normalize_sql_id("sub1/query.sql") == "folder/sub1/query.sql"


def test_normalize_sql_id_candidate_suffix_multiple_matches():
    by_id = {
        "folder1/sub/query.sql": SqlCatalogEntry(
            sql_id="folder1/sub/query.sql", path=Path("folder1/sub/query.sql")
        ),
        "folder2/sub/query.sql": SqlCatalogEntry(
            sql_id="folder2/sub/query.sql", path=Path("folder2/sub/query.sql")
        ),
    }
    by_name = {
        "query.sql": [by_id["folder1/sub/query.sql"], by_id["folder2/sub/query.sql"]],
    }
    with patch("utilitarios.sql_catalog._index_entries", return_value=(by_id, by_name)):
        with patch("utilitarios.sql_catalog.get_sql_id", return_value=None):
            assert normalize_sql_id("sub/query.sql") is None


def test_normalize_sql_id_no_match(mock_catalog):
    assert normalize_sql_id("completely_unknown.sql") is None
