import json
from datetime import datetime
from pathlib import Path

import pytest
from pytest_mock import MockerFixture

from src.utilitarios.perf_monitor import (
    _root_dir,
    _serializar_valor,
    caminho_log_performance,
    registrar_evento_performance,
)


def test_root_dir():
    """Test that _root_dir returns a valid Path object."""
    root = _root_dir()
    assert isinstance(root, Path)
    assert root.name == "fiscal-parquet-analyzer" or root.is_dir()


def test_caminho_log_performance(mocker: MockerFixture, tmp_path: Path):
    """Test that caminho_log_performance returns the correct path and creates parent dirs."""
    mocker.patch("src.utilitarios.perf_monitor._root_dir", return_value=tmp_path)
    path = caminho_log_performance()

    expected_path = tmp_path / "logs" / "performance" / "perf_events.jsonl"
    assert path == expected_path
    assert expected_path.parent.exists()
    assert expected_path.parent.is_dir()


def test_serializar_valor():
    """Test the recursive serialization of different data types."""
    # Basic types
    assert _serializar_valor("string") == "string"
    assert _serializar_valor(123) == 123
    assert _serializar_valor(45.6) == 45.6
    assert _serializar_valor(True) is True
    assert _serializar_valor(None) is None

    # Path
    path_obj = Path("/tmp/test")
    assert _serializar_valor(path_obj) == str(path_obj)

    # Collections
    assert _serializar_valor([1, "two", Path("/three")]) == [1, "two", str(Path("/three"))]
    assert _serializar_valor((1, 2)) == [1, 2]
    assert _serializar_valor({1, 2}) in ([1, 2], [2, 1])

    # Dictionaries
    nested_dict = {
        "key1": "value",
        2: Path("/test"),
        "key3": [1, 2, {3: 4}]
    }
    expected_dict = {
        "key1": "value",
        "2": str(Path("/test")),
        "key3": [1, 2, {"3": 4}]
    }
    assert _serializar_valor(nested_dict) == expected_dict

    # Custom object (fallback to str)
    class CustomObj:
        def __str__(self):
            return "custom"

    assert _serializar_valor(CustomObj()) == "custom"


def test_registrar_evento_performance_basic(mocker: MockerFixture, tmp_path: Path):
    """Test writing a basic performance event to file."""
    log_file = tmp_path / "perf.jsonl"
    mocker.patch("src.utilitarios.perf_monitor.caminho_log_performance", return_value=log_file)

    # Fix datetime to check the timestamp
    mock_datetime = mocker.patch("src.utilitarios.perf_monitor.datetime")
    mock_datetime.now.return_value.isoformat.return_value = "2023-10-27T10:00:00"

    registrar_evento_performance("test_event")

    assert log_file.exists()
    content = log_file.read_text(encoding="utf-8").strip()
    data = json.loads(content)

    assert data["timestamp"] == "2023-10-27T10:00:00"
    assert data["evento"] == "test_event"
    assert data["status"] == "ok"
    assert "duracao_s" not in data
    assert "contexto" not in data


def test_registrar_evento_performance_full(mocker: MockerFixture, tmp_path: Path):
    """Test writing a performance event with all optional fields."""
    log_file = tmp_path / "perf.jsonl"
    mocker.patch("src.utilitarios.perf_monitor.caminho_log_performance", return_value=log_file)

    context = {"user_id": 123, "path": Path("/test")}
    registrar_evento_performance(
        evento="complex_event",
        duracao_s=1.2345678,
        contexto=context,
        status="error"
    )

    assert log_file.exists()
    content = log_file.read_text(encoding="utf-8").strip()
    data = json.loads(content)

    assert data["evento"] == "complex_event"
    assert data["status"] == "error"
    assert data["duracao_s"] == 1.234568  # rounded to 6 decimal places
    assert data["contexto"] == {"user_id": 123, "path": str(Path("/test"))}


def test_registrar_evento_performance_exception_handling(mocker: MockerFixture):
    """Test that exceptions during logging are caught and ignored."""
    # Mock caminho_log_performance to raise an exception
    mocker.patch("src.utilitarios.perf_monitor.caminho_log_performance", side_effect=PermissionError("Access denied"))

    # This should not raise an exception
    try:
        registrar_evento_performance("test_event")
    except Exception as e:
        pytest.fail(f"Exception was not caught: {e}")
