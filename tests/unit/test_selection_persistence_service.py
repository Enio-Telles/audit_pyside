from __future__ import annotations

import json
from pathlib import Path

import pytest

from interface_grafica.services.selection_persistence_service import (
    SelectionPersistenceService,
)


def test_init_file_not_exist(tmp_path: Path) -> None:
    svc = SelectionPersistenceService(file_path=tmp_path / "sel.json")
    assert svc._cache == {}


def test_load_valid_json(tmp_path: Path) -> None:
    f = tmp_path / "sel.json"
    f.write_text(json.dumps({"key": ["a", "b"]}), encoding="utf-8")
    svc = SelectionPersistenceService(file_path=f)
    assert svc._cache.get("key") == ["a", "b"]


def test_load_invalid_json(tmp_path: Path) -> None:
    f = tmp_path / "sel.json"
    f.write_text("not valid json {{", encoding="utf-8")
    svc = SelectionPersistenceService(file_path=f)
    assert svc._cache == {}


def test_set_and_get_selections(tmp_path: Path) -> None:
    svc = SelectionPersistenceService(file_path=tmp_path / "sel.json")
    svc.set_selections("tables", ["tab1", "tab2"])
    result = svc.get_selections("tables")
    assert result == ["tab1", "tab2"]


def test_get_selections_missing_key(tmp_path: Path) -> None:
    svc = SelectionPersistenceService(file_path=tmp_path / "sel.json")
    assert svc.get_selections("nonexistent") == []


def test_get_selections_non_list_value(tmp_path: Path) -> None:
    f = tmp_path / "sel.json"
    f.write_text(json.dumps({"tables": "not a list"}), encoding="utf-8")
    svc = SelectionPersistenceService(file_path=f)
    assert svc.get_selections("tables") == []


def test_set_selections_ultimas_consultas(tmp_path: Path) -> None:
    svc = SelectionPersistenceService(file_path=tmp_path / "sel.json")
    svc.set_selections("ultimas_consultas", ["algum_item"])
    result = svc.get_selections("ultimas_consultas")
    assert isinstance(result, list)


def test_set_value_and_get_value(tmp_path: Path) -> None:
    svc = SelectionPersistenceService(file_path=tmp_path / "sel.json")
    svc.set_value("my_key", 42)
    assert svc.get_value("my_key") == 42
    assert svc.get_value("missing", "default") == "default"


def test_save_exception_swallowed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    svc = SelectionPersistenceService(file_path=tmp_path / "sel.json")

    def _fail_write(self, *args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(Path, "write_text", _fail_write)
    svc._save()


def test_migrate_legacy_changes_value(tmp_path: Path) -> None:
    f = tmp_path / "sel.json"
    f.write_text(
        json.dumps({"ultimas_consultas": ["__id_inexistente_xyz_abc__"]}),
        encoding="utf-8",
    )
    svc = SelectionPersistenceService(file_path=f)
    result = svc.get_selections("ultimas_consultas")
    assert isinstance(result, list)


def test_migrate_legacy_no_change(tmp_path: Path) -> None:
    f = tmp_path / "sel.json"
    f.write_text(json.dumps({"ultimas_consultas": []}), encoding="utf-8")
    svc = SelectionPersistenceService(file_path=f)
    assert svc.get_selections("ultimas_consultas") == []
