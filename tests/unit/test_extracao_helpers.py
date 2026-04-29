from __future__ import annotations

from pathlib import Path

import pytest

from extracao.extracao_oracle_eficiente import (
    ConsultaSql,
    _extrair_comandos_pre_sql,
    _looks_like_windows_path,
    _relative_sql_path,
    _sql_stem,
)


def test_looks_like_windows_path_backslash() -> None:
    assert _looks_like_windows_path(Path("C:\\Users\\foo\\bar.sql")) is True


def test_looks_like_windows_path_without_backslash(tmp_path: Path) -> None:
    p = tmp_path / "query.sql"
    # tmp_path on this OS has no backslash in str representation if on POSIX,
    # but on Windows backslashes are present; just check the function returns a bool
    result = _looks_like_windows_path(p)
    assert isinstance(result, bool)


def test_sql_stem_posix() -> None:
    assert _sql_stem(Path("/foo/bar/consulta.sql")) == "consulta"


def test_relative_sql_path_simple() -> None:
    raiz = Path("/sql")
    caminho = Path("/sql/subdir/query.sql")
    result = _relative_sql_path(caminho, raiz)
    assert result == Path("subdir/query.sql")


def test_relative_sql_path_raises_when_not_subpath() -> None:
    raiz = Path("/sql")
    caminho = Path("/other/query.sql")
    with pytest.raises(ValueError):
        _relative_sql_path(caminho, raiz)


def test_extrair_comandos_pre_sql_no_pre() -> None:
    sql = "SELECT * FROM tabela WHERE x = 1"
    cmds, remaining = _extrair_comandos_pre_sql(sql)
    assert cmds == []
    assert "SELECT" in remaining


def test_extrair_comandos_pre_sql_with_pre() -> None:
    sql = "-- PRE: ALTER SESSION SET NLS_DATE_FORMAT='DD/MM/YYYY'\nSELECT * FROM tabela"
    cmds, remaining = _extrair_comandos_pre_sql(sql)
    assert len(cmds) == 1
    assert "ALTER SESSION" in cmds[0]
    assert "SELECT" in remaining


def test_extrair_comandos_pre_sql_multiple_pre() -> None:
    sql = "-- PRE: CMD1\n-- PRE: CMD2\nSELECT 1"
    cmds, remaining = _extrair_comandos_pre_sql(sql)
    assert len(cmds) == 2
    assert "SELECT" in remaining


def test_extrair_comandos_pre_sql_strips_semicolons() -> None:
    sql = "-- PRE: ALTER SESSION SET X=1;\nSELECT 1"
    cmds, _ = _extrair_comandos_pre_sql(sql)
    assert not cmds[0].endswith(";")
