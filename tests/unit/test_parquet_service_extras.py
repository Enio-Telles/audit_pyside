from __future__ import annotations

import shutil
from pathlib import Path

import polars as pl
import pytest

from interface_grafica.services.parquet_service import FilterCondition, ParquetService


# ---------------------------------------------------------------------------
# _conditions_key — return () when conditions is None (line 86)
# ---------------------------------------------------------------------------


def test_conditions_key_none() -> None:
    assert ParquetService._conditions_key(None) == ()


# ---------------------------------------------------------------------------
# list_cnpjs — root not exist (lines 92, 93) and with dirs (lines 94, 99)
# ---------------------------------------------------------------------------


def test_list_cnpjs_root_not_exist(tmp_path: Path) -> None:
    subdir = tmp_path / "data"
    svc = ParquetService(root=subdir)
    shutil.rmtree(subdir)
    assert svc.list_cnpjs() == []


def test_list_cnpjs_with_cnpj_dir(tmp_path: Path) -> None:
    svc = ParquetService(root=tmp_path)
    (tmp_path / "12345678000190").mkdir()
    (tmp_path / "nao_e_cnpj").mkdir()
    result = svc.list_cnpjs()
    assert "12345678000190" in result
    assert "nao_e_cnpj" not in result


# ---------------------------------------------------------------------------
# list_parquet_files — nonexistent cnpj (line 107)
# ---------------------------------------------------------------------------


def test_list_parquet_files_cnpj_not_exist(tmp_path: Path) -> None:
    svc = ParquetService(root=tmp_path)
    assert svc.list_parquet_files("99999999999999") == []


# ---------------------------------------------------------------------------
# _normalize_operator — lines 181, 183, 187, 203
# ---------------------------------------------------------------------------


def test_normalize_operator_comeca_com() -> None:
    assert ParquetService._normalize_operator("começa com") == "comeca_com"


def test_normalize_operator_termina_com() -> None:
    assert ParquetService._normalize_operator("termina algo") == "termina_com"


def test_normalize_operator_e_nulo() -> None:
    assert ParquetService._normalize_operator("e_nulo") == "e_nulo"


def test_normalize_operator_unknown() -> None:
    assert ParquetService._normalize_operator("xpto_desconhecido") == "xpto_desconhecido"


# ---------------------------------------------------------------------------
# _is_list_dtype — lines 208, 211, 212
# ---------------------------------------------------------------------------


def test_is_list_dtype_none() -> None:
    assert ParquetService._is_list_dtype(None) is False


def test_is_list_dtype_list_type() -> None:
    assert ParquetService._is_list_dtype(pl.List(pl.Utf8)) is True


class _FakeDtype:
    def base_type(self):
        raise RuntimeError("boom")

    def __str__(self) -> str:
        return "List[str]"


def test_is_list_dtype_exception_fallback() -> None:
    assert ParquetService._is_list_dtype(_FakeDtype()) is True


# ---------------------------------------------------------------------------
# _build_expr — lines 233, 235, 237, 241–245, 247, 251, 257, 259
# ---------------------------------------------------------------------------


@pytest.fixture
def svc(tmp_path: Path) -> ParquetService:
    return ParquetService(root=tmp_path)


def test_build_expr_comeca_com(svc: ParquetService) -> None:
    cond = FilterCondition("a", "começa com", "te")
    lf = pl.DataFrame({"a": ["test", "other"]}).lazy()
    result = lf.filter(svc._build_expr(cond)).collect()
    assert result.height == 1


def test_build_expr_termina_com(svc: ParquetService) -> None:
    cond = FilterCondition("a", "termina com", "st")
    lf = pl.DataFrame({"a": ["test", "other"]}).lazy()
    result = lf.filter(svc._build_expr(cond)).collect()
    assert result.height == 1


def test_build_expr_e_nulo(svc: ParquetService) -> None:
    cond = FilterCondition("a", "e nulo", "")
    lf = pl.DataFrame({"a": ["x", None, ""]}).lazy()
    result = lf.filter(svc._build_expr(cond)).collect()
    assert result.height == 2


def test_build_expr_numeric_valid(svc: ParquetService) -> None:
    cond = FilterCondition("val", "maior", "5")
    lf = pl.DataFrame({"val": [10.0, 3.0]}).lazy()
    result = lf.filter(svc._build_expr(cond)).collect()
    assert result.height == 1


def test_build_expr_numeric_invalid_value(svc: ParquetService) -> None:
    cond = FilterCondition("val", "maior", "abc")
    lf = pl.DataFrame({"val": ["10", "3"]}).lazy()
    result = lf.filter(svc._build_expr(cond)).collect()
    assert result.height == 0


# ---------------------------------------------------------------------------
# apply_filters — lines 269–273, 277
# ---------------------------------------------------------------------------


def test_apply_filters_available_columns_none(svc: ParquetService) -> None:
    lf = pl.DataFrame({"a": ["x", "y"]}).lazy()
    cond = FilterCondition("a", "igual", "x")
    result = svc.apply_filters(lf, [cond], available_columns=None).collect()
    assert result.height == 1


def test_apply_filters_empty_column(svc: ParquetService) -> None:
    lf = pl.DataFrame({"a": ["x", "y"]}).lazy()
    cond = FilterCondition("", "igual", "x")
    result = svc.apply_filters(lf, [cond]).collect()
    assert result.height == 2


# ---------------------------------------------------------------------------
# load_dataset — cache hit (lines 441, 442, 454)
# ---------------------------------------------------------------------------


def test_load_dataset_cache_hit(tmp_path: Path) -> None:
    svc = ParquetService(root=tmp_path)
    df = pl.DataFrame({"a": [1, 2, 3]})
    p = tmp_path / "data.parquet"
    df.write_parquet(str(p))
    r1 = svc.load_dataset(p)
    r2 = svc.load_dataset(p)
    assert r1.shape == r2.shape == (3, 1)
    # Both calls should have resolved to a single cache entry.
    assert len(svc._dataset_cache) == 1


# ---------------------------------------------------------------------------
# load_dataset — dataset cache eviction (line 462)
# ---------------------------------------------------------------------------


def test_load_dataset_cache_eviction(tmp_path: Path) -> None:
    svc = ParquetService(root=tmp_path)
    for i in range(7):
        df = pl.DataFrame({"a": [i]})
        p = tmp_path / f"data_{i}.parquet"
        df.write_parquet(str(p))
        svc.load_dataset(p)
    assert len(svc._dataset_cache) <= svc._dataset_cache_limit
