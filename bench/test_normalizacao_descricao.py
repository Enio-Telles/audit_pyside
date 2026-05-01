"""
Benchmarks para normalizacao de descricoes de produtos.

Compara o baseline map_elements+lambda com a implementacao real
de expr_normalizar_descricao (utilitarios.text).
"""
from __future__ import annotations

import re
import unicodedata

import polars as pl
import pytest

from utilitarios.text import expr_normalizar_descricao


pytestmark = pytest.mark.bench


def _normalize_baseline(s: str) -> str:
    """Baseline Python puro equivalente ao normalize_desc original."""
    if not s:
        return s
    t = unicodedata.normalize("NFD", s)
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    t = t.upper()
    t = re.sub(r"[^A-Z0-9\s\-\*/]", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _serie(descricoes: list[str]) -> pl.Series:
    return pl.Series("descricao", descricoes)


@pytest.mark.benchmark(group="normalizacao_100k")
def test_baseline_map_elements(benchmark: object, bench_descricoes_100k: list[str]) -> None:
    """Baseline: map_elements com lambda Python puro."""
    serie = _serie(bench_descricoes_100k)

    def run() -> pl.Series:
        return serie.map_elements(_normalize_baseline, return_dtype=pl.Utf8)

    benchmark(run)  # type: ignore[operator]


@pytest.mark.benchmark(group="normalizacao_100k")
def test_expr_normalizar_descricao(benchmark: object, bench_descricoes_100k: list[str]) -> None:
    """Implementacao real: expr_normalizar_descricao via Polars Expr."""
    df = pl.DataFrame({"descricao": bench_descricoes_100k})

    def run() -> pl.DataFrame:
        return df.with_columns(expr_normalizar_descricao("descricao").alias("norm"))

    benchmark(run)  # type: ignore[operator]
