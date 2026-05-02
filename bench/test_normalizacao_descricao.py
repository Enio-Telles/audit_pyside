"""
Benchmarks para normalizacao de descricoes de produtos.

Compara o baseline map_elements+lambda com a implementacao real
de expr_normalizar_descricao (utilitarios.text).
"""

import re

import polars as pl
import pytest

from utilitarios.text import expr_normalizar_descricao, normalize_desc


pytestmark = pytest.mark.bench
_TOKEN_DELIMITER_RE = re.compile(r"(?<=\w)[-/]|[-/](?=\w)")


def _normalize_baseline(s: str) -> str:
    """Baseline map_elements equivalente a expr_normalizar_descricao."""
    if not s:
        return s
    return normalize_desc(_TOKEN_DELIMITER_RE.sub(" ", s))


def _serie(descricoes: list[str]) -> pl.Series:
    return pl.Series("descricao", descricoes)


def test_normalize_baseline_matches_expr_delimiters() -> None:
    descricoes = ["CAFE-100/UN", "ITEM/A-B", "PROD - CX", "MED/10"]
    baseline = _serie(descricoes).map_elements(_normalize_baseline, return_dtype=pl.Utf8)
    expr = pl.DataFrame({"descricao": descricoes}).select(
        expr_normalizar_descricao("descricao").alias("norm")
    )["norm"]
    assert baseline.to_list() == expr.to_list()


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
