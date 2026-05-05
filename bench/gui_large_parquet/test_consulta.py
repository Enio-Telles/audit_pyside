"""
Benchmarks para a consulta de parquets da GUI (TTFP, page_change, filter).
"""

import pytest
from pathlib import Path
from interface_grafica.services.parquet_query_service import ParquetQueryService
from interface_grafica.services.parquet_service import FilterCondition

pytestmark = pytest.mark.bench

@pytest.fixture
def query_service() -> ParquetQueryService:
    # Service com threshold padrao (512MB).
    # Arquivos de 64MB vao rodar em Polars.
    return ParquetQueryService()

@pytest.mark.benchmark(group="gui_consulta")
def test_consulta_open_ttfp(benchmark, query_service: ParquetQueryService, bench_large_parquet_64mb: Path) -> None:
    """Benchmark: TTFP (Time to First Page) simulando a abertura de uma aba."""
    path = bench_large_parquet_64mb

    def run():
        query_service.get_schema(path)
        query_service.get_count(path)
        query_service.get_page(path, filters=None, visible_columns=None, page=1, page_size=200)

    benchmark(run)

@pytest.mark.benchmark(group="gui_consulta")
def test_consulta_page_change(benchmark, query_service: ParquetQueryService, bench_large_parquet_64mb: Path) -> None:
    """Benchmark: Mudanca de pagina para uma pagina arbitraria (e.g. 10)."""
    path = bench_large_parquet_64mb

    def run():
        query_service.get_page(path, filters=None, visible_columns=None, page=10, page_size=200)

    benchmark(run)

@pytest.mark.benchmark(group="gui_consulta")
def test_consulta_filter(benchmark, query_service: ParquetQueryService, bench_large_parquet_64mb: Path) -> None:
    """Benchmark: Filtro 'contem' em coluna de texto."""
    path = bench_large_parquet_64mb
    
    # Certificar-se que a coluna existe e foi gerada. O mock usa 'descricao', assim como o real.
    f = [FilterCondition(column="descricao", operator="contem", value="PROD")]

    def run():
        query_service.get_count(path, f)
        query_service.get_page(path, filters=f, visible_columns=None, page=1, page_size=200)

    benchmark(run)
