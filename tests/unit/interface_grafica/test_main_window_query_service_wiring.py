"""
Testes de wiring: verifica que MainWindow instancia ParquetQueryService corretamente
e que o query_service reutiliza o parquet_service (backend Polars compartilhado).

Nao instancia a MainWindow real (evita dependencia de PySide6/display).
Testa somente as propriedades de composicao que podem ser verificadas sem GUI.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from interface_grafica.services.parquet_query_service import ParquetQueryService
from interface_grafica.services.parquet_service import ParquetService


def test_parquet_query_service_aceita_polars_service_injetado() -> None:
    """query_service deve reutilizar o parquet_service existente como backend Polars."""
    polars_svc = ParquetService()
    query_svc = ParquetQueryService(polars_service=polars_svc)
    assert query_svc._polars is polars_svc


def test_parquet_query_service_threshold_default() -> None:
    """Threshold default deve ser 512 MB."""
    svc = ParquetQueryService()
    assert svc._threshold_bytes == 512 * 1024 * 1024


def test_parquet_query_service_threshold_customizavel() -> None:
    svc = ParquetQueryService(threshold_mb=256)
    assert svc._threshold_bytes == 256 * 1024 * 1024


def test_query_service_usa_polars_service_injetado_para_get_schema(tmp_path) -> None:
    """get_schema para arquivo pequeno delega ao parquet_service injetado."""
    import polars as pl
    path = tmp_path / "test.parquet"
    pl.DataFrame({"a": [1, 2], "b": ["x", "y"]}).write_parquet(path)

    mock_polars = MagicMock(spec=ParquetService)
    mock_polars.get_schema.return_value = ["a", "b"]
    svc = ParquetQueryService(polars_service=mock_polars, threshold_mb=999_999)

    result = svc.get_schema(path)

    mock_polars.get_schema.assert_called_once_with(path)
    assert result == ["a", "b"]


def test_query_service_usa_duckdb_para_arquivo_grande(tmp_path) -> None:
    """get_schema para arquivo > threshold delega ao duckdb_service."""
    import polars as pl
    path = tmp_path / "grande.parquet"
    pl.DataFrame({"x": [1]}).write_parquet(path)

    mock_duckdb = MagicMock()
    mock_duckdb.get_schema.return_value = ["x"]
    svc = ParquetQueryService(duckdb_service=mock_duckdb, threshold_mb=0)

    result = svc.get_schema(path)

    mock_duckdb.get_schema.assert_called_once_with(path)
    assert result == ["x"]


def test_query_service_get_page_kwargs_corretos(tmp_path) -> None:
    """get_page repassa path e filters com os nomes corretos."""
    import polars as pl
    from interface_grafica.services.parquet_service import FilterCondition, PageResult

    path = tmp_path / "arq.parquet"
    pl.DataFrame({"id": [1, 2, 3]}).write_parquet(path)

    page_result = PageResult(
        total_rows=3,
        df_all_columns=pl.DataFrame({"id": [1]}),
        df_visible=pl.DataFrame({"id": [1]}),
        columns=["id"],
        visible_columns=["id"],
    )
    mock_polars = MagicMock(spec=ParquetService)
    mock_polars.get_page.return_value = page_result
    svc = ParquetQueryService(polars_service=mock_polars, threshold_mb=999_999)

    filtros = [FilterCondition(column="id", operator="igual", value="1")]
    result = svc.get_page(
        path=path,
        filters=filtros,
        visible_columns=["id"],
        page=1,
        page_size=10,
    )

    mock_polars.get_page.assert_called_once_with(
        path, filtros, ["id"], 1, 10, None, False
    )
    assert result is page_result
