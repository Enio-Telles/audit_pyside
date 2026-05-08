"""
ParquetQueryService — router automatico entre Polars e DuckDB.

Regras de roteamento:
- Diretorio particionado (path.is_dir()) -> sempre DuckDB
- Arquivo > threshold_mb -> DuckDB (lazy, sem materializar)
- Arquivo <= threshold_mb -> ParquetService (Polars, com LRU cache)

A interface e identica para os dois backends; o chamador nao precisa
saber qual backend esta ativo.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from interface_grafica.config import DEFAULT_PAGE_SIZE, LARGE_PARQUET_THRESHOLD_MB
from interface_grafica.services.duckdb_parquet_service import DuckDBParquetService
from interface_grafica.services.parquet_service import (
    FilterCondition,
    PageResult,
    ParquetService,
)

if TYPE_CHECKING:
    pass


class ParquetQueryService:
    """
    Facade unificada para consultas paginadas a Parquets.

    Roteia automaticamente para DuckDB (arquivos grandes / diretorios)
    ou ParquetService/Polars (arquivos pequenos com cache LRU).
    """

    def __init__(
        self,
        polars_service: ParquetService | None = None,
        duckdb_service: DuckDBParquetService | None = None,
        threshold_mb: int = LARGE_PARQUET_THRESHOLD_MB,
        v2_root: Path | None = None,
    ) -> None:
        self._polars = (
            polars_service if polars_service is not None else ParquetService(v2_root=v2_root)
        )
        self._duckdb = duckdb_service if duckdb_service is not None else DuckDBParquetService()
        self._threshold_bytes = threshold_mb * 1024 * 1024
        self._v2_root = Path(v2_root) if v2_root else None

    # ------------------------------------------------------------------
    # Roteamento
    # ------------------------------------------------------------------

    def _resolve_v2_path(self, path: Path) -> Path:
        """Retorna o path v2 side-by-side quando configurado e existente."""
        if self._v2_root is None:
            return path
        root = getattr(self._polars, "root", None)
        if root is None:
            return path
        try:
            rel = path.relative_to(root)
        except ValueError:
            return path
        candidate = self._v2_root / rel
        if candidate.exists():
            return candidate
        return path

    def usa_duckdb(self, path: Path) -> bool:
        """Retorna True se o caminho deve ser consultado via DuckDB."""
        resolved = self._resolve_v2_path(path)
        if resolved.is_dir():
            return True
        try:
            return resolved.stat().st_size > self._threshold_bytes
        except OSError:
            return False

    # ------------------------------------------------------------------
    # API publica
    # ------------------------------------------------------------------

    def get_schema(self, path: Path) -> list[str]:
        """Retorna lista de nomes de colunas."""
        resolved = self._resolve_v2_path(path)
        if self.usa_duckdb(resolved):
            return self._duckdb.get_schema(resolved)
        return self._polars.get_schema(path)

    def get_count(
        self,
        path: Path,
        filters: list[FilterCondition] | None = None,
    ) -> int:
        """Total de linhas com filtros opcionais."""
        resolved = self._resolve_v2_path(path)
        if self.usa_duckdb(resolved):
            return self._duckdb.get_count(resolved, filters)
        lf = self._polars.build_lazyframe(path, filters)
        return int(lf.select(pl.len()).collect().item())

    def get_page(
        self,
        path: Path,
        filters: list[FilterCondition] | None,
        visible_columns: list[str] | None,
        page: int,
        page_size: int = DEFAULT_PAGE_SIZE,
        sort_by: str | None = None,
        sort_desc: bool = False,
    ) -> PageResult:
        """Pagina de dados com projection pushdown e sort opcional."""
        resolved = self._resolve_v2_path(path)
        if self.usa_duckdb(resolved):
            return self._duckdb.get_page(
                resolved, filters, visible_columns, page, page_size, sort_by, sort_desc
            )
        return self._polars.get_page(
            path, filters or [], visible_columns, page, page_size, sort_by, sort_desc
        )

    def get_distinct_values(
        self,
        path: Path,
        column: str,
        search: str = "",
        limit: int = 200,
    ) -> list[str]:
        """
        Valores distintos de uma coluna com filtro e limite.

        Para arquivos pequenos usa Polars scan_parquet.
        Para arquivos grandes / diretorios usa DuckDB.
        """
        resolved = self._resolve_v2_path(path)
        if self.usa_duckdb(resolved):
            return self._duckdb.get_distinct_values(resolved, column, search, limit)
        return self._get_distinct_polars(path, column, search, limit)

    def export_to_parquet(
        self,
        path: Path,
        filters: list[FilterCondition] | None,
        columns: list[str] | None,
        target: Path,
    ) -> None:
        """Exporta recorte filtrado para Parquet."""
        resolved = self._resolve_v2_path(path)
        if self.usa_duckdb(resolved):
            self._duckdb.export_query_to_parquet(resolved, filters, columns, target)
            return
        df = self._polars.load_dataset(path, filters or [], columns)
        self._polars.save_dataset(target, df)

    def export_to_csv(
        self,
        path: Path,
        filters: list[FilterCondition] | None,
        columns: list[str] | None,
        target: Path,
    ) -> None:
        """Exporta recorte filtrado para CSV com separador ponto-e-virgula."""
        resolved = self._resolve_v2_path(path)
        if self.usa_duckdb(resolved):
            self._duckdb.export_query_to_csv(resolved, filters, columns, target)
            return
        df = self._polars.load_dataset(path, filters or [], columns)
        target.parent.mkdir(parents=True, exist_ok=True)
        df.write_csv(target, separator=";")

    # ------------------------------------------------------------------
    # Helpers internos
    # ------------------------------------------------------------------

    def _get_distinct_polars(
        self,
        path: Path,
        column: str,
        search: str,
        limit: int,
    ) -> list[str]:
        try:
            schema = pl.read_parquet_schema(path)
        except Exception:
            return []
        if column not in schema:
            return []
        lf = pl.scan_parquet(path).select(
            pl.col(column).cast(pl.Utf8, strict=False).alias("val")
        )
        if search:
            lf = lf.filter(
                pl.col("val").str.to_lowercase().str.contains(search.lower(), literal=True)
            )
        rows = (
            lf.unique()
            .sort("val")
            .limit(limit)
            .collect()["val"]
            .drop_nulls()
            .to_list()
        )
        return rows
