"""
DuckDBParquetService — backend de consulta lazy para Parquets grandes.

Regras de uso:
- Cada metodo publico abre sua propria conexao DuckDB in-memory.
- Nao compartilhar conexao entre threads.
- Toda query SQL usa parametros posicionais (?), nunca concatenacao de string de usuario.
- Retornar DataFrame Polars pequeno via .pl() — nunca o arquivo inteiro.
- Suporta arquivo unico e diretorio particionado (Hive ou glob).
"""
from __future__ import annotations

from pathlib import Path
from time import perf_counter
from typing import Iterable

import duckdb
import polars as pl

from interface_grafica.config import DEFAULT_PAGE_SIZE
from interface_grafica.services.parquet_service import FilterCondition, PageResult
from utilitarios.perf_monitor import registrar_evento_performance


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------


def _source(path: Path) -> tuple[str, str]:
    """
    Retorna (glob_pattern, from_clause) para uso em SQL.

    Para diretorio particionado, usa hive_partitioning=true.
    Para arquivo unico, usa read_parquet simples.
    """
    if path.is_dir():
        pattern = str(path / "**" / "*.parquet")
        return pattern, "read_parquet(?, hive_partitioning=true)"
    return str(path), "read_parquet(?)"


def _get_schema_map(conn: duckdb.DuckDBPyConnection, from_clause: str, source_path: str) -> dict[str, str]:
    """Retorna {coluna: tipo_duckdb} para o Parquet."""
    rows = conn.execute(
        f"SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM {from_clause} LIMIT 0)",
        [source_path],
    ).fetchall()
    return {row[0]: row[1] for row in rows}


def _normalize_op(op: str) -> str:
    """Normaliza operador de filtro para canonical form."""
    op_l = (op or "").strip().lower()
    if op_l.startswith("cont"):
        return "contem"
    if op_l.startswith("come"):
        return "comeca_com"
    if op_l.startswith("termina"):
        return "termina_com"
    if "nulo" in op_l:
        return "nao_e_nulo" if ("não" in op_l or "nao" in op_l) else "e_nulo"
    aliases: dict[str, set[str]] = {
        "contem": {"contém", "contem"},
        "igual": {"igual", "="},
        "comeca_com": {"começa com", "comeca com"},
        "termina_com": {"termina com"},
        "maior": {">"},
        "maior_igual": {">="},
        "menor": {"<"},
        "menor_igual": {"<="},
        "e_nulo": {"é nulo", "e nulo"},
        "nao_e_nulo": {"não é nulo", "nao e nulo"},
    }
    for canonical, opts in aliases.items():
        if op_l in opts:
            return canonical
    return op_l


def _build_where(
    conditions: Iterable[FilterCondition] | None,
    schema_map: dict[str, str],
) -> tuple[str, list]:
    """
    Converte FilterConditions em clausula WHERE parametrizada DuckDB.
    Retorna (sql_fragment, params).
    Nunca concatena valores do usuario na string SQL.
    """
    if not conditions:
        return "", []

    clauses: list[str] = []
    params: list = []

    for cond in conditions:
        if not cond.column or cond.column not in schema_map:
            continue
        op = _normalize_op(cond.operator)
        val = cond.value or ""
        col_q = f'"{cond.column}"'
        col_text = f'CAST("{cond.column}" AS VARCHAR)'

        if op == "e_nulo":
            clauses.append(f"({col_q} IS NULL OR {col_text} = '')")
        elif op == "nao_e_nulo":
            clauses.append(f"({col_q} IS NOT NULL AND {col_text} != '')")
        elif op == "contem":
            if not val:
                continue
            clauses.append(f"LOWER({col_text}) LIKE LOWER(?)")
            params.append(f"%{val}%")
        elif op == "comeca_com":
            if not val:
                continue
            clauses.append(f"LOWER({col_text}) LIKE LOWER(?)")
            params.append(f"{val}%")
        elif op == "termina_com":
            if not val:
                continue
            clauses.append(f"LOWER({col_text}) LIKE LOWER(?)")
            params.append(f"%{val}")
        elif op == "igual":
            clauses.append(f"{col_text} = ?")
            params.append(val)
        elif op in ("maior", ">"):
            try:
                params.append(float(val.replace(",", ".")))
                clauses.append(f"TRY_CAST({col_q} AS DOUBLE) > ?")
            except (ValueError, AttributeError):
                continue
        elif op in ("maior_igual", ">="):
            try:
                params.append(float(val.replace(",", ".")))
                clauses.append(f"TRY_CAST({col_q} AS DOUBLE) >= ?")
            except (ValueError, AttributeError):
                continue
        elif op in ("menor", "<"):
            try:
                params.append(float(val.replace(",", ".")))
                clauses.append(f"TRY_CAST({col_q} AS DOUBLE) < ?")
            except (ValueError, AttributeError):
                continue
        elif op in ("menor_igual", "<="):
            try:
                params.append(float(val.replace(",", ".")))
                clauses.append(f"TRY_CAST({col_q} AS DOUBLE) <= ?")
            except (ValueError, AttributeError):
                continue

    if not clauses:
        return "", []
    return " WHERE " + " AND ".join(clauses), params


def _build_order(sort_by: str | None, sort_desc: bool, schema_map: dict[str, str]) -> str:
    if sort_by and sort_by in schema_map:
        direction = "DESC" if sort_desc else "ASC"
        return f' ORDER BY "{sort_by}" {direction}'
    return ""


# ---------------------------------------------------------------------------
# Servico principal
# ---------------------------------------------------------------------------


class DuckDBParquetService:
    """
    Consulta lazy de Parquets grandes via DuckDB.

    Cada metodo abre uma conexao in-memory independente — seguro para uso
    em workers Qt paralelos sem lock externo.
    """

    def get_schema(self, parquet_path: Path) -> list[str]:
        """Retorna lista de nomes de colunas sem materializar dados."""
        inicio = perf_counter()
        src, from_clause = _source(parquet_path)
        with duckdb.connect(":memory:") as conn:
            schema_map = _get_schema_map(conn, from_clause, src)
        cols = list(schema_map.keys())
        registrar_evento_performance(
            "duckdb_parquet_service.get_schema",
            perf_counter() - inicio,
            {"parquet_path": parquet_path, "colunas": len(cols)},
        )
        return cols

    def get_count(
        self,
        parquet_path: Path,
        filters: list[FilterCondition] | None = None,
    ) -> int:
        """Retorna total de linhas (com filtros opcionais) sem materializar o DataFrame."""
        inicio = perf_counter()
        src, from_clause = _source(parquet_path)
        with duckdb.connect(":memory:") as conn:
            schema_map = _get_schema_map(conn, from_clause, src)
            where, params = _build_where(filters, schema_map)
            sql = f"SELECT COUNT(*) FROM {from_clause}{where}"
            total = int(conn.execute(sql, [src] + params).fetchone()[0])  # type: ignore[index]
        registrar_evento_performance(
            "duckdb_parquet_service.get_count",
            perf_counter() - inicio,
            {
                "parquet_path": parquet_path,
                "total": total,
                "quantidade_filtros": len(filters or []),
            },
        )
        return total

    def get_page(
        self,
        parquet_path: Path,
        filters: list[FilterCondition] | None,
        visible_columns: list[str] | None,
        page: int,
        page_size: int = DEFAULT_PAGE_SIZE,
        sort_by: str | None = None,
        sort_desc: bool = False,
    ) -> PageResult:
        """
        Retorna uma pagina de dados com projection pushdown.

        Somente as colunas visiveis (+ coluna de sort, se houver) sao buscadas.
        Nao materializa o arquivo inteiro.
        """
        inicio = perf_counter()
        page = max(page, 1)
        src, from_clause = _source(parquet_path)

        with duckdb.connect(":memory:") as conn:
            schema_map = _get_schema_map(conn, from_clause, src)
            all_columns = list(schema_map.keys())

            # Projection pushdown: colunas visiveis + sort_by
            cols_to_fetch = list(visible_columns) if visible_columns else all_columns[:]
            if sort_by and sort_by in schema_map and sort_by not in cols_to_fetch:
                cols_to_fetch.append(sort_by)
            cols_to_fetch = [c for c in cols_to_fetch if c in schema_map]
            if not cols_to_fetch:
                cols_to_fetch = all_columns[:1] if all_columns else []

            select_cols = ", ".join(f'"{c}"' for c in cols_to_fetch) or "*"
            where, filter_params = _build_where(filters, schema_map)
            order = _build_order(sort_by, sort_desc, schema_map)
            offset = (page - 1) * page_size

            # Count
            count_sql = f"SELECT COUNT(*) FROM {from_clause}{where}"
            total_rows = int(conn.execute(count_sql, [src] + filter_params).fetchone()[0])  # type: ignore[index]

            # Page
            page_sql = (
                f"SELECT {select_cols} FROM {from_clause}"
                f"{where}{order} LIMIT ? OFFSET ?"
            )
            df = conn.execute(page_sql, [src] + filter_params + [page_size, offset]).pl()

        # df_visible: somente as colunas visiveis (sem sort_by extra)
        actual_visible = visible_columns if visible_columns else all_columns
        df_visible = df.select([c for c in actual_visible if c in df.columns])

        elapsed = perf_counter() - inicio
        registrar_evento_performance(
            "duckdb_parquet_service.get_page",
            elapsed,
            {
                "parquet_path": parquet_path,
                "page": page,
                "page_size": page_size,
                "total_rows": total_rows,
                "linhas_pagina": df.height,
                "colunas_projetadas": len(cols_to_fetch),
                "quantidade_filtros": len(filters or []),
            },
        )
        return PageResult(
            total_rows=total_rows,
            df_all_columns=df,
            df_visible=df_visible,
            columns=all_columns,
            visible_columns=list(actual_visible),
        )

    def get_distinct_values(
        self,
        parquet_path: Path,
        column: str,
        search: str = "",
        limit: int = 200,
    ) -> list[str]:
        """
        Retorna valores distintos de uma coluna, com filtro de busca e limite.

        Nao materializa a coluna inteira — usa LIMIT no SQL.
        """
        inicio = perf_counter()
        src, from_clause = _source(parquet_path)
        with duckdb.connect(":memory:") as conn:
            schema_map = _get_schema_map(conn, from_clause, src)
            if column not in schema_map:
                return []
            col_text = f'CAST("{column}" AS VARCHAR)'
            if search:
                where = f" WHERE LOWER({col_text}) LIKE LOWER(?)"
                params: list = [src, f"%{search}%", limit]
            else:
                where = ""
                params = [src, limit]
            sql = (
                f"SELECT DISTINCT {col_text} AS val "
                f"FROM {from_clause}"
                f"{where} "
                f"ORDER BY val "
                f"LIMIT ?"
            )
            rows = conn.execute(sql, params).fetchall()
        values = [row[0] for row in rows if row[0] is not None]
        registrar_evento_performance(
            "duckdb_parquet_service.get_distinct_values",
            perf_counter() - inicio,
            {
                "parquet_path": parquet_path,
                "column": column,
                "search": search,
                "limit": limit,
                "retornados": len(values),
            },
        )
        return values

    def export_query_to_parquet(
        self,
        parquet_path: Path,
        filters: list[FilterCondition] | None,
        columns: list[str] | None,
        target: Path,
    ) -> None:
        """
        Exporta recorte filtrado para Parquet via DuckDB COPY.

        Estrategia: CREATE TABLE AS SELECT com ? parametrizados (seguro para
        filtros do usuario), depois COPY para o arquivo de destino (path interno,
        sem input do usuario — uso de f-string e seguro aqui).
        """
        inicio = perf_counter()
        target.parent.mkdir(parents=True, exist_ok=True)
        src, from_clause = _source(parquet_path)
        # Caminhos internos: converte para forward slashes para compatibilidade SQL
        target_sql = str(target).replace("\\", "/")
        with duckdb.connect(":memory:") as conn:
            schema_map = _get_schema_map(conn, from_clause, src)
            cols_to_export = [c for c in (columns or []) if c in schema_map] or list(schema_map.keys())
            select_cols = ", ".join(f'"{c}"' for c in cols_to_export)
            where, filter_params = _build_where(filters, schema_map)
            # Passo 1: materializa recorte em tabela temporaria com filtros parametrizados
            conn.execute(
                f"CREATE TABLE _export AS SELECT {select_cols} FROM {from_clause}{where}",
                [src] + filter_params,
            )
            # Passo 2: COPY para arquivo — target e caminho interno (sem input do usuario)
            conn.execute(
                f"COPY _export TO '{target_sql}' "
                f"(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 200000)"
            )
        registrar_evento_performance(
            "duckdb_parquet_service.export_to_parquet",
            perf_counter() - inicio,
            {"parquet_path": parquet_path, "target": target, "quantidade_filtros": len(filters or [])},
        )

    def export_query_to_csv(
        self,
        parquet_path: Path,
        filters: list[FilterCondition] | None,
        columns: list[str] | None,
        target: Path,
    ) -> None:
        """
        Exporta recorte filtrado para CSV via DuckDB COPY.

        Nao coleta o resultado em Python. Usa separador ponto-e-virgula.
        """
        inicio = perf_counter()
        target.parent.mkdir(parents=True, exist_ok=True)
        src, from_clause = _source(parquet_path)
        target_sql = str(target).replace("\\", "/")
        with duckdb.connect(":memory:") as conn:
            schema_map = _get_schema_map(conn, from_clause, src)
            cols_to_export = [c for c in (columns or []) if c in schema_map] or list(schema_map.keys())
            select_cols = ", ".join(f'"{c}"' for c in cols_to_export)
            where, filter_params = _build_where(filters, schema_map)
            conn.execute(
                f"CREATE TABLE _export AS SELECT {select_cols} FROM {from_clause}{where}",
                [src] + filter_params,
            )
            conn.execute(
                f"COPY _export TO '{target_sql}' (FORMAT CSV, HEADER true, DELIMITER ';')"
            )
        registrar_evento_performance(
            "duckdb_parquet_service.export_to_csv",
            perf_counter() - inicio,
            {"parquet_path": parquet_path, "target": target, "quantidade_filtros": len(filters or [])},
        )
