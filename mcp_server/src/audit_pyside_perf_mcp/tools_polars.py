"""
Tools para análise de dados com Polars.
"""

try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False

from .config import Config
from .security import guard_path


def register_polars_tools(mcp):
    """Registra tools de análise Polars."""

    @mcp.tool()
    def polars_profile_csv(path: str, max_rows: int = 1000) -> dict:
        """Gera perfil básico de um CSV com Polars."""
        if not POLARS_AVAILABLE:
            return {"ok": False, "error": "Polars não instalado"}

        try:
            full_path = guard_path(path)
            df = pl.read_csv(full_path, n_rows=max_rows)

            return {
                "ok": True,
                "rows": df.height,
                "columns": df.width,
                "schema": {name: str(dtype) for name, dtype in df.schema.items()},
                "null_counts": df.null_count().to_dicts()[0],
            }
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @mcp.tool()
    def polars_profile_parquet(path: str, max_rows: int = 1000) -> dict:
        """Gera perfil básico de um Parquet com Polars LazyFrame."""
        if not POLARS_AVAILABLE:
            return {"ok": False, "error": "Polars não instalado"}

        try:
            full_path = guard_path(path)
            lf = pl.scan_parquet(full_path)
            df = lf.head(max_rows).collect()

            return {
                "ok": True,
                "rows": df.height,
                "columns": df.width,
                "schema": {name: str(dtype) for name, dtype in df.schema.items()},
                "null_counts": df.null_count().to_dicts()[0],
            }
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @mcp.tool()
    def polars_schema_csv(path: str) -> dict:
        """Lê schema de um CSV (sem carregar dados)."""
        if not POLARS_AVAILABLE:
            return {"ok": False, "error": "Polars não instalado"}

        try:
            full_path = guard_path(path)
            df = pl.read_csv(full_path, n_rows=1)
            return {
                "ok": True,
                "schema": {name: str(dtype) for name, dtype in df.schema.items()}
            }
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @mcp.tool()
    def polars_schema_parquet(path: str) -> dict:
        """Lê schema de um Parquet usando LazyFrame."""
        if not POLARS_AVAILABLE:
            return {"ok": False, "error": "Polars não instalado"}

        try:
            full_path = guard_path(path)
            lf = pl.scan_parquet(full_path)
            return {
                "ok": True,
                "schema": {name: str(dtype) for name, dtype in lf.schema.items()}
            }
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @mcp.tool()
    def polars_validate_nulls(path: str, file_type: str = "parquet") -> dict:
        """Valida nulos por coluna."""
        if not POLARS_AVAILABLE:
            return {"ok": False, "error": "Polars não instalado"}

        try:
            full_path = guard_path(path)
            if file_type == "parquet":
                df = pl.scan_parquet(full_path).collect()
            else:
                df = pl.read_csv(full_path)

            null_info = df.null_count().to_dicts()[0]
            return {
                "ok": True,
                "total_rows": df.height,
                "null_per_column": null_info,
                "null_pct_per_column": {
                    col: (count / df.height * 100) if df.height > 0 else 0
                    for col, count in null_info.items()
                },
            }
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @mcp.tool()
    def polars_detect_duplicates(path: str, file_type: str = "parquet") -> dict:
        """Detecta duplicatas em um arquivo."""
        if not POLARS_AVAILABLE:
            return {"ok": False, "error": "Polars não instalado"}

        try:
            full_path = guard_path(path)
            if file_type == "parquet":
                df = pl.scan_parquet(full_path).collect()
            else:
                df = pl.read_csv(full_path)

            dup_count = df.is_duplicated().sum()
            return {
                "ok": True,
                "total_rows": df.height,
                "duplicate_rows": int(dup_count),
                "duplicate_pct": (dup_count / df.height * 100) if df.height > 0 else 0,
            }
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @mcp.tool()
    def polars_compare_exports(file_a: str, file_b: str, sample_size: int = 100) -> dict:
        """Compara dois arquivos (CSV/Parquet) por schema e amostra."""
        if not POLARS_AVAILABLE:
            return {"ok": False, "error": "Polars não instalado"}

        try:
            path_a = guard_path(file_a)
            path_b = guard_path(file_b)

            # Lê ambos
            if path_a.suffix.lower() == ".parquet":
                df_a = pl.scan_parquet(path_a).head(sample_size).collect()
            else:
                df_a = pl.read_csv(path_a, n_rows=sample_size)

            if path_b.suffix.lower() == ".parquet":
                df_b = pl.scan_parquet(path_b).head(sample_size).collect()
            else:
                df_b = pl.read_csv(path_b, n_rows=sample_size)

            return {
                "ok": True,
                "file_a_rows": df_a.height,
                "file_b_rows": df_b.height,
                "file_a_schema": {name: str(dtype) for name, dtype in df_a.schema.items()},
                "file_b_schema": {name: str(dtype) for name, dtype in df_b.schema.items()},
                "schemas_match": df_a.schema == df_b.schema,
            }
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @mcp.tool()
    def polars_run_lazy_query(parquet_path: str, query_expr: str) -> dict:
        """Executa query Polars lazy contra um Parquet."""
        if not POLARS_AVAILABLE:
            return {"ok": False, "error": "Polars não instalado"}

        try:
            full_path = guard_path(parquet_path)
            lf = pl.scan_parquet(full_path)

            # Avalia a expressão
            result_df = eval(f"lf{query_expr}").collect()

            return {
                "ok": True,
                "rows": result_df.height,
                "data": result_df.to_dicts()[:50],  # Limita para não sobrecarregar resposta
            }
        except Exception as e:
            return {"ok": False, "error": str(e)}
