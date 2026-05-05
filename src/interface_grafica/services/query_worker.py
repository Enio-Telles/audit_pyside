"""
Worker QThread para execucao assincrona de consultas Oracle.

Evita congelamento da interface durante consultas demoradas.
Emite sinais de progresso, sucesso e falha.
"""

from __future__ import annotations

import os
from pathlib import Path
from time import perf_counter
from typing import Any

import polars as pl
from PySide6.QtCore import QThread, Signal
from rich import print as rprint

from interface_grafica.utils.retry import retry_on_io
from transformacao.auxiliares.logs import log_exception
from utilitarios.perf_monitor import registrar_evento_performance
from utilitarios.project_paths import ENV_PATH

# ---------------------------------------------------------------------------
# Reutiliza funcoes do pipeline existente quando possivel
# ---------------------------------------------------------------------------
try:
    from pipeline_oracle_parquet import conectar_oracle
except ImportError:
    conectar_oracle = None  # type: ignore[assignment]

# Cache para as credenciais do banco de dados (evita repetidos os.getenv)
_DB_CONFIG_CACHE: dict[str, Any] | None = None


class QueryCancelledError(RuntimeError):
    """Consulta cancelada pelo usuario."""


def _iter_env_candidates() -> list[Path]:
    candidates = [ENV_PATH, Path.cwd() / ".env"]
    ordered: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
        except Exception:
            resolved = candidate
        if resolved in seen:
            continue
        seen.add(resolved)
        ordered.append(candidate)
    return ordered


@retry_on_io()
def _conectar_oracle_fallback():
    """Conexao Oracle standalone caso o import falhe."""
    try:
        import oracledb
    except ImportError as exc:
        raise RuntimeError("O pacote 'oracledb' nao esta instalado.") from exc

    global _DB_CONFIG_CACHE

    if _DB_CONFIG_CACHE is None:
        from dotenv import load_dotenv

        for candidate in _iter_env_candidates():
            if candidate.exists():
                load_dotenv(candidate, override=False, encoding="latin-1")
                break

        _DB_CONFIG_CACHE = {
            "host": (os.getenv("ORACLE_HOST") or "").strip(),
            "porta": (os.getenv("ORACLE_PORT") or "").strip(),
            "servico": (os.getenv("ORACLE_SERVICE") or "").strip(),
            "usuario": (os.getenv("DB_USER") or "").strip(),
            "senha": (os.getenv("DB_PASSWORD") or "").strip(),
        }

    cfg = _DB_CONFIG_CACHE
    if not all(cfg.values()):
        raise RuntimeError(
            "Configuracao Oracle incompleta. Verifique as variaveis ORACLE_HOST, ORACLE_PORT, ORACLE_SERVICE, DB_USER e DB_PASSWORD no .env"
        )

    dsn = oracledb.makedsn(cfg["host"], int(cfg["porta"]), service_name=cfg["servico"])
    conn = oracledb.connect(user=cfg["usuario"], password=cfg["senha"], dsn=dsn)
    with conn.cursor() as cursor:
        cursor.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'")
    return conn


class QueryWorker(QThread):
    """
    Executa uma consulta SQL no Oracle em thread separada.

    Signals:
        progress(str): mensagens de status intermediarias
        finished_ok(pl.DataFrame): DataFrame Polars com os resultados
        failed(str): mensagem de erro
    """

    progress = Signal(str)
    finished_ok = Signal(object)  # pl.DataFrame
    failed = Signal(str)

    def __init__(
        self,
        sql: str,
        binds: dict[str, Any],
        fetch_size: int = 50_000,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self.sql = sql
        self.binds = binds
        self.fetch_size = fetch_size

    def _raise_if_cancelled(self) -> None:
        if self.isInterruptionRequested():
            raise QueryCancelledError("Consulta cancelada pelo usuario.")

    def _connect(self) -> Any:
        self.progress.emit("Conectando ao Oracle...")
        inicio_conexao = perf_counter()
        if conectar_oracle is not None:
            conn = conectar_oracle()
        else:
            conn = _conectar_oracle_fallback()
        self._raise_if_cancelled()
        registrar_evento_performance(
            "query_worker.conectar_oracle",
            perf_counter() - inicio_conexao,
            {
                "fetch_size": self.fetch_size,
                "quantidade_binds": len(self.binds or {}),
            },
        )
        return conn

    def _execute_and_fetch(self, conn: Any) -> tuple[list[str], list[tuple]]:
        self.progress.emit("Executando consulta...")
        with conn.cursor() as cursor:
            cursor.arraysize = self.fetch_size
            cursor.prefetchrows = self.fetch_size

            inicio_execute = perf_counter()
            cursor.execute(self.sql, self.binds)
            self._raise_if_cancelled()
            registrar_evento_performance(
                "query_worker.execute",
                perf_counter() - inicio_execute,
                {
                    "fetch_size": self.fetch_size,
                    "quantidade_binds": len(self.binds or {}),
                },
            )

            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            all_rows: list[tuple] = []

            batch_num = 0
            inicio_fetch = perf_counter()
            while True:
                self._raise_if_cancelled()
                rows = cursor.fetchmany(self.fetch_size)
                if not rows:
                    break
                all_rows.extend(rows)
                batch_num += 1
                self.progress.emit(f"Lidas {len(all_rows):,} linhas...")
            registrar_evento_performance(
                "query_worker.fetchmany",
                perf_counter() - inicio_fetch,
                {
                    "fetch_size": self.fetch_size,
                    "batches": batch_num,
                    "linhas": len(all_rows),
                    "colunas": len(columns),
                },
            )
        return columns, all_rows

    def _build_dataframe(self, columns: list[str], all_rows: list[tuple]) -> pl.DataFrame:
        inicio_dataframe = perf_counter()
        if not all_rows:
            self.progress.emit("Consulta retornou 0 linhas.")
            df = pl.DataFrame({col: [] for col in columns})
        else:
            # Otimizacao Bolt: criar DataFrame diretamente de tuplas (muito mais rapido)
            df = pl.DataFrame(
                all_rows,
                schema=columns,
                orient="row",
                infer_schema_length=min(len(all_rows), 1000),
            )
        registrar_evento_performance(
            "query_worker.build_dataframe",
            perf_counter() - inicio_dataframe,
            {
                "linhas": df.height,
                "colunas": df.width,
            },
        )
        return df

    def _handle_cancellation(self, conn: Any, exc: Exception, inicio_total: float) -> None:
        if conn is not None:
            try:
                cancel = getattr(conn, "cancel", None)
                if callable(cancel):
                    cancel()
            except Exception as cancel_exc:
                log_exception(cancel_exc)
        registrar_evento_performance(
            "query_worker.total",
            perf_counter() - inicio_total,
            {
                "fetch_size": self.fetch_size,
                "quantidade_binds": len(self.binds or {}),
                "erro": str(exc),
            },
            status="cancelled",
        )
        self.progress.emit("Consulta cancelada.")
        self.failed.emit("Consulta cancelada pelo usuario.")

    def _handle_error(self, exc: Exception, inicio_total: float) -> None:
        log_exception(exc)
        registrar_evento_performance(
            "query_worker.total",
            perf_counter() - inicio_total,
            {
                "fetch_size": self.fetch_size,
                "quantidade_binds": len(self.binds or {}),
                "erro": str(exc),
            },
            status="error",
        )
        # 🛡️ Sentinel: Sanitize error message to prevent leaking internal database schema/details to the UI
        rprint(f"[red]Erro interno no QueryWorker:[/red] {exc}")
        safe_error_msg = "Ocorreu um erro ao executar a consulta no banco de dados. Verifique os logs para mais detalhes."
        self.failed.emit(safe_error_msg)

    def _close_connection(self, conn: Any) -> None:
        if conn is not None:
            try:
                conn.close()
            except Exception as close_exc:
                log_exception(close_exc)
                rprint(f"[yellow]Aviso: Erro ao fechar conexao Oracle:[/yellow] {close_exc}")

    def run(self) -> None:
        conn = None
        inicio_total = perf_counter()
        try:
            self._raise_if_cancelled()
            conn = self._connect()
            self._raise_if_cancelled()

            columns, all_rows = self._execute_and_fetch(conn)
            self._raise_if_cancelled()

            df = self._build_dataframe(columns, all_rows)

            self.progress.emit(f"Concluido: {df.height:,} linhas, {df.width} colunas.")
            registrar_evento_performance(
                "query_worker.total",
                perf_counter() - inicio_total,
                {
                    "fetch_size": self.fetch_size,
                    "linhas": df.height,
                    "colunas": df.width,
                    "quantidade_binds": len(self.binds or {}),
                },
            )
            self.finished_ok.emit(df)

        except QueryCancelledError as exc:
            self._handle_cancellation(conn, exc, inicio_total)

        except Exception as exc:
            self._handle_error(exc, inicio_total)

        finally:
            self._close_connection(conn)
