"""
Tools para auditoria segura do Oracle: metadados, planos, queries read-only.
"""

import time
import os
from typing import Any

try:
    import oracledb
    ORACLE_AVAILABLE = True
except ImportError:
    ORACLE_AVAILABLE = False

from .config import Config
from .security import guard_sql, SqlSecurityError


def register_oracle_tools(mcp):
    """Registra tools seguras de Oracle."""

    @mcp.tool()
    def oracle_ping() -> dict:
        """Testa conexão com Oracle usando variáveis de ambiente."""
        if not ORACLE_AVAILABLE:
            return {"ok": False, "error": "oracledb não instalado"}

        if not all([Config.ORACLE_USER, Config.ORACLE_PASSWORD, Config.ORACLE_DSN]):
            return {"ok": False, "error": "Credenciais Oracle não configuradas"}

        try:
            with oracledb.connect(
                user=Config.ORACLE_USER,
                password=Config.ORACLE_PASSWORD,
                dsn=Config.ORACLE_DSN,
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute("select 1 from dual")
                    value = cur.fetchone()[0]
                    return {"ok": True, "result": f"Oracle conectado (select 1 = {value})"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @mcp.tool()
    def oracle_describe_table(owner: str, table_name: str) -> dict:
        """Descreve colunas de uma tabela Oracle (metadados)."""
        if not ORACLE_AVAILABLE:
            return {"ok": False, "error": "oracledb não instalado"}

        owner = owner.upper()
        table_name = table_name.upper()

        sql = """
            select column_name, data_type, nullable, data_length, data_precision, data_scale
            from all_tab_columns
            where owner = :owner
              and table_name = :table_name
            order by column_id
        """

        try:
            with oracledb.connect(
                user=Config.ORACLE_USER,
                password=Config.ORACLE_PASSWORD,
                dsn=Config.ORACLE_DSN,
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, owner=owner, table_name=table_name)
                    columns = [d[0].lower() for d in cur.description]
                    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
                    return {"ok": True, "columns": rows}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @mcp.tool()
    def oracle_index_report(owner: str, table_name: str) -> dict:
        """Lista índices de uma tabela Oracle."""
        if not ORACLE_AVAILABLE:
            return {"ok": False, "error": "oracledb não instalado"}

        owner = owner.upper()
        table_name = table_name.upper()

        sql = """
            select index_name, index_type, uniqueness
            from all_indexes
            where table_owner = :owner
              and table_name = :table_name
            order by index_name
        """

        try:
            with oracledb.connect(
                user=Config.ORACLE_USER,
                password=Config.ORACLE_PASSWORD,
                dsn=Config.ORACLE_DSN,
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, owner=owner, table_name=table_name)
                    columns = [d[0].lower() for d in cur.description]
                    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
                    return {"ok": True, "indexes": rows}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @mcp.tool()
    def oracle_explain_select(sql: str) -> dict:
        """Mostra plano de execução EXPLAIN PLAN para SELECT permitido."""
        if not ORACLE_AVAILABLE:
            return {"ok": False, "error": "oracledb não instalado"}

        try:
            guard_sql(sql)
        except SqlSecurityError as e:
            return {"ok": False, "error": str(e)}

        plan_sql = f"explain plan for {sql}"

        try:
            with oracledb.connect(
                user=Config.ORACLE_USER,
                password=Config.ORACLE_PASSWORD,
                dsn=Config.ORACLE_DSN,
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute(plan_sql)

                    cur.execute(
                        "select plan_table_output from table(dbms_xplan.display())"
                    )
                    plan = [row[0] for row in cur.fetchall()]
                    return {"ok": True, "plan": "\n".join(plan)}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @mcp.tool()
    def oracle_readonly_query(sql: str, max_rows: int = 50) -> dict:
        """Executa SELECT/WITH com limite de linhas (read-only)."""
        if not ORACLE_AVAILABLE:
            return {"ok": False, "error": "oracledb não instalado"}

        try:
            guard_sql(sql)
        except SqlSecurityError as e:
            return {"ok": False, "error": str(e)}

        max_rows = max(1, min(max_rows, Config.MAX_QUERY_ROWS))

        wrapped_sql = f"""
            select *
            from (
                {sql}
            )
            where rownum <= :max_rows
        """

        started = time.perf_counter()

        try:
            with oracledb.connect(
                user=Config.ORACLE_USER,
                password=Config.ORACLE_PASSWORD,
                dsn=Config.ORACLE_DSN,
                timeout=Config.SQL_TIMEOUT_SEC,
            ) as conn:
                with conn.cursor() as cur:
                    cur.arraysize = 1000
                    cur.execute(wrapped_sql, max_rows=max_rows)

                    columns = [d[0].lower() for d in cur.description]
                    rows = [dict(zip(columns, row)) for row in cur.fetchall()]

            elapsed_ms = round((time.perf_counter() - started) * 1000, 2)

            return {
                "ok": True,
                "rows_returned": len(rows),
                "max_rows": max_rows,
                "elapsed_ms": elapsed_ms,
                "data": rows,
            }
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @mcp.tool()
    def oracle_table_stats(owner: str, table_name: str) -> dict:
        """Mostra estatísticas de uma tabela (tamanho, linhas, etc.)."""
        if not ORACLE_AVAILABLE:
            return {"ok": False, "error": "oracledb não instalado"}

        owner = owner.upper()
        table_name = table_name.upper()

        sql = """
            select num_rows, blocks, avg_row_len, last_analyzed
            from all_tables
            where owner = :owner
              and table_name = :table_name
        """

        try:
            with oracledb.connect(
                user=Config.ORACLE_USER,
                password=Config.ORACLE_PASSWORD,
                dsn=Config.ORACLE_DSN,
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, owner=owner, table_name=table_name)
                    row = cur.fetchone()
                    if row:
                        columns = [d[0].lower() for d in cur.description]
                        return {"ok": True, "stats": dict(zip(columns, row))}
                    else:
                        return {"ok": False, "error": "Tabela não encontrada"}
        except Exception as e:
            return {"ok": False, "error": str(e)}
