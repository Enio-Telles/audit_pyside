"""
Servico para leitura, parse e execucao de arquivos SQL do catalogo local.

Responsabilidades:
- listar arquivos .sql apenas da arvore canonica do projeto;
- ler conteudo SQL com fallback de encoding;
- extrair bind variables Oracle (:param) e inferir tipo de widget;
- construir dicionario de binds para execucao.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from utilitarios.extrair_parametros import extrair_parametros_sql
from utilitarios.sql_catalog import list_sql_entries, resolve_sql_path


WIDGET_DATE = "date"
WIDGET_TEXT = "text"


@dataclass
class ParamInfo:
    """Informacoes sobre um parametro SQL detectado."""

    name: str
    widget_type: str = WIDGET_TEXT
    placeholder: str = ""


@dataclass
class SqlFileInfo:
    """Metadados de um arquivo SQL do catalogo local."""

    sql_id: str
    path: Path
    display_name: str
    source_dir: str


class SqlService:
    def list_sql_files(self) -> list[SqlFileInfo]:
        result: list[SqlFileInfo] = []
        for entry in list_sql_entries():
            result.append(
                SqlFileInfo(
                    sql_id=entry.sql_id,
                    path=entry.path,
                    display_name=entry.display_name,
                    source_dir=entry.source_label,
                )
            )
        return result

    @staticmethod
    def read_sql(path_or_id: Path | str) -> str:
        path = resolve_sql_path(path_or_id)
        for enc in ("utf-8", "latin-1", "cp1252", "iso-8859-1"):
            try:
                return path.read_text(encoding=enc).strip().rstrip(";")
            except UnicodeDecodeError:
                continue
        raise RuntimeError(f"Nao foi possivel ler o SQL: {path}")

    @staticmethod
    def extract_params(sql: str) -> list[ParamInfo]:
        raw_names = extrair_parametros_sql(sql)
        all_matches = re.findall(r"(?<!\[):([A-Za-z_]\w*)", sql)
        seen: set[str] = set()
        ordered: list[str] = []
        for name in all_matches:
            low = name.lower()
            if low in seen or name not in raw_names:
                continue
            seen.add(low)
            ordered.append(name)

        params: list[ParamInfo] = []
        for name in ordered:
            params.append(
                ParamInfo(
                    name=name,
                    widget_type=SqlService._infer_widget_type(name),
                    placeholder=SqlService._infer_placeholder(name),
                )
            )
        return params

    @staticmethod
    def _infer_widget_type(name: str) -> str:
        low = name.lower()
        if low.startswith(("data_", "dt_", "date_")) or low in ("data_limite_processamento",):
            return WIDGET_DATE
        return WIDGET_TEXT

    @staticmethod
    def _infer_placeholder(name: str) -> str:
        low = name.lower()
        if "cnpj" in low:
            return "Somente digitos"
        if low.startswith(("data_", "dt_")):
            return "DD/MM/AAAA"
        return ""

    @staticmethod
    def build_binds(sql: str, values: dict[str, Any]) -> dict[str, Any]:
        provided = {k.lower(): v for k, v in values.items()}
        binds: dict[str, Any] = {}
        matches = re.findall(r"(?<!\[):([A-Za-z_]\w*)", sql)
        seen: set[str] = set()
        for name in matches:
            low = name.lower()
            if low in seen:
                continue
            seen.add(low)
            binds[name] = provided.get(low)
        return binds

    @staticmethod
    def executar_sql(sql: str, params: dict[str, Any] | None = None, cnpj: str | None = None) -> list[dict[str, Any]]:
        try:
            from interface_grafica.services.query_worker import _conectar_oracle_fallback
        except Exception as exc:
            from transformacao.auxiliares.logs import log_exception
            log_exception(exc)
            raise RuntimeError("Nao foi possivel inicializar a conexao com o banco de dados.") from exc

        values = dict(params or {})
        if cnpj and not any(str(k).lower() == "cnpj" for k in values):
            values["CNPJ"] = cnpj

        # Envia apenas bind variables existentes no SQL para evitar DPY-4008.
        binds = SqlService.build_binds(sql, values)

        conn = _conectar_oracle_fallback()
        try:
            with conn.cursor() as cursor:
                if binds:
                    cursor.execute(sql, binds)
                else:
                    cursor.execute(sql)
                columns = [desc[0] for desc in cursor.description or []]
                rows = cursor.fetchall()
            if not rows:
                return []
            return [dict(zip(columns, row)) for row in rows]
        finally:
            conn.close()
