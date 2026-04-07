from __future__ import annotations

import math
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from interface_grafica.services.sql_service import SqlService
from utilitarios.sql_catalog import list_sql_entries, normalize_sql_id

router = APIRouter()


def _safe_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, list):
        return [_safe_value(x) for x in v]
    return v


@router.get("/files")
def list_sql_files():
    return [{"name": entry.path.name, "path": entry.sql_id} for entry in list_sql_entries()]


class SqlRequest(BaseModel):
    sql: str
    cnpj: str | None = None
    params: dict[str, str] = {}
    page: int = 1
    page_size: int = 200


@router.post("/execute")
def execute_sql(req: SqlRequest):
    """Execute a parametric SQL file against the Oracle DB (if available)."""
    try:
        svc = SqlService()
        result = svc.executar_sql(req.sql, params=req.params, cnpj=req.cnpj)
        all_rows = [_safe_value(dict(row)) for row in (result or [])]
        total_count = len(all_rows)
        page_size = min(max(1, req.page_size), 2000)
        offset = (max(1, req.page) - 1) * page_size
        rows = all_rows[offset : offset + page_size]
        total_pages = max(1, math.ceil(total_count / page_size))
        return {
            "rows": rows,
            "count": total_count,
            "page": req.page,
            "page_size": page_size,
            "total_pages": total_pages,
        }
    except Exception as exc:
        raise HTTPException(500, f"Erro SQL: {exc}") from exc


@router.get("/file")
def read_sql_file(path: str):
    sql_id = normalize_sql_id(path)
    if sql_id is None:
        raise HTTPException(400, "Caminho invalido ou SQL fora do catalogo")

    try:
        return {"content": SqlService.read_sql(sql_id)}
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:
        raise HTTPException(500, f"Erro ao ler SQL: {exc}") from exc
