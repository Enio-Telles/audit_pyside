from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from interface_grafica.config import SQL_DIR, EXTRA_SQL_DIRS

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
    dirs = [SQL_DIR] + EXTRA_SQL_DIRS
    files = []
    for d in dirs:
        if Path(d).exists():
            for f in Path(d).glob("*.sql"):
                files.append({"name": f.name, "path": str(f)})
    return files


class SqlRequest(BaseModel):
    sql: str
    cnpj: str | None = None
    params: dict[str, str] = {}


@router.post("/execute")
def execute_sql(req: SqlRequest):
    """Execute a parametric SQL file against the Oracle DB (if available)."""
    try:
        from interface_grafica.services.sql_service import SqlService
        svc = SqlService()
        result = svc.executar_sql(req.sql, params=req.params, cnpj=req.cnpj)
        rows = [dict(row) for row in (result or [])]
        return {"rows": rows, "count": len(rows)}
    except Exception as exc:
        raise HTTPException(500, f"Erro SQL: {exc}")


@router.get("/file")
def read_sql_file(path: str):
    try:
        p = Path(path).resolve()
        allowed_dirs = [Path(d).resolve() for d in [SQL_DIR] + EXTRA_SQL_DIRS]
        if not any(p.is_relative_to(d) for d in allowed_dirs):
            raise ValueError("Path outside allowed directories")
    except Exception:
        raise HTTPException(400, "Caminho inválido ou acesso negado")

    if not p.exists() or p.suffix != ".sql":
        raise HTTPException(404, "Arquivo SQL não encontrado")
    return {"content": p.read_text(encoding="utf-8", errors="replace")}
