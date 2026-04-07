from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from interface_grafica.services.sql_service import SqlService
from utilitarios.project_paths import SQL_ROOT
from utilitarios.sql_catalog import list_sql_entries, normalize_sql_id

router = APIRouter()

# Arquivos protegidos — não podem ser excluídos pela API
_PROTECTED_PREFIX = "arquivos_parquet"
_VALID_NAME_RE = re.compile(r"^[\w\-\.]+$")


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


@router.post("/execute")
def execute_sql(req: SqlRequest):
    """Execute a parametric SQL file against the Oracle DB (if available)."""
    try:
        svc = SqlService()
        result = svc.executar_sql(req.sql, params=req.params, cnpj=req.cnpj)
        rows = [_safe_value(dict(row)) for row in (result or [])]
        return {"rows": rows, "count": len(rows)}
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


class SqlFileCreate(BaseModel):
    name: str   # só o nome, sem extensão e sem separadores de path
    folder: str  # subpasta relativa dentro de sql/ (vazio = raiz)
    content: str


@router.post("/files", status_code=201)
def create_sql_file(req: SqlFileCreate):
    """Cria um novo arquivo .sql dentro de sql/ (sem sobrescrever existentes)."""
    # Validar nome: apenas word chars, hífen e ponto — sem path separators
    name = req.name.strip()
    if not name or not _VALID_NAME_RE.match(name):
        raise HTTPException(
            400,
            "Nome inválido. Use apenas letras, números, underscore, hífen ou ponto.",
        )

    # Garantir extensão .sql
    if not name.lower().endswith(".sql"):
        name = name + ".sql"

    # Construir pasta destino — proteger contra path traversal
    if req.folder.strip():
        # Normalizar e garantir que fique dentro de SQL_ROOT
        folder_norm = Path(req.folder.strip().replace("\\", "/"))
        if folder_norm.is_absolute() or ".." in folder_norm.parts:
            raise HTTPException(400, "Pasta inválida.")
        dest_dir = (SQL_ROOT / folder_norm).resolve()
        if not str(dest_dir).startswith(str(SQL_ROOT.resolve())):
            raise HTTPException(400, "Pasta fora do diretório permitido.")
    else:
        dest_dir = SQL_ROOT.resolve()

    dest_file = dest_dir / name

    if dest_file.exists():
        raise HTTPException(409, f"Arquivo '{name}' já existe nesta pasta.")

    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_file.write_text(req.content, encoding="utf-8")

    # Calcular sql_id relativo a SQL_ROOT
    sql_id = dest_file.relative_to(SQL_ROOT).as_posix()
    return {"path": sql_id}


@router.delete("/files")
def delete_sql_file(path: str = Query(..., description="sql_id do arquivo a remover")):
    """Remove um arquivo .sql do disco (protegidos em arquivos_parquet/ não podem ser excluídos)."""
    sql_id = normalize_sql_id(path)
    if sql_id is None:
        raise HTTPException(400, "Arquivo não encontrado no catálogo.")

    if sql_id.startswith(_PROTECTED_PREFIX):
        raise HTTPException(403, "Arquivos atomizados não podem ser excluídos por esta API.")

    target = SQL_ROOT / sql_id
    if not target.exists():
        raise HTTPException(404, f"Arquivo '{sql_id}' não encontrado no disco.")

    target.unlink()
    return {"deleted": sql_id}
