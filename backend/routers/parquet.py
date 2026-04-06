from __future__ import annotations

import math
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from interface_grafica.services.parquet_service import FilterCondition, ParquetService
from interface_grafica.config import CNPJ_ROOT

router = APIRouter()


class FilterItem(BaseModel):
    column: str
    operator: str
    value: str = ""


class QueryRequest(BaseModel):
    path: str
    filters: list[FilterItem] = []
    visible_columns: list[str] = []
    page: int = 1
    page_size: int = 200
    sort_by: str | None = None
    sort_desc: bool = False


def _safe_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, list):
        return [_safe_value(x) for x in v]
    return v


@router.post("/query")
def query_parquet(req: QueryRequest):
    try:
        p = Path(req.path).resolve()
        if not p.is_relative_to(CNPJ_ROOT.resolve()):
            raise ValueError()
    except Exception:
        raise HTTPException(400, "Caminho inválido ou acesso negado")
    if not p.exists():
        raise HTTPException(404, "Arquivo nao encontrado")
    svc = ParquetService(CNPJ_ROOT)
    conditions = [
        FilterCondition(column=f.column, operator=f.operator, value=f.value)
        for f in req.filters
    ]
    visible = req.visible_columns if req.visible_columns else None
    result = svc.paginate(
        parquet_path=p,
        conditions=conditions,
        visible_columns=visible,
        page=req.page,
        page_size=req.page_size,
        sort_by=req.sort_by,
        sort_desc=req.sort_desc,
    )
    rows = [
        {col: _safe_value(row[col]) for col in result.df_visible.columns}
        for row in result.df_visible.to_dicts()
    ]
    total_pages = max(1, math.ceil(result.total_rows / req.page_size))
    return {
        "total_rows": result.total_rows,
        "page": req.page,
        "page_size": req.page_size,
        "total_pages": total_pages,
        "columns": result.visible_columns,
        "all_columns": result.columns,
        "rows": rows,
    }
