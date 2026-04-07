from __future__ import annotations

import io
import math
from pathlib import Path
from typing import Any

import polars as pl
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from interface_grafica.services.parquet_service import FilterCondition, ParquetService
from interface_grafica.config import CNPJ_ROOT

router = APIRouter()
_parquet_service = ParquetService(CNPJ_ROOT)


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
    sort_by_list: list[str] = []


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
    svc = _parquet_service
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
        sort_by_list=req.sort_by_list if req.sort_by_list else None,
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


class MetadataResponse(BaseModel):
    path: str
    total_rows: int
    columns: list[str]
    dtypes: dict[str, str]
    sample: list[dict]
    numeric_stats: dict[str, dict[str, Any]]


@router.get("/metadata")
def get_metadata(path: str):
    try:
        p = Path(path).resolve()
        if not p.is_relative_to(CNPJ_ROOT.resolve()):
            raise ValueError()
    except Exception:
        raise HTTPException(400, "Caminho inválido ou acesso negado")
    if not p.exists():
        raise HTTPException(404, "Arquivo nao encontrado")

    lf = pl.scan_parquet(p)
    schema = lf.schema
    total = lf.select(pl.len()).collect()[0, 0]
    sample_rows = lf.head(5).collect().to_dicts()
    stats: dict[str, dict[str, Any]] = {}
    numeric_types = {
        pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Int16, pl.Int8,
        pl.UInt64, pl.UInt32, pl.UInt16, pl.UInt8,
    }
    for col, dtype in schema.items():
        if type(dtype) in numeric_types:
            row = lf.select(
                pl.col(col).min().alias("min"),
                pl.col(col).max().alias("max"),
                pl.col(col).mean().alias("mean"),
                pl.col(col).null_count().alias("null_count"),
            ).collect().to_dicts()[0]
            stats[col] = {k: _safe_value(v) for k, v in row.items()}

    return MetadataResponse(
        path=str(p),
        total_rows=int(total),
        columns=list(schema.keys()),
        dtypes={k: str(v) for k, v in schema.items()},
        sample=[{k: _safe_value(v) for k, v in r.items()} for r in sample_rows],
        numeric_stats=stats,
    )


@router.post("/export-csv")
def export_csv(req: QueryRequest):
    try:
        p = Path(req.path).resolve()
        if not p.is_relative_to(CNPJ_ROOT.resolve()):
            raise ValueError()
    except Exception:
        raise HTTPException(400, "Caminho inválido ou acesso negado")
    if not p.exists():
        raise HTTPException(404, "Arquivo nao encontrado")

    svc = _parquet_service
    conditions = [
        FilterCondition(column=f.column, operator=f.operator, value=f.value)
        for f in req.filters
    ]
    visible = req.visible_columns if req.visible_columns else None
    # Export all rows without pagination
    result = svc.paginate(
        parquet_path=p,
        conditions=conditions,
        visible_columns=visible,
        page=1,
        page_size=999_999,
        sort_by=req.sort_by,
        sort_desc=req.sort_desc,
        sort_by_list=req.sort_by_list if req.sort_by_list else None,
    )
    buf = io.StringIO()
    result.df_visible.write_csv(buf)
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=export.csv"},
    )
