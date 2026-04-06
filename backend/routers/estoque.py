from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Any

import polars as pl
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from interface_grafica.config import CNPJ_ROOT

router = APIRouter()


def _sanitize(cnpj: str) -> str:
    return re.sub(r"\D", "", cnpj or "")


def _pasta_produtos(cnpj: str) -> Path:
    return CNPJ_ROOT / cnpj / "analises" / "produtos"


def _safe_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, list):
        return [_safe_value(x) for x in v]
    return v


def _df_to_response(df: pl.DataFrame, page: int = 1, page_size: int = 500) -> dict:
    total = df.height
    start = (page - 1) * page_size
    end = start + page_size
    df_page = df.slice(start, page_size)
    rows = [
        {col: _safe_value(row[col]) for col in df_page.columns}
        for row in df_page.to_dicts()
    ]
    return {
        "total_rows": total,
        "page": page,
        "page_size": page_size,
        "total_pages": max(1, math.ceil(total / page_size)),
        "columns": df_page.columns,
        "rows": rows,
    }


def _resposta_paginada_vazia(page: int = 1, page_size: int = 500) -> dict:
    """
    Retorna uma estrutura vazia compatível com o contrato esperado pelo frontend.

    Isso evita erro de leitura quando o parquet analítico ainda não foi gerado.
    """
    return {
        "total_rows": 0,
        "page": page,
        "page_size": page_size,
        "total_pages": 1,
        "columns": [],
        "rows": [],
    }


def _ler_tabela_estoque_ou_vazia(path: Path, page: int = 1, page_size: int = 500) -> dict:
    """
    Lê o parquet solicitado ou devolve uma resposta vazia quando o artefato não existe.
    """
    if not path.exists():
        return _resposta_paginada_vazia(page, page_size)
    df = pl.read_parquet(path)
    return _df_to_response(df, page, page_size)


@router.get("/{cnpj}/mov_estoque")
def get_mov_estoque(cnpj: str, page: int = 1, page_size: int = 500):
    cnpj = _sanitize(cnpj)
    path = _pasta_produtos(cnpj) / f"mov_estoque_{cnpj}.parquet"
    return _ler_tabela_estoque_ou_vazia(path, page, page_size)


@router.get("/{cnpj}/tabela_mensal")
def get_tabela_mensal(cnpj: str, page: int = 1, page_size: int = 500):
    cnpj = _sanitize(cnpj)
    path = _pasta_produtos(cnpj) / f"aba_mensal_{cnpj}.parquet"
    return _ler_tabela_estoque_ou_vazia(path, page, page_size)


@router.get("/{cnpj}/tabela_anual")
def get_tabela_anual(cnpj: str, page: int = 1, page_size: int = 500):
    cnpj = _sanitize(cnpj)
    path = _pasta_produtos(cnpj) / f"aba_anual_{cnpj}.parquet"
    return _ler_tabela_estoque_ou_vazia(path, page, page_size)


@router.get("/{cnpj}/id_agrupados")
def get_id_agrupados(cnpj: str, page: int = 1, page_size: int = 500):
    cnpj = _sanitize(cnpj)
    path = _pasta_produtos(cnpj) / f"produtos_final_{cnpj}.parquet"
    return _ler_tabela_estoque_ou_vazia(path, page, page_size)


@router.get("/{cnpj}/fatores_conversao")
def get_fatores_conversao(cnpj: str, page: int = 1, page_size: int = 500):
    cnpj = _sanitize(cnpj)
    path = _pasta_produtos(cnpj) / f"fatores_conversao_{cnpj}.parquet"
    return _ler_tabela_estoque_ou_vazia(path, page, page_size)


class FatorUpdate(BaseModel):
    id_agrupado: str
    id_produtos: str
    fator: float | None = None
    unid_ref: str | None = None


class UnidRefBatchUpdate(BaseModel):
    id_agrupado: str
    unid_ref: str


@router.patch("/{cnpj}/fatores_conversao/batch_unid_ref")
def patch_fatores_unid_ref_batch(cnpj: str, req: UnidRefBatchUpdate):
    cnpj = _sanitize(cnpj)
    path = _pasta_produtos(cnpj) / f"fatores_conversao_{cnpj}.parquet"
    if not path.exists():
        raise HTTPException(404, "fatores_conversao não encontrado")
    df = pl.read_parquet(path)
    if "unid_ref_manual" not in df.columns:
        df = df.with_columns(pl.lit(False).alias("unid_ref_manual"))
    mask = pl.col("id_agrupado") == req.id_agrupado
    df = df.with_columns([
        pl.when(mask).then(pl.lit(req.unid_ref)).otherwise(pl.col("unid_ref")).alias("unid_ref"),
        pl.when(mask).then(pl.lit(True)).otherwise(pl.col("unid_ref_manual")).alias("unid_ref_manual"),
    ])
    df.write_parquet(path)
    return {"ok": True}


@router.patch("/{cnpj}/fatores_conversao")
def patch_fatores_conversao(cnpj: str, req: FatorUpdate):
    cnpj = _sanitize(cnpj)
    path = _pasta_produtos(cnpj) / f"fatores_conversao_{cnpj}.parquet"
    if not path.exists():
        raise HTTPException(404, "fatores_conversao não encontrado")
    df = pl.read_parquet(path)
    if "fator_manual" not in df.columns:
        df = df.with_columns(pl.lit(False).alias("fator_manual"))
    if "unid_ref_manual" not in df.columns:
        df = df.with_columns(pl.lit(False).alias("unid_ref_manual"))
    mask = (pl.col("id_agrupado") == req.id_agrupado) & (pl.col("id_produtos") == req.id_produtos)
    if req.fator is not None:
        df = df.with_columns([
            pl.when(mask).then(pl.lit(req.fator)).otherwise(pl.col("fator")).alias("fator"),
            pl.when(mask).then(pl.lit(True)).otherwise(pl.col("fator_manual")).alias("fator_manual"),
        ])
    if req.unid_ref is not None:
        df = df.with_columns([
            pl.when(mask).then(pl.lit(req.unid_ref)).otherwise(pl.col("unid_ref")).alias("unid_ref"),
            pl.when(mask).then(pl.lit(True)).otherwise(pl.col("unid_ref_manual")).alias("unid_ref_manual"),
        ])
    df.write_parquet(path)
    return {"ok": True}
