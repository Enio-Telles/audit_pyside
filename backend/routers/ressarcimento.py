from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Any

import polars as pl
from fastapi import APIRouter

from interface_grafica.config import CNPJ_ROOT

router = APIRouter()


def _sanitize(cnpj: str) -> str:
    return re.sub(r"\D", "", cnpj or "")


def _pasta_ressarcimento(cnpj: str) -> Path:
    return CNPJ_ROOT / cnpj / "analises" / "ressarcimento_st"


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


def _resposta_vazia(page: int = 1, page_size: int = 500) -> dict:
    return {
        "total_rows": 0,
        "page": page,
        "page_size": page_size,
        "total_pages": 1,
        "columns": [],
        "rows": [],
    }


def _ler_tabela_ressarcimento_ou_vazia(path: Path, page: int = 1, page_size: int = 500) -> dict:
    if not path.exists():
        return _resposta_vazia(page, page_size)
    return _df_to_response(pl.read_parquet(path), page, page_size)


@router.get("/{cnpj}/itens")
def get_itens(cnpj: str, page: int = 1, page_size: int = 500):
    cnpj = _sanitize(cnpj)
    path = _pasta_ressarcimento(cnpj) / f"ressarcimento_st_item_{cnpj}.parquet"
    return _ler_tabela_ressarcimento_ou_vazia(path, page, page_size)


@router.get("/{cnpj}/mensal")
def get_mensal(cnpj: str, page: int = 1, page_size: int = 500):
    cnpj = _sanitize(cnpj)
    path = _pasta_ressarcimento(cnpj) / f"ressarcimento_st_mensal_{cnpj}.parquet"
    return _ler_tabela_ressarcimento_ou_vazia(path, page, page_size)


@router.get("/{cnpj}/conciliacao")
def get_conciliacao(cnpj: str, page: int = 1, page_size: int = 500):
    cnpj = _sanitize(cnpj)
    path = _pasta_ressarcimento(cnpj) / f"ressarcimento_st_conciliacao_{cnpj}.parquet"
    return _ler_tabela_ressarcimento_ou_vazia(path, page, page_size)


@router.get("/{cnpj}/validacoes")
def get_validacoes(cnpj: str, page: int = 1, page_size: int = 500):
    cnpj = _sanitize(cnpj)
    path = _pasta_ressarcimento(cnpj) / f"ressarcimento_st_validacoes_{cnpj}.parquet"
    return _ler_tabela_ressarcimento_ou_vazia(path, page, page_size)


@router.get("/{cnpj}/resumo")
def get_resumo(cnpj: str):
    cnpj = _sanitize(cnpj)
    pasta_produtos = _pasta_produtos(cnpj)
    pasta_ressarcimento = _pasta_ressarcimento(cnpj)

    prerequisitos = {
        "efd_atomizacao": (CNPJ_ROOT / cnpj / "analises" / "atomizadas" / f"c176_tipado_{cnpj}.parquet").exists(),
        "c176_xml": (pasta_produtos / f"c176_xml_{cnpj}.parquet").exists(),
        "fatores_conversao": (pasta_produtos / f"fatores_conversao_{cnpj}.parquet").exists(),
    }
    faltantes = [nome for nome, ok in prerequisitos.items() if not ok]

    path_item = pasta_ressarcimento / f"ressarcimento_st_item_{cnpj}.parquet"
    if not path_item.exists():
        return {
            "ready": False,
            "prerequisitos": prerequisitos,
            "faltantes": faltantes,
            "qtd_itens": 0,
            "pendencias_conversao": 0,
            "parciais_pos_2022": 0,
            "itens_com_st_calc": 0,
            "itens_com_fronteira": 0,
            "cobertura_pre_2023": 0.0,
            "cobertura_pos_2023": 0.0,
        }

    df = pl.read_parquet(path_item)
    total = df.height
    pendencias = df.filter(pl.col("status_calculo") == "pendente_conversao").height
    parciais = df.filter(pl.col("status_calculo") == "parcial_pos_2022").height
    com_calc = df.filter(pl.col("possui_st_calc_ate_2022") == True).height
    com_fronteira = df.filter(pl.col("possui_fronteira") == True).height

    pre_2023 = df.filter(pl.col("ano_ref") <= 2022)
    pos_2023 = df.filter(pl.col("ano_ref") > 2022)

    cobertura_pre = 0.0
    if pre_2023.height > 0:
        cobertura_pre = round(
            (pre_2023.filter(pl.col("possui_st_calc_ate_2022") == True).height * 100.0)
            / pre_2023.height,
            2,
        )

    cobertura_pos = 0.0
    if pos_2023.height > 0:
        cobertura_pos = round(
            (pos_2023.filter(pl.col("possui_fronteira") == True).height * 100.0)
            / pos_2023.height,
            2,
        )

    return {
        "ready": total > 0 and not faltantes,
        "prerequisitos": prerequisitos,
        "faltantes": faltantes,
        "qtd_itens": total,
        "pendencias_conversao": pendencias,
        "parciais_pos_2022": parciais,
        "itens_com_st_calc": com_calc,
        "itens_com_fronteira": com_fronteira,
        "cobertura_pre_2023": cobertura_pre,
        "cobertura_pos_2023": cobertura_pos,
    }
