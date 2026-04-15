"""
Funções compartilhadas para os routers do FastAPI.

Este módulo centraliza utilitários comuns usados por múltiplos routers
para evitar duplicação de código e facilitar manutenção.
"""

from __future__ import annotations

import math
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl
from fastapi import HTTPException


def sanitize_cnpj(cnpj: str) -> str:
    """Remove caracteres não numéricos do CNPJ."""
    return re.sub(r"\D", "", cnpj or "")


def safe_value(v: Any) -> Any:
    """Converte valores inválidos (NaN, Inf) para None."""
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, list):
        return [safe_value(x) for x in v]
    return v


def format_dt_inv_value(v: Any) -> Any:
    """Formata datas para o formato DD/MM/YYYY."""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.strftime("%d/%m/%Y")
    if isinstance(v, date):
        return v.strftime("%d/%m/%Y")

    s = str(v).strip()
    if not s:
        return s

    if re.match(r"^\d{2}/\d{2}/\d{4}$", s):
        return s

    if re.match(r"^\d{4}-\d{2}-\d{2}", s):
        return f"{s[8:10]}/{s[5:7]}/{s[0:4]}"

    if re.match(r"^\d{8}$", s):
        maybe_year = int(s[0:4])
        if 1900 <= maybe_year <= 2100:
            return f"{s[6:8]}/{s[4:6]}/{s[0:4]}"
        return f"{s[0:2]}/{s[2:4]}/{s[4:8]}"

    return s


def df_to_response(df: pl.DataFrame, page: int = 1, page_size: int = 500) -> dict:
    """
    Converte DataFrame para formato de resposta paginada.
    
    Args:
        df: DataFrame completo
        page: Página atual (1-based)
        page_size: Tamanho da página
        
    Returns:
        Dict com total_rows, page, total_pages, columns, rows
    """
    total = df.height
    start = (page - 1) * page_size
    end = start + page_size
    df_page = df.slice(start, page_size)
    
    rows = []
    for row in df_page.to_dicts():
        row_out: dict[str, Any] = {}
        for col in df_page.columns:
            value = safe_value(row[col])
            if col == "dt_inv":
                value = format_dt_inv_value(value)
            row_out[col] = value
        rows.append(row_out)
    
    return {
        "total_rows": total,
        "page": page,
        "page_size": page_size,
        "total_pages": max(1, math.ceil(total / page_size)),
        "columns": df_page.columns,
        "rows": rows,
    }


def resposta_vazia(page: int = 1, page_size: int = 500) -> dict:
    """Retorna resposta vazia padrão."""
    return {
        "total_rows": 0,
        "page": page,
        "page_size": page_size,
        "total_pages": 1,
        "columns": [],
        "rows": [],
    }


def validar_cnpj(cnpj: str) -> str:
    """
    Valida formato do CNPJ (apenas dígitos).
    
    Raises:
        HTTPException: Se CNPJ for inválido
    """
    sanitized = sanitize_cnpj(cnpj)
    if not sanitized or len(sanitized) not in (14, 8):
        raise HTTPException(
            status_code=400,
            detail=f"CNPJ inválido: '{cnpj}'. Deve ter 14 dígitos (CNPJ completo) ou 8 (raiz)."
        )
    return sanitized
