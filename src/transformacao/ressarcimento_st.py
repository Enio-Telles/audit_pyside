"""Proxy module for backward compatibility. Real module: transformacao.ressarcimento_st_pkg"""

from __future__ import annotations

from pathlib import Path

from transformacao.ressarcimento_st_pkg import executar_pipeline_ressarcimento_st


def gerar_ressarcimento_st(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    return executar_pipeline_ressarcimento_st(cnpj, pasta_cnpj)
