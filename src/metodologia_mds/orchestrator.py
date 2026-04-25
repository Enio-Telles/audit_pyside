"""Orquestrador para tarefas de alto nível da metodologia MDS.

Fornece funções convenientes para executar etapas do pipeline relacionadas
à metodologia: geração de `mov_estoque`, cálculos por período, mensais e anuais.
"""
from __future__ import annotations
from typing import Optional


def gerar_movimentacao_estoque(cnpj: str, pasta_cnpj: Optional[str] = None) -> bool:
    """Gera a tabela de movimentação de estoque para o CNPJ informado.

    Args:
        cnpj: CNPJ do contribuinte (14 dígitos, com ou sem formatação).
        pasta_cnpj: Caminho base do CNPJ; usa o padrão do pipeline quando None.

    Returns:
        True em caso de sucesso; False em falha.
    """
    try:
        from transformacao.movimentacao_estoque_pkg.movimentacao_estoque import (
            gerar_movimentacao_estoque as _gerar,
        )
    except Exception as exc:  # pragma: no cover - import path issues surfaced at runtime
        raise RuntimeError(f"Não foi possível importar módulo de movimentacao_estoque: {exc}") from exc

    return _gerar(cnpj, pasta_cnpj)


def gerar_calculos_periodos(cnpj: str, pasta_cnpj: Optional[str] = None) -> bool:
    """Gera os cálculos agregados por período para o CNPJ informado.

    Args:
        cnpj: CNPJ do contribuinte.
        pasta_cnpj: Caminho base do CNPJ; usa o padrão do pipeline quando None.

    Returns:
        True em caso de sucesso; False em falha.
    """
    try:
        from transformacao.calculos_periodo_pkg.calculos_periodo import (
            gerar_calculos_periodos as _gerar_per,
        )
    except Exception as exc:
        raise RuntimeError(f"Não foi possível importar calculos_periodo: {exc}") from exc

    return _gerar_per(cnpj, pasta_cnpj)


def gerar_calculos_mensais(cnpj: str, pasta_cnpj: Optional[str] = None) -> bool:
    """Gera os cálculos mensais de estoque e movimentação para o CNPJ informado.

    Args:
        cnpj: CNPJ do contribuinte.
        pasta_cnpj: Caminho base do CNPJ; usa o padrão do pipeline quando None.

    Returns:
        True em caso de sucesso; False em falha.
    """
    try:
        from transformacao.calculos_mensais_pkg.calculos_mensais import (
            gerar_calculos_mensais as _gerar_mes,
        )
    except Exception as exc:
        raise RuntimeError(f"Não foi possível importar calculos_mensais: {exc}") from exc

    return _gerar_mes(cnpj, pasta_cnpj)


def gerar_calculos_anuais(cnpj: str, pasta_cnpj: Optional[str] = None) -> bool:
    """Gera os cálculos anuais de estoque e movimentação para o CNPJ informado.

    Args:
        cnpj: CNPJ do contribuinte.
        pasta_cnpj: Caminho base do CNPJ; usa o padrão do pipeline quando None.

    Returns:
        True em caso de sucesso; False em falha.
    """
    try:
        from transformacao.calculos_anuais_pkg.calculos_anuais import (
            gerar_calculos_anuais as _gerar_ano,
        )
    except Exception as exc:
        raise RuntimeError(f"Não foi possível importar calculos_anuais: {exc}") from exc

    return _gerar_ano(cnpj, pasta_cnpj)


__all__ = [
    "gerar_movimentacao_estoque",
    "gerar_calculos_periodos",
    "gerar_calculos_mensais",
    "gerar_calculos_anuais",
]
