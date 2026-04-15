"""
Funções utilitárias compartilhadas para cálculos mensais e anuais.

Este módulo centraliza funções duplicadas entre os pacotes de cálculos
para evitar repetição de código e facilitar manutenção.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from utilitarios.project_paths import DADOS_DIR


def resolver_ref(nome_arquivo: str) -> Path | None:
    """
    Resolve caminho de arquivo de referência baseado em DADOS_DIR.
    
    Tenta múltiplos caminhos candidatos para compatibilidade com diferentes
    estruturas de diretórios.
    
    Args:
        nome_arquivo: Nome do arquivo a localizar
        
    Returns:
        Path do arquivo ou None se não encontrado
    """
    candidatos = [
        DADOS_DIR / "referencias" / "referencias" / "CO_SEFIN" / nome_arquivo,
        DADOS_DIR / "referencias" / "CO_SEFIN" / nome_arquivo,
        DADOS_DIR / "referencias" / nome_arquivo,
    ]
    for caminho in candidatos:
        if caminho.exists():
            return caminho
    return None


def boolish_expr(col_name: str) -> pl.Expr:
    """
    Cria expressão para converter valores textuais booleanos.
    
    Suporta múltiplos formatos: "true", "1", "s", "sim", "y", "yes", etc.
    
    Args:
        col_name: Nome da coluna a converter
        
    Returns:
        Expressão Polars que retorna Boolean
    """
    texto = (
        pl.col(col_name)
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.strip_chars()
        .str.to_lowercase()
    )
    return texto.is_in(["true", "1", "s", "sim", "y", "yes"])


def format_st_periodos(registros) -> str:
    """
    Formata lista de registros ST em string legível.
    
    Args:
        registros: Lista de dicts com campos it_in_st, vig_ini, vig_fim
        
    Returns:
        String formatada com períodos ST ou vazia
    """
    if registros is None:
        return ""
    if isinstance(registros, pl.Series):
        registros = registros.to_list()
    if not registros:
        return ""

    periodos = []
    for registro in registros:
        if not registro:
            continue
        status = str(registro.get("it_in_st") or "").strip()
        dt_ini = registro.get("vig_ini")
        dt_fim = registro.get("vig_fim")
        if not status or dt_ini is None or dt_fim is None:
            continue
        periodos.append((status, dt_ini, dt_fim))

    if not periodos:
        return ""

    periodos.sort(key=lambda item: (item[1], item[2], item[0]))
    return ";".join(
        f"['{status}' de {dt_ini.strftime('%d/%m/%Y')} ate {dt_fim.strftime('%d/%m/%Y')}]"
        for status, dt_ini, dt_fim in periodos
    )
