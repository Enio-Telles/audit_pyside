"""
schemas_agregacao.py

Schemas canônicos para as tabelas de agregação de produtos.

Centraliza a definição de colunas e tipos esperados para:
- produtos_agrupados (tabela mestre / MDM)
- map_produto_agrupado (tabela ponte)
- produtos_final (tabela final enriquecida)
- id_agrupados (resumo consolidado)
- fontes_agr (*_agr: c170, bloco_h, nfe, nfce)

Uso:
    from utilitarios.schemas_agregacao import (
        SCHEMA_PRODUTOS_AGRUPADOS,
        SCHEMA_MAP_PRODUTO_AGRUPADO,
        SCHEMA_PRODUTOS_FINAL,
        SCHEMA_ID_AGRUPADOS,
        SCHEMA_FONTES_AGR,
    )
    validar_schema(df, SCHEMA_PRODUTOS_AGRUPADOS, contexto="produtos_agrupados")
"""

from __future__ import annotations

import polars as pl


# ===========================================================================
# Tabela mestre: produtos_agrupados_{cnpj}.parquet
# ===========================================================================
SCHEMA_PRODUTOS_AGRUPADOS: dict[str, pl.DataType] = {
    "id_agrupado": pl.Utf8,
    "lista_chave_produto": pl.List(pl.Utf8),
    "descr_padrao": pl.Utf8,
    "ncm_padrao": pl.Utf8,
    "cest_padrao": pl.Utf8,
    "gtin_padrao": pl.Utf8,
    "lista_ncm": pl.List(pl.Utf8),
    "lista_cest": pl.List(pl.Utf8),
    "lista_gtin": pl.List(pl.Utf8),
    "lista_descricoes": pl.List(pl.Utf8),
    "lista_desc_compl": pl.List(pl.Utf8),
    "lista_co_sefin": pl.List(pl.Utf8),
    "co_sefin_padrao": pl.Utf8,
    "lista_unidades": pl.List(pl.Utf8),
    "co_sefin_divergentes": pl.Boolean,
    "fontes": pl.List(pl.Utf8),
    "ids_origem_agrupamento": pl.List(pl.Utf8),
    "lista_itens_agrupados": pl.List(pl.Utf8),
    "versao_agrupamento": pl.Int64,
}

# Colunas obrigatórias para validação mínima (tolerante a colunas extras)
COLUNAS_OBRIGATORIAS_AGRUPADOS: list[str] = [
    "id_agrupado",
    "lista_chave_produto",
    "descr_padrao",
    "ncm_padrao",
    "cest_padrao",
    "gtin_padrao",
    "lista_ncm",
    "lista_cest",
    "lista_gtin",
    "lista_descricoes",
    "lista_desc_compl",
    "lista_co_sefin",
    "co_sefin_padrao",
    "lista_unidades",
    "co_sefin_divergentes",
    "fontes",
]

# ===========================================================================
# Tabela ponte: map_produto_agrupado_{cnpj}.parquet
# ===========================================================================
SCHEMA_MAP_PRODUTO_AGRUPADO: dict[str, pl.DataType] = {
    "chave_produto": pl.Utf8,
    "id_agrupado": pl.Utf8,
    "codigo_fonte": pl.Utf8,
    "descricao_normalizada": pl.Utf8,
}

COLUNAS_OBRIGATORIAS_MAP: list[str] = [
    "chave_produto",
    "id_agrupado",
]

# ===========================================================================
# Tabela final: produtos_final_{cnpj}.parquet
# ===========================================================================
SCHEMA_PRODUTOS_FINAL: dict[str, pl.DataType] = {
    "id_descricao": pl.Utf8,
    "id_agrupado": pl.Utf8,
    "descricao_normalizada": pl.Utf8,
    "descricao": pl.Utf8,
    "descricao_final": pl.Utf8,
    "descr_padrao": pl.Utf8,
    "ncm_final": pl.Utf8,
    "cest_final": pl.Utf8,
    "gtin_final": pl.Utf8,
    "co_sefin_final": pl.Utf8,
    "co_sefin_padrao": pl.Utf8,
    "unid_ref_sugerida": pl.Utf8,
    "lista_desc_compl": pl.List(pl.Utf8),
    "lista_codigos": pl.List(pl.Utf8),
    "lista_unid": pl.List(pl.Utf8),
    "lista_unidades_agr": pl.List(pl.Utf8),
    "lista_co_sefin_agr": pl.List(pl.Utf8),
}

COLUNAS_OBRIGATORIAS_FINAL: list[str] = [
    "id_descricao",
    "id_agrupado",
    "descricao_normalizada",
    "descricao",
    "descricao_final",
    "descr_padrao",
    "ncm_final",
    "cest_final",
    "gtin_final",
    "co_sefin_final",
    "unid_ref_sugerida",
]

# ===========================================================================
# Resumo consolidado: id_agrupados_{cnpj}.parquet
# ===========================================================================
SCHEMA_ID_AGRUPADOS: dict[str, pl.DataType] = {
    "id_agrupado": pl.Utf8,
    "descr_padrao": pl.Utf8,
    "lista_descricoes": pl.List(pl.Utf8),
    "lista_codigos": pl.List(pl.Utf8),
    "lista_unidades": pl.List(pl.Utf8),
}

COLUNAS_OBRIGATORIAS_ID_AGRUPADOS: list[str] = [
    "id_agrupado",
    "descr_padrao",
    "lista_descricoes",
    "lista_codigos",
    "lista_unidades",
]

# ===========================================================================
# Fontes agregadas: c170_agr, bloco_h_agr, nfe_agr, nfce_agr
# ===========================================================================
COLUNAS_OBRIGATORIAS_FONTES_AGR: list[str] = [
    "id_agrupado",
    "codigo_fonte",
    "descr_padrao",
    "ncm_padrao",
    "cest_padrao",
    "co_sefin_agr",
    "unid_ref_sugerida",
]

# Colunas opcionais de rastreabilidade (preservar quando presentes)
COLUNAS_RASTREABILIDADE_FONTES: list[str] = [
    "codigo_fonte",
    "id_linha_origem",
]
