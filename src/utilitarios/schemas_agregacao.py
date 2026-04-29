"""
schemas_agregacao.py

Schemas canônicos para as tabelas de agregação de produtos.
"""

from __future__ import annotations

import polars as pl

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
    "criterio_agrupamento": pl.Utf8,
    "origem_agrupamento": pl.Utf8,
    "qtd_descricoes_grupo": pl.Int64,
    "versao_agrupamento": pl.Int64,
}

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
    "criterio_agrupamento",
    "origem_agrupamento",
]

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

SCHEMA_PRODUTOS_FINAL: dict[str, pl.DataType] = {
    "id_descricao": pl.Utf8,
    "id_agrupado": pl.Utf8,
    "id_agrupado_base": pl.Utf8,
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
    "criterio_agrupamento": pl.Utf8,
    "origem_agrupamento": pl.Utf8,
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

SCHEMA_ID_AGRUPADOS: dict[str, pl.DataType] = {
    "id_agrupado": pl.Utf8,
    "descr_padrao": pl.Utf8,
    "lista_descricoes": pl.List(pl.Utf8),
    "qtd_descricoes": pl.Int64,
    "lista_codigos": pl.List(pl.Utf8),
    "lista_unidades": pl.List(pl.Utf8),
}

COLUNAS_OBRIGATORIAS_ID_AGRUPADOS: list[str] = [
    "id_agrupado",
    "descr_padrao",
    "lista_descricoes",
    "qtd_descricoes",
    "lista_codigos",
    "lista_unidades",
]

COLUNAS_OBRIGATORIAS_FONTES_AGR: list[str] = [
    "id_agrupado",
    "codigo_fonte",
    "descr_padrao",
    "ncm_padrao",
    "cest_padrao",
    "co_sefin_agr",
    "unid_ref_sugerida",
]

COLUNAS_RASTREABILIDADE_FONTES: list[str] = [
    "codigo_fonte",
    "id_linha_origem",
]
