"""
04_produtos_final.py

Objetivo: inicializar a camada de agrupamento manual e gerar a tabela final
de produtos recalculavel a partir de descricao_produtos.

Heuristica de agrupamento automatico (vetorizada):
1. GTIN comum entre produtos -> mesmo id_agrupado
2. descricao_normalizada igual com intersecao de NCM -> mesmo id_agrupado
3. Fallback: descricao_normalizada igual sem NCM -> mesmo id_agrupado

Saidas:
- produtos_agrupados_<cnpj>.parquet   (tabela mestre / MDM)
- map_produto_agrupado_<cnpj>.parquet  (tabela ponte expandida)
- produtos_final_<cnpj>.parquet        (tabela final enriquecida)
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from utilitarios.project_paths import PROJECT_ROOT

import polars as pl
from rich import print as rprint

ROOT_DIR = PROJECT_ROOT
SRC_DIR = ROOT_DIR / "src"
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"


try:
    from utilitarios.salvar_para_parquet import salvar_para_parquet
    from utilitarios.validacao_schema import (
        SchemaValidacaoError,
        validar_parquet_essencial,
    )
    from transformacao.descricao_produtos import descricao_produtos
    from transformacao.id_agrupados import gerar_id_agrupados
except ImportError as e:
    rprint(f"[red]Erro ao importar modulos:[/red] {e}")
    sys.exit(1)


def _gerar_id_agrupado(seq: int) -> str:
    return f"id_agrupado_{seq}"


def get_mode_expr(col_name: str) -> pl.Expr:
    return (
        pl.col(col_name)
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .filter(pl.col(col_name).cast(pl.Utf8, strict=False).str.strip_chars() != "")
        .mode()
        .first()
    )


def _clean_list_expr(col_name: str) -> pl.Expr:
    return (
        pl.col(col_name)
        .list.eval(
            pl.element()
            .cast(pl.Utf8, strict=False)
            .str.strip_chars()
            .filter(pl.element().is_not_null() & (pl.element().str.strip_chars() != ""))
        )
        .list.unique()
        .list.sort()
    )


# ===========================================================================
# Heuristica de agrupamento automatico (A1) — vetorizada
# ===========================================================================

def _agrupar_por_gtin(df_descricoes: pl.DataFrame) -> pl.DataFrame:
    """
    Trilha 1: Produtos que compartilham GTIN sao agrupados no mesmo grupo.

    Retorna df_descricoes com coluna `id_agrupado_gtin` onde GTINs comuns
    recebem o mesmo ID. Descricoes sem GTIN ficam com null.
    """
    # Explode lista_gtin para ter uma linha por (id_descricao, gtin)
    df_com_gtin = (
        df_descricoes
        .filter(pl.col("lista_gtin").list.len() > 0)
        .select(["id_descricao", "descricao_normalizada", "lista_gtin"])
        .explode("lista_gtin")
        .rename({"lista_gtin": "__gtin__"})
    )

    if df_com_gtin.is_empty():
        return df_descricoes.with_columns(pl.lit(None, dtype=pl.Utf8).alias("id_agrupado_gtin"))

    # Agrupar por GTIN: todos os id_descricao com mesmo GTIN recebem grupo
    df_gtin_groups = (
        df_com_gtin
        .group_by("__gtin__")
        .agg(pl.col("id_descricao").alias("__ids_com_gtin__"))
    )

    # Conectar grupos transitivos: se A e B compartilham GTIN1, B e C compartilham GTIN2,
    # entao A, B, C estao no mesmo grupo. Fazemos via explode + group_by id_descricao.
    df_ids_gtin = (
        df_gtin_groups
        .explode("__ids_com_gtin__")
        .rename({"__ids_com_gtin__": "id_descricao"})
        .group_by("id_descricao")
        .agg(pl.col("__gtin__").sort().alias("__gtins_do_item__"))
    )

    # Para simplificar sem union-find, usamos o menor GTIN como chave do grupo
    # (grupos transitivos perfeitos exigiriam union-find iterativo; na pratica,
    # GTIN duplicado direto e o caso mais comum).
    df_com_gtin = df_com_gtin.join(
        df_ids_gtin, on="id_descricao", how="left"
    ).with_columns(
        pl.col("__gtins_do_item__").list.first().alias("__gtin_repr__")
    )

    # Atribuir id_agrupado por GTIN repr
    df_gtin_seq = (
        df_com_gtin
        .select("__gtin_repr__")
        .unique()
        .sort("__gtin_repr__")
        .with_row_index("seq_gtin", offset=1)
        .with_columns(pl.format("id_agrupado_gt_{}", pl.col("seq_gtin")).alias("id_agrupado_gtin"))
        .drop("seq_gtin")
    )

    df_com_gtin = df_com_gtin.join(df_gtin_seq, on="__gtin_repr__", how="left").select([
        "id_descricao", pl.col("id_agrupado_gtin")
    ])

    return df_descricoes.join(df_com_gtin, on="id_descricao", how="left")


def _agrupar_por_descricao_ncm(df: pl.DataFrame) -> pl.DataFrame:
    """
    Trilha 2+3: Descricao_normalizada igual com intersecao de NCM -> grupo.
    Fallback: descricao_normalizada igual sem NCM -> grupo.

    Retorna df com coluna `id_agrupado_desc_ncm`.
    """
    # Para cada descricao_normalizada, verificar se ha NCM comum
    df_com_ncm = (
        df
        .filter(pl.col("lista_ncm").list.len() > 0)
        .select(["id_descricao", "descricao_normalizada", "lista_ncm"])
        .explode("lista_ncm")
        .rename({"lista_ncm": "__ncm__"})
    )

    if df_com_ncm.is_empty():
        # Sem NCM em nenhum: agrupar tudo por descricao_normalizacao
        return df.with_columns(
            pl.format("id_agrupado_dn_{}", pl.col("id_descricao")).alias("id_agrupado_desc_ncm")
        )

    # Descricoes com NCM: agrupar por (descricao_normalizada, ncm)
    df_desc_ncm_groups = (
        df_com_ncm
        .group_by(["descricao_normalizada", "__ncm__"])
        .agg(pl.col("id_descricao").alias("__ids__"))
    )

    # Atribuir ID sequencial por grupo
    df_desc_ncm_groups = df_desc_ncm_groups.with_row_index("seq_ncm", offset=1).with_columns(
        pl.format("id_agrupado_dn_{}", pl.col("seq_ncm")).alias("id_agrupado_desc_ncm")
    )

    # Explode ids e juntar de volta
    df_ids_ncm = (
        df_desc_ncm_groups
        .select(["__ids__", "id_agrupado_desc_ncm"])
        .explode("__ids__")
        .rename({"__ids__": "id_descricao"})
    )

    # Descricoes SEM NCM: cada descricao_normalizada vira seu proprio grupo (agrupado por descricao)
    offset_sem_ncm = df_desc_ncm_groups.height + 1
    
    df_sem_ncm_base = (
        df
        .filter(pl.col("lista_ncm").list.len() == 0)
        .select(["id_descricao", "descricao_normalizada"])
    )
    
    if df_sem_ncm_base.is_empty():
        df_result = df_ids_ncm
    else:
        df_sem_ncm = (
            df_sem_ncm_base
            .group_by("descricao_normalizada")
            .agg(pl.col("id_descricao").alias("__ids__"))
            .with_row_index("seq_sem_ncm", offset=offset_sem_ncm)
            .with_columns(pl.format("id_agrupado_dn_{}", pl.col("seq_sem_ncm")).alias("id_agrupado_desc_ncm"))
            .explode("__ids__")
            .rename({"__ids__": "id_descricao"})
            .select(["id_descricao", "id_agrupado_desc_ncm"])
        )
        df_result = pl.concat([df_ids_ncm, df_sem_ncm])

    return df.join(df_result, on="id_descricao", how="left")


def _fundir_grupos(df: pl.DataFrame) -> pl.DataFrame:
    """
    Fundir as trilhas GTIN e descricao+NCM em um unico `id_agrupado`.

    Estrategia: para cada id_descricao, se ha id_agrupado_gtin, usar como base.
    Depois, conectar grupos de descricao+NCM que compartilham id_agrupado_gtin.
    Para simplicidade vetorizada:
    - Se tem GTIN: id_agrupado = id_agrupado_gtin
    - Se nao tem GTIN: id_agrupado = id_agrupado_desc_ncm
    """
    return df.with_columns(
        pl.coalesce([pl.col("id_agrupado_gtin"), pl.col("id_agrupado_desc_ncm")]).alias("id_agrupado")
    ).drop(["id_agrupado_gtin", "id_agrupado_desc_ncm"])


def _aplicar_heuristica_agrupamento(df_descricoes: pl.DataFrame) -> pl.DataFrame:
    """Pipeline completo de agrupamento automatico vetorizado."""
    df = _agrupar_por_gtin(df_descricoes)
    df = _agrupar_por_descricao_ncm(df)
    df = _fundir_grupos(df)
    return df


# ===========================================================================
# Funcao principal
# ===========================================================================

def produtos_agrupados(cnpj: str, pasta_cnpj: Path | None = None, versao: int = 1) -> bool:
    """
    Gera produtos_agrupados, map_produto_agrupado e produtos_final.

    Parametros:
        cnpj: CNPJ valido (11 ou 14 digitos).
        pasta_cnpj: caminho da pasta do CNPJ (default: CNPJ_ROOT/<cnpj>).
        versao: versao do agrupamento (incrementar a cada reprocessamento manual).
    """
    cnpj = re.sub(r"\D", "", cnpj or "")
    if len(cnpj) not in {11, 14}:
        raise ValueError("CPF/CNPJ invalido.")

    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj

    pasta_analises = pasta_cnpj / "analises" / "produtos"
    arq_descricoes = pasta_analises / f"descricao_produtos_{cnpj}.parquet"
    arq_item_unid = pasta_analises / f"item_unidades_{cnpj}.parquet"

    if not arq_descricoes.exists():
        rprint("[yellow]descricao_produtos nao encontrado. Gerando base...[/yellow]")
        if not descricao_produtos(cnpj, pasta_cnpj):
            return False

    if not arq_descricoes.exists() or not arq_item_unid.exists():
        rprint("[red]Arquivos base para agrupamento final nao encontrados.[/red]")
        return False

    rprint(f"[bold cyan]Gerando produtos_agrupados/final para CNPJ: {cnpj} (v{versao})[/bold cyan]")

    try:
        schema_descricoes = validar_parquet_essencial(
            arq_descricoes,
            ["id_descricao", "descricao_normalizada", "descricao"],
            contexto="produtos_final/descricao_produtos",
        )
        validar_parquet_essencial(
            arq_item_unid,
            ["descricao", "ncm", "cest", "gtin", "co_sefin_item"],
            contexto="produtos_final/item_unidades",
        )
    except SchemaValidacaoError as exc:
        rprint(f"[red]{exc}[/red]")
        return False

    df_descricoes = pl.scan_parquet(arq_descricoes).select(schema_descricoes).collect()
    df_item_unid = (
        pl.scan_parquet(arq_item_unid)
        .select(["descricao", "ncm", "cest", "gtin", "co_sefin_item"])
        .collect()
    )

    if df_descricoes.is_empty():
        rprint("[yellow]descricao_produtos esta vazio.[/yellow]")
        return False

    for col in ["lista_unid", "fontes", "lista_co_sefin", "lista_id_item_unid", "lista_id_item",
                  "lista_ncm", "lista_cest", "lista_gtin", "lista_desc_compl"]:
        if col not in df_descricoes.columns:
            df_descricoes = df_descricoes.with_columns(pl.lit([]).cast(pl.List(pl.String)).alias(col))

    # Cast any List(Null) columns to List(String)
    _list_str_cols = ["lista_ncm", "lista_cest", "lista_gtin", "lista_unid", "lista_co_sefin", "lista_desc_compl"]
    df_descricoes = df_descricoes.with_columns([
        pl.col(c).cast(pl.List(pl.String), strict=False)
        for c in _list_str_cols
        if c in df_descricoes.columns
    ])

    # ===========================================================================
    # A1: Aplicar heuristica de agrupamento automatico (GTIN + descricao+NCM)
    # ===========================================================================
    df_descricoes = _aplicar_heuristica_agrupamento(df_descricoes)

    # Garantir que todos tem id_agrupado (fallback por id_descricao se heuristicas falharem)
    df_descricoes = df_descricoes.with_columns(
        pl.when(pl.col("id_agrupado").is_null())
        .then(pl.format("id_agrupado_{}", pl.col("id_descricao")))
        .otherwise(pl.col("id_agrupado"))
        .alias("id_agrupado")
    )

    # ===========================================================================
    # Calcular atributos padrao por id_agrupado (nao mais por descricao_normalizada)
    # ===========================================================================
    df_item_unid_norm = (
        df_item_unid
        .filter(pl.col("descricao").is_not_null())
        .with_columns(
            pl.col("descricao")
            .cast(pl.Utf8, strict=False)
            .str.to_uppercase()
            .str.replace_all(r"\s+", " ")
            .alias("__descricao_upper")
        )
    )

    # Join item_unidades com descricoes para obter id_agrupado de cada item
    df_item_com_grupo = (
        df_item_unid_norm
        .join(
            df_descricoes.select(["descricao_normalizada", "id_agrupado"]).unique(subset=["descricao_normalizada"]),
            left_on="__descricao_upper",
            right_on="descricao_normalizada",
            how="left"
        )
    )

    # Atributos padrao por id_agrupado
    df_padrao = (
        df_item_com_grupo
        .group_by("id_agrupado")
        .agg([
            pl.col("descricao").first().alias("descr_padrao"),
            get_mode_expr("ncm").alias("ncm_padrao"),
            get_mode_expr("cest").alias("cest_padrao"),
            get_mode_expr("gtin").alias("gtin_padrao"),
            get_mode_expr("co_sefin_item").alias("co_sefin_padrao")
        ])
    )

    # ===========================================================================
    # Construir tabela mestre
    # ===========================================================================
    df_mestra_base = df_descricoes.join(
        df_padrao, on="id_agrupado", how="left"
    )

    df_mestra = df_mestra_base.with_columns([
        pl.coalesce([pl.col("descr_padrao"), pl.col("descricao")]).alias("descr_padrao"),
        _clean_list_expr("lista_ncm").alias("lista_ncm"),
        _clean_list_expr("lista_cest").alias("lista_cest"),
        _clean_list_expr("lista_gtin").alias("lista_gtin"),
        _clean_list_expr("lista_co_sefin").alias("lista_co_sefin"),
        _clean_list_expr("lista_unid").alias("lista_unidades"),
        _clean_list_expr("fontes").alias("fontes"),

        pl.col("descricao").cast(pl.Utf8, strict=False).str.strip_chars()
          .filter(pl.col("descricao").cast(pl.Utf8, strict=False).str.strip_chars() != "")
          .map_elements(lambda x: [x] if x else [], return_dtype=pl.List(pl.Utf8)).alias("lista_descricoes"),

        _clean_list_expr("lista_desc_compl").alias("lista_desc_compl"),

        # Rastreabilidade: IDs de origem do agrupamento automatico
        pl.concat_str([pl.col("id_agrupado")]).cast(pl.List(pl.Utf8)).alias("ids_origem_agrupamento"),
        pl.col("descricao").cast(pl.Utf8, strict=False).str.strip_chars()
          .filter(pl.col("descricao").cast(pl.Utf8, strict=False).str.strip_chars() != "")
          .map_elements(lambda x: [x] if x else [], return_dtype=pl.List(pl.Utf8)).alias("lista_itens_agrupados"),

        # M3: versao do agrupamento
        pl.lit(versao).cast(pl.Int64).alias("versao_agrupamento"),
    ]).with_columns([
        (pl.col("lista_co_sefin").list.len() > 1).alias("co_sefin_divergentes")
    ])

    # Agregar lista_chave_produto por id_agrupado
    df_mestra_chaves = (
        df_mestra
        .group_by("id_agrupado")
        .agg([
            pl.col("id_descricao").alias("lista_chave_produto"),
            pl.col("descr_padrao").first().alias("descr_padrao"),
            pl.col("ncm_padrao").first().alias("ncm_padrao"),
            pl.col("cest_padrao").first().alias("cest_padrao"),
            pl.col("gtin_padrao").first().alias("gtin_padrao"),
            pl.col("lista_ncm").first().alias("lista_ncm"),
            pl.col("lista_cest").first().alias("lista_cest"),
            pl.col("lista_gtin").first().alias("lista_gtin"),
            pl.col("lista_descricoes").first().alias("lista_descricoes"),
            pl.col("lista_desc_compl").first().alias("lista_desc_compl"),
            pl.col("lista_co_sefin").first().alias("lista_co_sefin"),
            pl.col("co_sefin_padrao").first().alias("co_sefin_padrao"),
            pl.col("lista_unidades").first().alias("lista_unidades"),
            pl.col("co_sefin_divergentes").first().alias("co_sefin_divergentes"),
            pl.col("fontes").first().alias("fontes"),
            pl.col("ids_origem_agrupamento").first().alias("ids_origem_agrupamento"),
            pl.col("lista_itens_agrupados").first().alias("lista_itens_agrupados"),
            pl.col("versao_agrupamento").first().alias("versao_agrupamento"),
        ])
    )

    df_mestra = df_mestra_chaves.select([
        pl.col("id_agrupado").cast(pl.String),
        pl.col("lista_chave_produto").cast(pl.List(pl.String)),
        pl.col("descr_padrao").cast(pl.String),
        pl.col("ncm_padrao").cast(pl.String),
        pl.col("cest_padrao").cast(pl.String),
        pl.col("gtin_padrao").cast(pl.String),
        pl.col("lista_ncm").cast(pl.List(pl.String)),
        pl.col("lista_cest").cast(pl.List(pl.String)),
        pl.col("lista_gtin").cast(pl.List(pl.String)),
        pl.col("lista_descricoes").cast(pl.List(pl.String)),
        pl.col("lista_desc_compl").cast(pl.List(pl.String)),
        pl.col("lista_co_sefin").cast(pl.List(pl.String)),
        pl.col("co_sefin_padrao").cast(pl.String),
        pl.col("lista_unidades").cast(pl.List(pl.String)),
        pl.col("co_sefin_divergentes").cast(pl.Boolean),
        pl.col("fontes").cast(pl.List(pl.String)),
        pl.col("ids_origem_agrupamento").cast(pl.List(pl.String)),
        pl.col("lista_itens_agrupados").cast(pl.List(pl.String)),
        pl.col("versao_agrupamento").cast(pl.Int64),
    ])

    # ===========================================================================
    # R3: Tabela ponte expandida (codigo_fonte + descricao_normalizada)
    # ===========================================================================
    df_ponte = (
        df_descricoes
        .filter(pl.col("id_descricao").is_not_null())
        .select([
            pl.col("id_descricao").alias("chave_produto"),
            "id_agrupado",
            "descricao_normalizada",
        ])
    )

    # Tentar incluir codigo_fonte se disponivel em item_unidades
    if "codigo_fonte" in df_item_unid.columns:
        df_cf = df_item_unid.select(["descricao", "codigo_fonte"]).unique(subset=["descricao"])
        df_ponte = df_ponte.join(
            df_cf.rename({"descricao": "descr_ref"}),
            left_on="descricao_normalizada",
            right_on=df_cf.select(pl.col("descr_ref").cast(pl.Utf8, strict=False).str.to_uppercase().str.replace_all(r"\s+", " ").alias("descricao_normalizada_tmp")).get_column("descricao_normalizada_tmp"),
            how="left"
        )
        # Simplificacao: incluir codigo_fonte via join por descricao_normalizada
        df_cf_norm = df_item_unid.with_columns(
            pl.col("descricao").cast(pl.Utf8, strict=False).str.to_uppercase().str.replace_all(r"\s+", " ").alias("__dn__")
        ).select([pl.col("__dn__").alias("descricao_normalizada"), "codigo_fonte"]).unique(subset=["descricao_normalizada"])
        df_ponte = df_ponte.join(df_cf_norm, on="descricao_normalizada", how="left")
    else:
        df_ponte = df_ponte.with_columns(pl.lit(None, dtype=pl.Utf8).alias("codigo_fonte"))

    df_ponte = df_ponte.select([
        pl.col("chave_produto").cast(pl.String),
        pl.col("id_agrupado").cast(pl.String),
        pl.col("codigo_fonte").cast(pl.String),
        pl.col("descricao_normalizada").cast(pl.String),
    ])

    # ===========================================================================
    # Salvar tabela mestre e ponte
    # ===========================================================================
    ok_mestra = salvar_para_parquet(df_mestra, pasta_analises, f"produtos_agrupados_{cnpj}.parquet")
    ok_ponte = salvar_para_parquet(df_ponte, pasta_analises, f"map_produto_agrupado_{cnpj}.parquet")
    if not (ok_mestra and ok_ponte):
        return False

    # ===========================================================================
    # Gerar produtos_final
    # ===========================================================================
    df_map = (
        df_mestra
        .select(
            [
                "id_agrupado",
                "lista_chave_produto",
                "descr_padrao",
                "ncm_padrao",
                "cest_padrao",
                "gtin_padrao",
                pl.col("lista_co_sefin").alias("lista_co_sefin_agr"),
                "co_sefin_padrao",
                pl.col("lista_unidades").alias("lista_unidades_agr"),
                "co_sefin_divergentes",
                pl.col("fontes").alias("fontes_agr"),
                "versao_agrupamento",
            ]
        )
        .explode("lista_chave_produto")
        .rename({"lista_chave_produto": "id_descricao"})
    )

    df_final = (
        df_descricoes
        .join(df_map, on="id_descricao", how="left")
        .with_columns(
            [
                pl.coalesce([pl.col("descr_padrao"), pl.col("descricao")]).alias("descricao_final"),
                pl.coalesce([pl.col("ncm_padrao"), pl.col("lista_ncm").list.first()]).alias("ncm_final"),
                pl.coalesce([pl.col("cest_padrao"), pl.col("lista_cest").list.first()]).alias("cest_final"),
                pl.coalesce([pl.col("gtin_padrao"), pl.col("lista_gtin").list.first()]).alias("gtin_final"),
                pl.coalesce(
                    [
                        pl.col("co_sefin_padrao"),
                        pl.col("lista_co_sefin_agr").list.first(),
                        pl.col("lista_co_sefin").list.first(),
                    ]
                ).alias("co_sefin_final"),
                pl.coalesce([pl.col("lista_unidades_agr").list.first(), pl.col("lista_unid").list.first()]).alias("unid_ref_sugerida"),
            ]
        )
        .sort(["id_agrupado", "id_descricao"], nulls_last=True)
    )

    ok_final = salvar_para_parquet(df_final, pasta_analises, f"produtos_final_{cnpj}.parquet")
    if not ok_final:
        return False
    return gerar_id_agrupados(cnpj, pasta_cnpj)


def gerar_produtos_final(cnpj: str, pasta_cnpj: Path | None = None, versao: int = 1) -> bool:
    return produtos_agrupados(cnpj, pasta_cnpj, versao=versao)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        produtos_agrupados(sys.argv[1])
    else:
        produtos_agrupados(input("CNPJ: "))
