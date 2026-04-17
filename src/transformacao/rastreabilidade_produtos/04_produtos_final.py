"""
04_produtos_final.py

Objetivo: inicializar a camada de agrupamento manual e gerar a tabela final
de produtos recalculável a partir de descricao_produtos.

Abordagem canônica:
1. agrupamento automático por descricao_normalizada;
2. persistência de de-para manual por id_descricao -> id_agrupado;
3. tabela ponte materializada por codigo_fonte, com fallback por descricao
    apenas quando a origem não fornecer codigo_fonte.

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
except ImportError as erro:
    rprint(f"[red]Erro ao importar modulos:[/red] {erro}")
    sys.exit(1)


def _gerar_id_agrupado(seq: int) -> str:
    return f"id_agrupado_{seq}"


def get_moda_expr(col_nome: str) -> pl.Expr:
    return (
        pl.col(col_nome)
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .filter(pl.col(col_nome).cast(pl.Utf8, strict=False).str.strip_chars() != "")
        .mode()
        .first()
    )


def _limpar_lista_expr(col_nome: str) -> pl.Expr:
    return (
        pl.col(col_nome)
        .list.eval(
            pl.element()
            .cast(pl.Utf8, strict=False)
            .str.strip_chars()
            .filter(pl.element().is_not_null() & (pl.element().str.strip_chars() != ""))
        )
        .list.unique()
        .list.sort()
    )


def _agrupar_lista_scalar(col_nome: str, alias: str) -> pl.Expr:
    return (
        pl.col(col_nome)
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .drop_nulls()
        .filter(pl.col(col_nome).cast(pl.Utf8, strict=False).str.strip_chars() != "")
        .unique()
        .sort()
        .alias(alias)
    )


def _agrupar_lista_lista(col_nome: str, alias: str) -> pl.Expr:
    return (
        pl.col(col_nome)
        .explode()
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .drop_nulls()
        .unique()
        .sort()
        .alias(alias)
    )


def _construir_tabela_ponte(df_descricoes: pl.DataFrame) -> pl.DataFrame:
    if "lista_codigo_fonte" not in df_descricoes.columns:
        return df_descricoes.select(
            [
                pl.col("id_descricao").alias("chave_produto"),
                "id_agrupado",
                pl.lit(None, dtype=pl.Utf8).alias("codigo_fonte"),
                "descricao_normalizada",
            ]
        ).unique()

    return (
        df_descricoes.select(
            [
                pl.col("id_descricao").alias("chave_produto"),
                "id_agrupado",
                "descricao_normalizada",
                pl.col("lista_codigo_fonte").alias("codigo_fonte"),
            ]
        )
        .explode("codigo_fonte")
        .with_columns(pl.col("codigo_fonte").cast(pl.Utf8, strict=False))
        .unique()
    )


def _aplicar_agrupamento_manual(
    df_descricoes: pl.DataFrame, pasta_analises: Path, cnpj: str
) -> pl.DataFrame:
    """
    Prioriza o mapeamento manual se existir.
    O arquivo mapa_agrupamento_manual_<cnpj>.parquet deve conter [id_descricao, id_agrupado].
    """
    caminho_manual = pasta_analises / f"mapa_agrupamento_manual_{cnpj}.parquet"

    if not caminho_manual.exists():
        # Fallback: 1 id_agrupado para cada id_descricao (Agrupamento por Descrição apenas)
        return df_descricoes.with_columns(pl.col("id_descricao").alias("id_agrupado"))

    try:
        df_manual = pl.read_parquet(caminho_manual).select(
            ["id_descricao", "id_agrupado"]
        )
        rprint(f"[green]Mapeamento manual carregado: {caminho_manual.name}[/green]")

        return df_descricoes.join(
            df_manual, on="id_descricao", how="left"
        ).with_columns(
            pl.coalesce([pl.col("id_agrupado"), pl.col("id_descricao")]).alias(
                "id_agrupado"
            )
        )
    except Exception as e:
        rprint(f"[yellow]Aviso ao carregar mapeamento manual: {e}[/yellow]")
        return df_descricoes.with_columns(pl.col("id_descricao").alias("id_agrupado"))


def _aplicar_heuristica_agrupamento(
    df_descricoes: pl.DataFrame, pasta_analises: Path, cnpj: str
) -> pl.DataFrame:
    """
    Pipeline de agrupamento. Seguindo a nova regra:
    1. Agrupamento por descricao_normalizada (ja eh a base do id_descricao).
    2. Aplicacao de DE-PARA manual se existir.
    """
    return _aplicar_agrupamento_manual(df_descricoes, pasta_analises, cnpj)


# ===========================================================================
# Funcao principal
# ===========================================================================


def produtos_agrupados(
    cnpj: str, pasta_cnpj: Path | None = None, versao: int = 1
) -> bool:
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

    rprint(
        f"[bold cyan]Gerando produtos_agrupados/final para CNPJ: {cnpj} (v{versao})[/bold cyan]"
    )

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

    for col in [
        "lista_unid",
        "fontes",
        "lista_co_sefin",
        "lista_id_item_unid",
        "lista_id_item",
        "lista_ncm",
        "lista_cest",
        "lista_gtin",
        "lista_desc_compl",
        "lista_codigo_fonte",
    ]:
        if col not in df_descricoes.columns:
            df_descricoes = df_descricoes.with_columns(
                pl.lit([]).cast(pl.List(pl.String)).alias(col)
            )

    # Cast any List(Null) columns to List(String)
    _list_str_cols = [
        "lista_ncm",
        "lista_cest",
        "lista_gtin",
        "lista_unid",
        "lista_co_sefin",
        "lista_desc_compl",
        "lista_codigo_fonte",
    ]
    df_descricoes = df_descricoes.with_columns(
        [
            pl.col(c).cast(pl.List(pl.String), strict=False)
            for c in _list_str_cols
            if c in df_descricoes.columns
        ]
    )

    # ===========================================================================
    # A1: Aplicar sistemática de agrupamento (Descrição + Mapeamento Manual)
    # ===========================================================================
    df_descricoes = _aplicar_heuristica_agrupamento(df_descricoes, pasta_analises, cnpj)

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
    df_item_unid_norm = df_item_unid.filter(
        pl.col("descricao").is_not_null()
    ).with_columns(
        pl.col("descricao")
        .cast(pl.Utf8, strict=False)
        .str.to_uppercase()
        .str.replace_all(r"\s+", " ")
        .alias("__descricao_upper")
    )

    # Join item_unidades com descricoes para obter id_agrupado de cada item
    df_item_com_grupo = df_item_unid_norm.join(
        df_descricoes.select(["descricao_normalizada", "id_agrupado"]).unique(
            subset=["descricao_normalizada"]
        ),
        left_on="__descricao_upper",
        right_on="descricao_normalizada",
        how="left",
    )

    # Atributos padrao por id_agrupado
    df_padrao = df_item_com_grupo.group_by("id_agrupado").agg(
        [
            pl.col("descricao").first().alias("descr_padrao"),
            get_moda_expr("ncm").alias("ncm_padrao"),
            get_moda_expr("cest").alias("cest_padrao"),
            get_moda_expr("gtin").alias("gtin_padrao"),
            get_moda_expr("co_sefin_item").alias("co_sefin_padrao"),
        ]
    )

    # ===========================================================================
    # Construir tabela mestre
    # ===========================================================================
    df_mestra = (
        df_descricoes.group_by("id_agrupado")
        .agg(
            [
                pl.col("id_descricao")
                .drop_nulls()
                .unique()
                .sort()
                .alias("lista_chave_produto"),
                _agrupar_lista_scalar("descricao", "lista_descricoes"),
                _agrupar_lista_scalar("descricao", "lista_itens_agrupados"),
                _agrupar_lista_lista("lista_desc_compl", "lista_desc_compl"),
                _agrupar_lista_lista("lista_ncm", "lista_ncm"),
                _agrupar_lista_lista("lista_cest", "lista_cest"),
                _agrupar_lista_lista("lista_gtin", "lista_gtin"),
                _agrupar_lista_lista("lista_co_sefin", "lista_co_sefin"),
                _agrupar_lista_lista("lista_unid", "lista_unidades"),
                _agrupar_lista_lista("fontes", "fontes"),
                pl.col("id_descricao")
                .drop_nulls()
                .unique()
                .sort()
                .alias("ids_origem_agrupamento"),
            ]
        )
        .join(df_padrao, on="id_agrupado", how="left")
        .with_columns(
            [
                pl.coalesce(
                    [pl.col("descr_padrao"), pl.col("lista_descricoes").list.first()]
                ).alias("descr_padrao"),
                pl.lit(versao).cast(pl.Int64).alias("versao_agrupamento"),
                (pl.col("lista_co_sefin").list.len() > 1).alias("co_sefin_divergentes"),
            ]
        )
        .select(
            [
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
            ]
        )
    )

    # ===========================================================================
    # R3: Tabela ponte expandida (codigo_fonte + descricao_normalizada)
    # ===========================================================================
    df_ponte_agregacao = _construir_tabela_ponte(df_descricoes).select(
        [
            pl.col("chave_produto").cast(pl.String),
            pl.col("id_agrupado").cast(pl.String),
            pl.col("codigo_fonte").cast(pl.String),
            pl.col("descricao_normalizada").cast(pl.String),
        ]
    )

    # ===========================================================================
    # Salvar tabela mestre e ponte
    # ===========================================================================
    ok_mestra = salvar_para_parquet(
        df_mestra, pasta_analises, f"produtos_agrupados_{cnpj}.parquet"
    )
    ok_ponte = salvar_para_parquet(
        df_ponte_agregacao, pasta_analises, f"map_produto_agrupado_{cnpj}.parquet"
    )
    if not (ok_mestra and ok_ponte):
        return False

    # ===========================================================================
    # Gerar produtos_final
    # ===========================================================================
    df_mapeamento = (
        df_mestra.select(
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
        df_descricoes.join(df_mapeamento, on="id_descricao", how="left")
        .with_columns(
            [
                pl.coalesce([pl.col("descr_padrao"), pl.col("descricao")]).alias(
                    "descricao_final"
                ),
                pl.coalesce(
                    [pl.col("ncm_padrao"), pl.col("lista_ncm").list.first()]
                ).alias("ncm_final"),
                pl.coalesce(
                    [pl.col("cest_padrao"), pl.col("lista_cest").list.first()]
                ).alias("cest_final"),
                pl.coalesce(
                    [pl.col("gtin_padrao"), pl.col("lista_gtin").list.first()]
                ).alias("gtin_final"),
                pl.coalesce(
                    [
                        pl.col("co_sefin_padrao"),
                        pl.col("lista_co_sefin_agr").list.first(),
                        pl.col("lista_co_sefin").list.first(),
                    ]
                ).alias("co_sefin_final"),
                pl.coalesce(
                    [
                        pl.col("lista_unidades_agr").list.first(),
                        pl.col("lista_unid").list.first(),
                    ]
                ).alias("unid_ref_sugerida"),
            ]
        )
        .sort(["id_agrupado", "id_descricao"], nulls_last=True)
    )

    ok_final = salvar_para_parquet(
        df_final, pasta_analises, f"produtos_final_{cnpj}.parquet"
    )
    if not ok_final:
        return False
    return gerar_id_agrupados(cnpj, pasta_cnpj)


def gerar_produtos_final(
    cnpj: str, pasta_cnpj: Path | None = None, versao: int = 1
) -> bool:
    return produtos_agrupados(cnpj, pasta_cnpj, versao=versao)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        produtos_agrupados(sys.argv[1])
    else:
        produtos_agrupados(input("CNPJ: "))
