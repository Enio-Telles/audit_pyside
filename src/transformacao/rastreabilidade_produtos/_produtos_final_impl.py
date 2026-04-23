"""
04_produtos_final.py

Objetivo: inicializar a camada de agrupamento manual e gerar a tabela final
de produtos recalculável a partir de descricao_produtos.

Abordagem canônica:
1. agrupamento automático determinístico por descricao_normalizada;
2. persistência de de-para manual por id_descricao e/ou descricao_normalizada;
3. tabela ponte materializada por codigo_fonte, com fallback por descricao
   apenas quando a origem não fornecer codigo_fonte.

Saidas:
- produtos_agrupados_<cnpj>.parquet   (tabela mestre / MDM)
- map_produto_agrupado_<cnpj>.parquet (tabela ponte expandida)
- produtos_final_<cnpj>.parquet       (tabela final enriquecida)
"""

from __future__ import annotations

import hashlib
import re
import sys
from pathlib import Path

import polars as pl
from rich import print as rprint
import logging

# logger is used for internal debug traces; keep default handler behavior
logger = logging.getLogger(__name__)

from utilitarios.project_paths import PROJECT_ROOT

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
    from utilitarios.text import expr_normalizar_descricao
except ImportError as erro:
    rprint(f"[red]Erro ao importar modulos:[/red] {erro}")
    sys.exit(1)


# Use centralized normalization from utilitarios.text.expr_normalizar_descricao


def _gerar_id_agrupado_automatico(texto_normalizado: str | None) -> str:
    texto = (texto_normalizado or "").strip()
    digest = hashlib.sha1(texto.encode("utf-8")).hexdigest()[:12]
    return f"id_agrupado_auto_{digest}"


def _gerar_id_agrupado_automatico_expr(col: str = "descricao_normalizada") -> pl.Expr:
    return (
        pl.col(col)
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .map_elements(_gerar_id_agrupado_automatico, return_dtype=pl.Utf8)
        .alias("id_agrupado_base")
    )


def get_moda_expr(col_nome: str) -> pl.Expr:
    return (
        pl.col(col_nome)
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .filter(pl.col(col_nome).cast(pl.Utf8, strict=False).str.strip_chars() != "")
        .mode()
        .first()
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
        .filter(pl.col(col_nome).explode().cast(pl.Utf8, strict=False).str.strip_chars() != "")
        .unique()
        .sort()
        .alias(alias)
    )


def _construir_tabela_ponte(df_descricoes: pl.DataFrame) -> pl.DataFrame:
    if "lista_codigo_fonte" not in df_descricoes.columns:
        rprint(
            "[yellow]Aviso: lista_codigo_fonte ausente em descricao_produtos. "
            "A tabela ponte terá codigo_fonte=None em todas as linhas. "
            "O vínculo de fontes usará apenas descricao_normalizada (fallback).[/yellow]"
        )
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


def _registrar_auditoria_mapa_manual(
    df_manual: pl.DataFrame,
    df_descricoes: pl.DataFrame,
    pasta_analises: Path,
    cnpj: str,
) -> None:
    if df_manual.is_empty():
        return

    cols_join = []
    if "id_descricao" in df_manual.columns:
        cols_join.append("id_descricao")
    if "descricao_normalizada" in df_manual.columns:
        cols_join.append("descricao_normalizada")
    if not cols_join:
        return

    df_existentes = df_descricoes.select(
        [c for c in ["id_descricao", "descricao_normalizada"] if c in df_descricoes.columns]
    ).unique()
    df_auditoria = df_manual.join(df_existentes, on=cols_join, how="anti")
    if df_auditoria.is_empty():
        return

    salvar_para_parquet(
        df_auditoria,
        pasta_analises,
        f"auditoria_mapa_agrupamento_manual_sem_match_{cnpj}.parquet",
    )


def _aplicar_agrupamento_automatico(df_descricoes: pl.DataFrame) -> pl.DataFrame:
    # Reuse `id_agrupado_base` if already materialized in `descricao_produtos`.
    cols = df_descricoes.columns if hasattr(df_descricoes, "columns") else []
    if "id_agrupado_base" in cols:
        return df_descricoes.with_columns(
            [
                pl.lit("automatico_descricao_normalizada").alias("criterio_agrupamento"),
                pl.lit("automatico").alias("origem_agrupamento"),
                pl.col("id_agrupado_base").alias("id_agrupado"),
            ]
        )

    return df_descricoes.with_columns(
        [
            _gerar_id_agrupado_automatico_expr("descricao_normalizada"),
            pl.lit("automatico_descricao_normalizada").alias("criterio_agrupamento"),
            pl.lit("automatico").alias("origem_agrupamento"),
        ]
    ).with_columns(pl.col("id_agrupado_base").alias("id_agrupado"))


def _aplicar_agrupamento_manual(
    df_descricoes: pl.DataFrame, pasta_analises: Path, cnpj: str
) -> pl.DataFrame:
    """
    Prioriza o mapeamento manual se existir.

    O arquivo mapa_agrupamento_manual_<cnpj>.parquet pode conter:
    - [id_descricao, id_agrupado]
    - [descricao_normalizada, id_agrupado]
    - ou ambos.
    """
    caminho_manual = pasta_analises / f"mapa_agrupamento_manual_{cnpj}.parquet"
    if not caminho_manual.exists():
        return df_descricoes

    try:
        df_manual_raw = pl.read_parquet(caminho_manual)
        colunas_validas = [
            c
            for c in ["id_descricao", "descricao_normalizada", "id_agrupado"]
            if c in df_manual_raw.columns
        ]
        if "id_agrupado" not in colunas_validas:
            rprint(
                f"[yellow]Mapa manual ignorado: {caminho_manual.name} sem coluna id_agrupado.[/yellow]"
            )
            return df_descricoes

        df_manual = (
            df_manual_raw.select(colunas_validas)
            .with_columns(
                pl.col("id_agrupado").cast(pl.Utf8, strict=False).alias("id_agrupado_manual")
            )
            .drop("id_agrupado")
        )
        _registrar_auditoria_mapa_manual(df_manual, df_descricoes, pasta_analises, cnpj)

        df_result = df_descricoes
        if "id_descricao" in df_manual.columns:
            df_manual_id = (
                df_manual.filter(pl.col("id_descricao").is_not_null())
                .select(["id_descricao", "id_agrupado_manual"])
                .unique(subset=["id_descricao"], keep="last")
            )
            df_result = df_result.join(df_manual_id, on="id_descricao", how="left")
        else:
            df_result = df_result.with_columns(
                pl.lit(None, dtype=pl.Utf8).alias("id_agrupado_manual")
            )

        if "descricao_normalizada" in df_manual.columns:
            df_manual_desc = (
                df_manual.filter(pl.col("descricao_normalizada").is_not_null())
                .select(["descricao_normalizada", "id_agrupado_manual"])
                .rename({"id_agrupado_manual": "id_agrupado_manual_desc"})
                .unique(subset=["descricao_normalizada"], keep="last")
            )
            df_result = df_result.join(df_manual_desc, on="descricao_normalizada", how="left")
        else:
            df_result = df_result.with_columns(
                pl.lit(None, dtype=pl.Utf8).alias("id_agrupado_manual_desc")
            )

        return (
            df_result.with_columns(
                pl.coalesce(
                    [
                        pl.col("id_agrupado_manual"),
                        pl.col("id_agrupado_manual_desc"),
                        pl.col("id_agrupado"),
                    ]
                ).alias("id_agrupado")
            )
            .with_columns(
                [
                    pl.when(
                        pl.col("id_agrupado_manual").is_not_null()
                        | pl.col("id_agrupado_manual_desc").is_not_null()
                    )
                    .then(pl.lit("manual"))
                    .otherwise(pl.col("criterio_agrupamento"))
                    .alias("criterio_agrupamento"),
                    pl.when(
                        pl.col("id_agrupado_manual").is_not_null()
                        | pl.col("id_agrupado_manual_desc").is_not_null()
                    )
                    .then(pl.lit("manual"))
                    .otherwise(pl.col("origem_agrupamento"))
                    .alias("origem_agrupamento"),
                ]
            )
            .drop(["id_agrupado_manual", "id_agrupado_manual_desc"], strict=False)
        )
    except Exception as e:
        rprint(f"[yellow]Aviso ao carregar mapeamento manual: {e}[/yellow]")
        return df_descricoes


def _aplicar_heuristica_agrupamento(
    df_descricoes: pl.DataFrame, pasta_analises: Path, cnpj: str
) -> pl.DataFrame:
    """
    Pipeline de agrupamento.

    1. Agrupamento automático determinístico por descricao_normalizada;
    2. Aplicação do de-para manual quando existir;
    3. Preservação da chave de origem automática em id_agrupado_base.
    """
    df_descricoes = _aplicar_agrupamento_automatico(df_descricoes)
    return _aplicar_agrupamento_manual(df_descricoes, pasta_analises, cnpj)


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

    # Debugging introspection: help diagnose AttributeError like "'DataFrame' object has no attribute 'agg'"
    try:
        logger.debug("polars version: %s", getattr(pl, "__version__", "unknown"))
        logger.debug("polars module file: %s", getattr(pl, "__file__", "builtin or unknown"))
        logger.debug(
            "df_descricoes type: %s; class: %s",
            type(df_descricoes),
            getattr(df_descricoes, "__class__", None),
        )
        logger.debug(
            "df_item_unid type: %s; class: %s",
            type(df_item_unid),
            getattr(df_item_unid, "__class__", None),
        )
        for name, var in [("df_descricoes", df_descricoes), ("df_item_unid", df_item_unid)]:
            has_agg = hasattr(var, "agg")
            has_group_by = hasattr(var, "group_by")
            has_groupby = hasattr(var, "groupby")
            logger.debug(
                "%s attributes -> agg: %s, group_by: %s, groupby: %s",
                name,
                has_agg,
                has_group_by,
                has_groupby,
            )
    except Exception:
        logger.debug("introspection helper failed")

    # Defensive conversions: if these objects are pandas DataFrames or other
    # convertible structures, coerce them into polars.DataFrame to avoid
    # AttributeError when calling polars-specific methods like .group_by/.agg.
    try:
        try:
            import pandas as pd
        except Exception:
            pd = None

        if pd is not None:
            if isinstance(df_descricoes, pd.DataFrame):
                logger.debug("converting df_descricoes from pandas.DataFrame to polars.DataFrame")
                df_descricoes = pl.from_pandas(df_descricoes)
            if isinstance(df_item_unid, pd.DataFrame):
                logger.debug("converting df_item_unid from pandas.DataFrame to polars.DataFrame")
                df_item_unid = pl.from_pandas(df_item_unid)

        # Final coercion attempt for other types (dicts, lists, etc.)
        if not isinstance(df_descricoes, pl.DataFrame):
            try:
                df_descricoes = pl.DataFrame(df_descricoes)
                logger.debug("coerced df_descricoes to polars.DataFrame")
            except Exception as _e:  # pragma: no cover - surface the original error
                rprint("[red]Failed to coerce df_descricoes to polars.DataFrame.[/red]")
                raise

        if not isinstance(df_item_unid, pl.DataFrame):
            try:
                df_item_unid = pl.DataFrame(df_item_unid)
                logger.debug("coerced df_item_unid to polars.DataFrame")
            except Exception as _e:  # pragma: no cover - surface the original error
                rprint("[red]Failed to coerce df_item_unid to polars.DataFrame.[/red]")
                raise
    except Exception:
        # Re-raise so the calling code (and UI) shows the original problem
        raise

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

    df_descricoes = df_descricoes.with_columns(
        [
            pl.col(c).cast(pl.List(pl.String), strict=False)
            for c in [
                "lista_ncm",
                "lista_cest",
                "lista_gtin",
                "lista_unid",
                "lista_co_sefin",
                "lista_desc_compl",
                "lista_codigo_fonte",
                "fontes",
            ]
            if c in df_descricoes.columns
        ]
    )

    df_descricoes = _aplicar_heuristica_agrupamento(df_descricoes, pasta_analises, cnpj)

    df_item_unid_norm = df_item_unid.filter(pl.col("descricao").is_not_null()).with_columns(
        expr_normalizar_descricao("descricao").alias("descricao_normalizada_item")
    )

    df_item_com_grupo = df_item_unid_norm.join(
        df_descricoes.select(["descricao_normalizada", "id_agrupado"]).unique(
            subset=["descricao_normalizada"]
        ),
        left_on="descricao_normalizada_item",
        right_on="descricao_normalizada",
        how="left",
    )

    df_padrao = df_item_com_grupo.group_by("id_agrupado").agg(
        [
            pl.col("descricao").first().alias("descr_padrao"),
            get_moda_expr("ncm").alias("ncm_padrao"),
            get_moda_expr("cest").alias("cest_padrao"),
            get_moda_expr("gtin").alias("gtin_padrao"),
            get_moda_expr("co_sefin_item").alias("co_sefin_padrao"),
        ]
    )

    df_mestra = (
        df_descricoes.group_by("id_agrupado")
        .agg(
            [
                pl.col("id_descricao").drop_nulls().unique().sort().alias("lista_chave_produto"),
                _agrupar_lista_scalar("descricao", "lista_descricoes"),
                _agrupar_lista_scalar("descricao", "lista_itens_agrupados"),
                _agrupar_lista_lista("lista_desc_compl", "lista_desc_compl"),
                _agrupar_lista_lista("lista_ncm", "lista_ncm"),
                _agrupar_lista_lista("lista_cest", "lista_cest"),
                _agrupar_lista_lista("lista_gtin", "lista_gtin"),
                _agrupar_lista_lista("lista_co_sefin", "lista_co_sefin"),
                _agrupar_lista_lista("lista_unid", "lista_unidades"),
                _agrupar_lista_lista("fontes", "fontes"),
                pl.col("id_agrupado_base")
                .drop_nulls()
                .unique()
                .sort()
                .alias("ids_origem_agrupamento"),
                pl.when(pl.col("origem_agrupamento").cast(pl.Utf8, strict=False) == "manual")
                .then(pl.lit("manual"))
                .otherwise(pl.lit("automatico_descricao_normalizada"))
                .max()
                .alias("criterio_agrupamento"),
                pl.when(pl.col("origem_agrupamento").cast(pl.Utf8, strict=False) == "manual")
                .then(pl.lit("manual"))
                .otherwise(pl.lit("automatico"))
                .max()
                .alias("origem_agrupamento"),
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
        .with_columns(
            pl.col("lista_chave_produto").list.len().cast(pl.Int64).alias("qtd_descricoes_grupo")
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
                pl.col("criterio_agrupamento").cast(pl.String),
                pl.col("origem_agrupamento").cast(pl.String),
                pl.col("qtd_descricoes_grupo").cast(pl.Int64),
                pl.col("versao_agrupamento").cast(pl.Int64),
            ]
        )
    )

    df_ponte_agregacao = _construir_tabela_ponte(df_descricoes).select(
        [
            pl.col("chave_produto").cast(pl.String),
            pl.col("id_agrupado").cast(pl.String),
            pl.col("codigo_fonte").cast(pl.String),
            pl.col("descricao_normalizada").cast(pl.String),
        ]
    )

    ok_mestra = salvar_para_parquet(df_mestra, pasta_analises, f"produtos_agrupados_{cnpj}.parquet")
    ok_ponte = salvar_para_parquet(
        df_ponte_agregacao, pasta_analises, f"map_produto_agrupado_{cnpj}.parquet"
    )
    if not (ok_mestra and ok_ponte):
        return False

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
                "criterio_agrupamento",
                "origem_agrupamento",
            ]
        )
        .explode("lista_chave_produto")
        .rename({"lista_chave_produto": "id_descricao"})
    )

    df_final = (
        df_descricoes.join(df_mapeamento, on="id_descricao", how="left")
        .with_columns(
            [
                pl.coalesce([pl.col("descr_padrao"), pl.col("descricao")]).alias("descricao_final"),
                pl.coalesce([pl.col("ncm_padrao"), pl.col("lista_ncm").list.first()]).alias(
                    "ncm_final"
                ),
                pl.coalesce([pl.col("cest_padrao"), pl.col("lista_cest").list.first()]).alias(
                    "cest_final"
                ),
                pl.coalesce([pl.col("gtin_padrao"), pl.col("lista_gtin").list.first()]).alias(
                    "gtin_final"
                ),
                pl.coalesce(
                    [
                        pl.col("co_sefin_padrao"),
                        pl.col("lista_co_sefin_agr").list.first(),
                        pl.col("lista_co_sefin").list.first(),
                    ]
                ).alias("co_sefin_final"),
                pl.coalesce(
                    [pl.col("lista_unidades_agr").list.first(), pl.col("lista_unid").list.first()]
                ).alias("unid_ref_sugerida"),
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
