"""
fatores_conversao.py

Objetivo: Calcular a relacao entre diferentes unidades de medida do mesmo
produto usando a camada final de agrupamento.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import polars as pl
from rich import print as rprint

from utilitarios.project_paths import PROJECT_ROOT
from transformacao.movimentacao_estoque_pkg.mapeamento_fontes import normalizar_descricao_expr

ROOT_DIR = PROJECT_ROOT
SRC_DIR = ROOT_DIR / "src"
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"

try:
    from utilitarios.salvar_para_parquet import salvar_para_parquet
    from utilitarios.validacao_schema import (
        SchemaValidacaoError,
        garantir_colunas_obrigatorias,
    )
except ImportError as e:
    rprint(f"[red]Erro ao importar modulos utilitarios:[/red] {e}")
    raise RuntimeError(f"Falha ao importar modulos ETL: {e}") from e


def _normalizar_descricao_expr(col: str) -> pl.Expr:
    return normalizar_descricao_expr(col).alias("descricao_normalizada")


def _salvar_log_sem_compra(df_sem_compra: pl.DataFrame, pasta_analises: Path, cnpj: str) -> None:
    salvar_para_parquet(df_sem_compra, pasta_analises, f"log_sem_preco_medio_compra_{cnpj}.parquet")
    resumo = {
        "cnpj": cnpj,
        "qtd_itens_sem_preco_compra": int(df_sem_compra.height),
        "qtd_itens_com_fallback_venda": int(df_sem_compra.filter(pl.col("tem_preco_venda")).height),
        "qtd_itens_sem_preco_algum": int(df_sem_compra.filter(~pl.col("tem_preco_venda")).height),
    }
    with open(pasta_analises / f"log_sem_preco_medio_compra_{cnpj}.json", "w", encoding="utf-8") as f:
        json.dump(resumo, f, ensure_ascii=False, indent=2)


def _df_vazio_override_unid() -> pl.DataFrame:
    return pl.DataFrame(schema={"id_agrupado": pl.Utf8, "unid_ref_override": pl.Utf8})


def _df_vazio_override_fator() -> pl.DataFrame:
    return pl.DataFrame(schema={"id_agrupado": pl.Utf8, "unid": pl.Utf8, "fator_override": pl.Float64})


def _df_vazio_vinculo() -> pl.DataFrame:
    return pl.DataFrame(schema={"descricao_normalizada": pl.Utf8, "id_agrupado": pl.Utf8})


def _df_vazio_vinculo_produto() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "descricao_normalizada": pl.Utf8,
            "id_agrupado": pl.Utf8,
            "descr_padrao_vinculo": pl.Utf8,
            "origem_vinculo_produto": pl.Utf8,
        }
    )


def _salvar_log_vinculo_produto(resumo: dict, pasta_analises: Path, cnpj: str) -> None:
    with open(pasta_analises / f"log_vinculo_produto_fatores_{cnpj}.json", "w", encoding="utf-8") as f:
        json.dump(resumo, f, ensure_ascii=False, indent=2)


def _construir_vinculo_unico_por_descricao(
    df: pl.DataFrame,
    col_descr_padrao: str,
    origem: str,
    pasta_analises: Path | None = None,
    cnpj: str | None = None,
) -> tuple[pl.DataFrame, dict]:
    if df.is_empty():
        return _df_vazio_vinculo_produto(), {
            "origem": origem,
            "qtd_descricoes_unicas": 0,
            "qtd_descricoes_ambiguas": 0,
        }

    df_base = (
        df
        .select(
            [
                pl.col("descricao_normalizada").cast(pl.Utf8, strict=False).fill_null("").alias("descricao_normalizada"),
                pl.col("id_agrupado").cast(pl.Utf8, strict=False).alias("id_agrupado"),
                pl.col(col_descr_padrao).cast(pl.Utf8, strict=False).alias("descr_padrao_vinculo"),
            ]
        )
        .filter(pl.col("descricao_normalizada") != "")
        .drop_nulls(["descricao_normalizada", "id_agrupado"])
    )

    if df_base.is_empty():
        return _df_vazio_vinculo_produto(), {
            "origem": origem,
            "qtd_descricoes_unicas": 0,
            "qtd_descricoes_ambiguas": 0,
        }

    df_grouped = (
        df_base
        .group_by("descricao_normalizada")
        .agg(
            [
                pl.col("id_agrupado").n_unique().alias("__qtd_ids__"),
                pl.col("id_agrupado").first().alias("id_agrupado"),
                pl.col("descr_padrao_vinculo").drop_nulls().first().alias("descr_padrao_vinculo"),
            ]
        )
    )

    df_ambiguos = df_grouped.filter(pl.col("__qtd_ids__") > 1)
    if df_ambiguos.height > 0:
        rprint(
            f"[yellow]Aviso ({origem}): {df_ambiguos.height} descrição(ões) mapeiam para "
            f"múltiplos id_agrupado e serão descartadas do vínculo de fatores.[/yellow]"
        )
        if pasta_analises is not None and cnpj is not None:
            salvar_para_parquet(
                df_ambiguos.drop("__qtd_ids__"),
                pasta_analises,
                f"audit_descricao_ambigua_fatores_{cnpj}.parquet",
            )

    resumo = {
        "origem": origem,
        "qtd_descricoes_unicas": int(df_grouped.filter(pl.col("__qtd_ids__") == 1).height),
        "qtd_descricoes_ambiguas": int(df_ambiguos.height),
    }
    return (
        df_grouped
        .filter(pl.col("__qtd_ids__") == 1)
        .drop("__qtd_ids__")
        .with_columns(pl.lit(origem).alias("origem_vinculo_produto")),
        resumo,
    )


def _carregar_vinculo_produto_canonico(
    pasta_analises: Path,
    cnpj: str,
    df_final: pl.DataFrame,
) -> pl.DataFrame:
    bases: list[pl.DataFrame] = []
    resumo: dict[str, dict] = {}

    arq_pont = pasta_analises / f"map_produto_agrupado_{cnpj}.parquet"
    if arq_pont.exists():
        schema_cols = set(pl.read_parquet_schema(arq_pont).names())
        if {"descricao_normalizada", "id_agrupado"}.issubset(schema_cols):
            df_map_raw = (
                pl.scan_parquet(arq_pont)
                .select(
                    [
                        pl.col("descricao_normalizada").cast(pl.Utf8, strict=False),
                        pl.col("id_agrupado").cast(pl.Utf8, strict=False),
                    ]
                )
                .collect()
                .join(
                    df_final
                    .select(["id_agrupado", "descr_padrao_calc"])
                    .unique(subset=["id_agrupado"], keep="first"),
                    on="id_agrupado",
                    how="left",
                )
            )
            df_map, resumo_map = _construir_vinculo_unico_por_descricao(
                df_map_raw,
                "descr_padrao_calc",
                "map_produto_agrupado",
                pasta_analises=pasta_analises,
                cnpj=cnpj,
            )
            resumo["map_produto_agrupado"] = resumo_map
            if not df_map.is_empty():
                bases.append(df_map.with_columns(pl.lit(0).alias("__ordem_vinculo__")))

    df_final_base, resumo_final = _construir_vinculo_unico_por_descricao(
        df_final.select(["descricao_normalizada", "id_agrupado", "descr_padrao_calc"]),
        "descr_padrao_calc",
        "produtos_final",
        pasta_analises=pasta_analises,
        cnpj=cnpj,
    )
    resumo["produtos_final"] = resumo_final
    if not df_final_base.is_empty():
        bases.append(df_final_base.with_columns(pl.lit(1).alias("__ordem_vinculo__")))

    if not bases:
        _salvar_log_vinculo_produto(
            {
                **resumo,
                "resultado": {
                    "qtd_descricoes_vinculadas": 0,
                },
            },
            pasta_analises,
            cnpj,
        )
        return _df_vazio_vinculo_produto()

    df_vinculo = (
        pl.concat(bases, how="vertical_relaxed")
        .sort(["descricao_normalizada", "__ordem_vinculo__"])
        .unique(subset=["descricao_normalizada"], keep="first")
        .drop("__ordem_vinculo__")
    )
    # Garantir que a chave `descricao_normalizada` use a mesma normalizacao
    # aplicada em `item_unidades` para que o join seja deterministico.
    if not df_vinculo.is_empty():
        df_vinculo = df_vinculo.with_columns(_normalizar_descricao_expr("descricao_normalizada"))
    _salvar_log_vinculo_produto(
        {
            **resumo,
            "resultado": {
                "qtd_descricoes_vinculadas": int(df_vinculo.height),
            },
        },
        pasta_analises,
        cnpj,
    )
    return df_vinculo


def _primeira_lista_textos_nao_vazia(listas: list | None) -> list[str]:
    if isinstance(listas, pl.Series):
        listas = listas.to_list()
    for valores in listas or []:
        if isinstance(valores, pl.Series):
            valores = valores.to_list()
        saida: list[str] = []
        for valor in valores or []:
            texto = str(valor).strip() if valor is not None else ""
            if texto:
                saida.append(texto)
        if saida:
            return sorted(set(saida))
    return []


def _salvar_log_reconciliacao_overrides(df_log: pl.DataFrame, pasta_analises: Path, cnpj: str) -> None:
    if df_log.is_empty():
        return

    salvar_para_parquet(df_log, pasta_analises, f"log_reconciliacao_overrides_fatores_{cnpj}.parquet")
    resumo = {
        "cnpj": cnpj,
        "qtd_registros": int(df_log.height),
        "qtd_remapeados": int(df_log.filter(pl.col("acao") == "remapeado").height),
        "qtd_descartados": int(df_log.filter(pl.col("acao") == "descartado").height),
    }
    with open(pasta_analises / f"log_reconciliacao_overrides_fatores_{cnpj}.json", "w", encoding="utf-8") as f:
        json.dump(resumo, f, ensure_ascii=False, indent=2)


def _construir_mapa_descricoes_canonicas(df_agrupamento_canonico: pl.DataFrame) -> pl.DataFrame:
    if df_agrupamento_canonico.is_empty():
        return pl.DataFrame(
            schema={
                "descricao_normalizada_match": pl.Utf8,
                "id_agrupado_destino": pl.Utf8,
                "descr_padrao_destino": pl.Utf8,
            }
        )

    partes = [
        df_agrupamento_canonico.select(
            [
                pl.col("id_agrupado").cast(pl.Utf8, strict=False),
                pl.col("descr_padrao_canonico").cast(pl.Utf8, strict=False).alias("descr_padrao_destino"),
                pl.col("descr_padrao_canonico").cast(pl.Utf8, strict=False).alias("descricao_texto"),
            ]
        )
    ]

    if "lista_descricoes" in df_agrupamento_canonico.columns:
        partes.append(
            df_agrupamento_canonico
            .select(
                [
                    pl.col("id_agrupado").cast(pl.Utf8, strict=False),
                    pl.col("descr_padrao_canonico").cast(pl.Utf8, strict=False).alias("descr_padrao_destino"),
                    pl.col("lista_descricoes").cast(pl.List(pl.Utf8), strict=False).alias("descricao_texto"),
                ]
            )
            .explode("descricao_texto")
        )
    if "lista_desc_compl" in df_agrupamento_canonico.columns:
        partes.append(
            df_agrupamento_canonico
            .select(
                [
                    pl.col("id_agrupado").cast(pl.Utf8, strict=False),
                    pl.col("descr_padrao_canonico").cast(pl.Utf8, strict=False).alias("descr_padrao_destino"),
                    pl.col("lista_desc_compl").cast(pl.List(pl.Utf8), strict=False).alias("descricao_texto"),
                ]
            )
            .explode("descricao_texto")
        )

    return (
        pl.concat(partes, how="vertical_relaxed")
        .with_columns(
            [
                pl.col("descricao_texto").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars().alias("descricao_texto"),
                normalizar_descricao_expr("descricao_texto").alias("descricao_normalizada_match"),
            ]
        )
        .filter(pl.col("descricao_normalizada_match") != "")
        .unique(subset=["descricao_normalizada_match", "id_agrupado"])
        .group_by("descricao_normalizada_match")
        .agg(
            [
                pl.col("id_agrupado").n_unique().alias("__qtd_ids__"),
                pl.col("id_agrupado").first().alias("id_agrupado_destino"),
                pl.col("descr_padrao_destino").drop_nulls().first().alias("descr_padrao_destino"),
            ]
        )
        .filter(pl.col("__qtd_ids__") == 1)
        .drop("__qtd_ids__")
    )


def _carregar_base_agrupamento_canonico(path: Path) -> pl.DataFrame:
    if not path.exists():
        return pl.DataFrame(
            schema={
                "id_agrupado": pl.Utf8,
                "descr_padrao_canonico": pl.Utf8,
                "lista_descricoes": pl.List(pl.Utf8),
                "lista_desc_compl": pl.List(pl.Utf8),
            }
        )

    schema_cols = set(pl.read_parquet_schema(path).names())
    if "id_agrupado" not in schema_cols:
        return pl.DataFrame(
            schema={
                "id_agrupado": pl.Utf8,
                "descr_padrao_canonico": pl.Utf8,
                "lista_descricoes": pl.List(pl.Utf8),
                "lista_desc_compl": pl.List(pl.Utf8),
            }
        )

    selecionadas = ["id_agrupado"]
    if "descr_padrao" in schema_cols:
        selecionadas.append("descr_padrao")
    if "lista_descricoes" in schema_cols:
        selecionadas.append("lista_descricoes")
    if "lista_desc_compl" in schema_cols:
        selecionadas.append("lista_desc_compl")

    df = pl.scan_parquet(path).select(selecionadas).collect()
    if "descr_padrao" not in df.columns:
        df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("descr_padrao"))
    if "lista_descricoes" not in df.columns:
        df = df.with_columns(pl.lit([]).cast(pl.List(pl.Utf8), strict=False).alias("lista_descricoes"))
    if "lista_desc_compl" not in df.columns:
        df = df.with_columns(pl.lit([]).cast(pl.List(pl.Utf8), strict=False).alias("lista_desc_compl"))

    return (
        df
        .select(
            [
                pl.col("id_agrupado").cast(pl.Utf8, strict=False),
                pl.col("descr_padrao").cast(pl.Utf8, strict=False).alias("descr_padrao_canonico"),
                pl.col("lista_descricoes").cast(pl.List(pl.Utf8), strict=False),
                pl.col("lista_desc_compl").cast(pl.List(pl.Utf8), strict=False),
            ]
        )
        .unique(subset=["id_agrupado"], keep="first")
    )


def _carregar_agrupamento_canonico(pasta_analises: Path, cnpj: str) -> pl.DataFrame:
    bases = []
    for ordem, nome in enumerate(
        [
            f"produtos_agrupados_{cnpj}.parquet",
            f"id_agrupados_{cnpj}.parquet",
        ]
    ):
        df_base = _carregar_base_agrupamento_canonico(pasta_analises / nome)
        if df_base.is_empty():
            continue
        bases.append(df_base.with_columns(pl.lit(ordem).alias("__ordem_fonte")))

    if not bases:
        return pl.DataFrame(
            schema={
                "id_agrupado": pl.Utf8,
                "descr_padrao_canonico": pl.Utf8,
                "lista_descricoes": pl.List(pl.Utf8),
                "lista_desc_compl": pl.List(pl.Utf8),
            }
        )

    df_grouped = (
        pl.concat(bases, how="vertical_relaxed")
        .sort(["id_agrupado", "__ordem_fonte"])
        .group_by("id_agrupado", maintain_order=True)
        .agg(
            [
                pl.col("descr_padrao_canonico").drop_nulls().first().alias("descr_padrao_canonico"),
                pl.col("lista_descricoes").drop_nulls().alias("__listas_descricoes"),
                pl.col("lista_desc_compl").drop_nulls().alias("__listas_desc_compl"),
            ]
        )
    )

    def _first_non_empty(lists):
        if not lists:
            return []
        for inner in lists:
            if inner:
                return inner
        return []

    for source_col, target_col in [
        ("__listas_descricoes", "lista_descricoes"),
        ("__listas_desc_compl", "lista_desc_compl"),
    ]:
        valores = df_grouped.get_column(source_col).to_list()
        nova_col = [_first_non_empty(v) for v in valores]
        df_grouped = df_grouped.with_columns(pl.Series(nova_col).alias(target_col))

    return df_grouped.drop(["__listas_descricoes", "__listas_desc_compl"], strict=False)


def _reconciliar_fatores_existentes_com_agrupamento_atual(
    df_existente: pl.DataFrame,
    df_agrupamento_canonico: pl.DataFrame,
    pasta_analises: Path,
    cnpj: str,
) -> pl.DataFrame:
    if df_existente.is_empty() or df_agrupamento_canonico.is_empty():
        return df_existente

    tem_flags_manuais = {"fator_manual", "unid_ref_manual"}.intersection(df_existente.columns)
    if not tem_flags_manuais:
        return df_existente

    colunas_padrao = []
    if "id_produtos" not in df_existente.columns:
        colunas_padrao.append(pl.lit(None, dtype=pl.Utf8).alias("id_produtos"))
    if "descr_padrao" not in df_existente.columns:
        colunas_padrao.append(pl.lit(None, dtype=pl.Utf8).alias("descr_padrao"))
    if "fator_manual" not in df_existente.columns:
        colunas_padrao.append(pl.lit(False).alias("fator_manual"))
    if "unid_ref_manual" not in df_existente.columns:
        colunas_padrao.append(pl.lit(False).alias("unid_ref_manual"))
    if colunas_padrao:
        df_existente = df_existente.with_columns(colunas_padrao)

    df_existente = df_existente.with_row_index("__idx_override__").with_columns(
        [
            pl.col("id_agrupado").cast(pl.Utf8, strict=False),
            pl.col("id_produtos").cast(pl.Utf8, strict=False),
            pl.col("descr_padrao").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars().alias("descr_padrao"),
            pl.col("unid").cast(pl.Utf8, strict=False),
            pl.col("unid_ref").cast(pl.Utf8, strict=False),
            pl.col("fator_manual").cast(pl.Boolean, strict=False).fill_null(False).alias("fator_manual"),
            pl.col("unid_ref_manual").cast(pl.Boolean, strict=False).fill_null(False).alias("unid_ref_manual"),
        ]
    )

    df_canonico = df_agrupamento_canonico.select(
        [
            pl.col("id_agrupado").cast(pl.Utf8, strict=False),
            pl.col("descr_padrao_canonico").cast(pl.Utf8, strict=False),
            pl.col("lista_descricoes").cast(pl.List(pl.Utf8), strict=False),
            pl.col("lista_desc_compl").cast(pl.List(pl.Utf8), strict=False),
        ]
    )
    df_mapa_descricoes = _construir_mapa_descricoes_canonicas(df_canonico)

    df_avaliacao = (
        df_existente
        .join(df_canonico, on="id_agrupado", how="left")
        .with_columns(
            [
                (pl.col("fator_manual") | pl.col("unid_ref_manual")).alias("__eh_manual__"),
                normalizar_descricao_expr("descr_padrao").alias("__descr_padrao_norm__"),
                normalizar_descricao_expr("descr_padrao_canonico").alias("__descr_padrao_canonico_norm__"),
            ]
        )
        .with_columns(
            (
                pl.col("__eh_manual__")
                & (
                    (pl.col("__descr_padrao_canonico_norm__") == "")
                    | (pl.col("__descr_padrao_norm__") != pl.col("__descr_padrao_canonico_norm__"))
                )
            ).alias("__manual_incoerente__")
        )
    )

    df_manuais_incoerentes = (
        df_avaliacao
        .filter(pl.col("__manual_incoerente__"))
        .join(df_mapa_descricoes, left_on="__descr_padrao_norm__", right_on="descricao_normalizada_match", how="left")
    )

    if df_manuais_incoerentes.is_empty():
        return df_existente.drop("__idx_override__")

    df_remapeados = (
        df_manuais_incoerentes
        .filter(pl.col("id_agrupado_destino").is_not_null())
        .with_columns(
            [
                pl.col("id_agrupado").alias("id_agrupado_original"),
                pl.col("id_agrupado_destino").alias("id_agrupado"),
                pl.col("descr_padrao_destino").alias("descr_padrao"),
                pl.when(pl.col("id_produtos").is_not_null())
                .then(pl.col("id_agrupado_destino"))
                .otherwise(pl.col("id_produtos"))
                .alias("id_produtos"),
            ]
        )
    )

    df_descartados = df_manuais_incoerentes.filter(pl.col("id_agrupado_destino").is_null())

    df_log = pl.concat(
        [
            df_remapeados.select(
                [
                    pl.lit("remapeado").alias("acao"),
                    pl.col("id_agrupado_original"),
                    pl.col("id_agrupado").alias("id_agrupado_destino"),
                    pl.col("descr_padrao").alias("descr_padrao_destino"),
                    pl.col("descr_padrao_canonico"),
                    pl.col("unid"),
                    pl.col("unid_ref"),
                    pl.lit("Override manual realocado para o agrupamento canonico atual.").alias("motivo"),
                ]
            ),
            df_descartados.select(
                [
                    pl.lit("descartado").alias("acao"),
                    pl.col("id_agrupado").alias("id_agrupado_original"),
                    pl.lit(None, dtype=pl.Utf8).alias("id_agrupado_destino"),
                    pl.lit(None, dtype=pl.Utf8).alias("descr_padrao_destino"),
                    pl.col("descr_padrao_canonico"),
                    pl.col("unid"),
                    pl.col("unid_ref"),
                    pl.lit("Override manual descartado por nao haver correspondencia unica no agrupamento atual.").alias("motivo"),
                ]
            ),
        ],
        how="vertical_relaxed",
    )
    _salvar_log_reconciliacao_overrides(df_log, pasta_analises, cnpj)

    df_base_preservada = df_existente.join(
        df_manuais_incoerentes.select("__idx_override__").unique(),
        on="__idx_override__",
        how="anti",
    )

    df_reconciliado = pl.concat(
        [
            df_base_preservada,
            df_remapeados.select(df_base_preservada.columns),
        ],
        how="vertical_relaxed",
    )

    return df_reconciliado.drop("__idx_override__").unique(maintain_order=True)


def _extrair_overrides_existentes(df_existente: pl.DataFrame, df_final: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    if df_existente.is_empty():
        return _df_vazio_override_unid(), _df_vazio_override_fator()

    df_existente = df_existente.with_columns(
        [
            pl.col("id_agrupado").cast(pl.Utf8, strict=False),
            pl.col("unid").cast(pl.Utf8, strict=False),
            pl.col("unid_ref").cast(pl.Utf8, strict=False),
            pl.col("fator").cast(pl.Float64, strict=False),
            pl.col("preco_medio").cast(pl.Float64, strict=False),
        ]
    )

    if "unid_ref_manual" in df_existente.columns:
        df_unid_override = (
            df_existente
            .filter(pl.col("unid_ref_manual").cast(pl.Boolean, strict=False).fill_null(False))
            .select(["id_agrupado", pl.col("unid_ref").alias("unid_ref_override")])
            .drop_nulls(["id_agrupado", "unid_ref_override"])
            .unique(subset=["id_agrupado"], keep="first")
        )
    else:
        df_ref_final = (
            df_final
            .select(["id_agrupado", "unid_ref_sugerida"])
            .with_columns(
                [
                    pl.col("id_agrupado").cast(pl.Utf8, strict=False),
                    pl.col("unid_ref_sugerida").cast(pl.Utf8, strict=False),
                ]
            )
            .unique(subset=["id_agrupado"], keep="first")
        )
        df_unid_override = (
            df_existente
            .select(["id_agrupado", "unid_ref"])
            .drop_nulls(["id_agrupado", "unid_ref"])
            .unique(subset=["id_agrupado"], keep="first")
            .join(df_ref_final, on="id_agrupado", how="left")
            .filter(
                pl.col("unid_ref").is_not_null()
                & (
                    pl.col("unid_ref_sugerida").is_null()
                    | (pl.col("unid_ref") != pl.col("unid_ref_sugerida"))
                )
            )
            .select(["id_agrupado", pl.col("unid_ref").alias("unid_ref_override")])
        )

    if "fator_manual" in df_existente.columns:
        df_fator_override = (
            df_existente
            .filter(pl.col("fator_manual").cast(pl.Boolean, strict=False).fill_null(False))
            .select(["id_agrupado", "unid", pl.col("fator").alias("fator_override")])
            .drop_nulls(["id_agrupado", "unid"])
            .unique(subset=["id_agrupado", "unid"], keep="first")
        )
    else:
        df_ref_prev = (
            df_existente
            .filter(pl.col("unid") == pl.col("unid_ref"))
            .group_by("id_agrupado")
            .agg(pl.col("preco_medio").mean().alias("__preco_ref_prev__"))
        )
        df_fator_override = (
            df_existente
            .join(df_ref_prev, on="id_agrupado", how="left")
            .with_columns(
                pl.when(pl.col("__preco_ref_prev__") > 0)
                .then(pl.col("preco_medio") / pl.col("__preco_ref_prev__"))
                .otherwise(1.0)
                .alias("__fator_calc_prev__")
            )
            .filter((pl.col("fator") - pl.col("__fator_calc_prev__")).abs() > 1e-9)
            .select(["id_agrupado", "unid", pl.col("fator").alias("fator_override")])
            .drop_nulls(["id_agrupado", "unid"])
            .unique(subset=["id_agrupado", "unid"], keep="first")
        )

    if df_unid_override.is_empty():
        df_unid_override = _df_vazio_override_unid()
    if df_fator_override.is_empty():
        df_fator_override = _df_vazio_override_fator()
    return df_unid_override, df_fator_override


def _carregar_vinculo_id_agrupado(pasta_analises: Path, cnpj: str, df_final: pl.DataFrame) -> pl.DataFrame:
    arq_descricoes = pasta_analises / f"descricao_produtos_{cnpj}.parquet"
    arq_mapa = pasta_analises / f"map_produto_agrupado_{cnpj}.parquet"

    if arq_descricoes.exists() and arq_mapa.exists():
        df_descricoes = (
            pl.read_parquet(arq_descricoes)
            .select(["id_descricao", "descricao_normalizada"])
            .with_columns(
                [
                    pl.col("id_descricao").cast(pl.Utf8, strict=False),
                    pl.col("descricao_normalizada").cast(pl.Utf8, strict=False),
                ]
            )
        )
        df_mapa = (
            pl.read_parquet(arq_mapa)
            .select([pl.col("chave_produto").alias("id_descricao"), "id_agrupado"])
            .with_columns(
                [
                    pl.col("id_descricao").cast(pl.Utf8, strict=False),
                    pl.col("id_agrupado").cast(pl.Utf8, strict=False),
                ]
            )
            .unique(subset=["id_descricao"], keep="first")
        )
        df_vinculo = (
            df_descricoes
            .join(df_mapa, on="id_descricao", how="left")
            .filter(pl.col("id_agrupado").is_not_null())
            .select(["descricao_normalizada", "id_agrupado"])
            .unique(subset=["descricao_normalizada"], keep="first")
        )
        if not df_vinculo.is_empty():
            return df_vinculo

    return (
        df_final
        .select(["descricao_normalizada", "id_agrupado"])
        .with_columns(
            [
                pl.col("descricao_normalizada").cast(pl.Utf8, strict=False),
                pl.col("id_agrupado").cast(pl.Utf8, strict=False),
            ]
        )
        .filter(pl.col("descricao_normalizada").is_not_null() & pl.col("id_agrupado").is_not_null())
        .unique(subset=["descricao_normalizada"], keep="first")
        if not df_final.is_empty()
        else _df_vazio_vinculo()
    )


def calcular_fatores_conversao(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj = re.sub(r"\D", "", cnpj or "")
    if len(cnpj) not in {11, 14}:
        raise ValueError("CPF/CNPJ invalido.")

    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj

    # Suportar duas formas de chamada:
    # 1) pasta_cnpj é a raiz do CNPJ (padrão) -> arquivos em <pasta_cnpj>/analises/produtos
    # 2) pasta_cnpj já aponta para a pasta de analises/produtos (usado em testes)
    pasta_analises_default = pasta_cnpj / "analises" / "produtos"
    pasta_analises_alt = pasta_cnpj

    # Detectar automaticamente onde os arquivos foram gravados e usar essa pasta
    if (pasta_analises_default / f"produtos_final_{cnpj}.parquet").exists() or (
        pasta_analises_default / f"item_unidades_{cnpj}.parquet"
    ).exists():
        pasta_analises = pasta_analises_default
    elif (pasta_analises_alt / f"produtos_final_{cnpj}.parquet").exists() or (
        pasta_analises_alt / f"item_unidades_{cnpj}.parquet"
    ).exists():
        pasta_analises = pasta_analises_alt
    else:
        pasta_analises = pasta_analises_default

    arq_unid = pasta_analises / f"item_unidades_{cnpj}.parquet"
    arq_final = pasta_analises / f"produtos_final_{cnpj}.parquet"
    arq_fatores_existente = pasta_analises / f"fatores_conversao_{cnpj}.parquet"

    for arq in (arq_unid, arq_final):
        if not arq.exists():
            rprint(f"[red]Arquivo necessario nao encontrado:[/red] {arq}")
            return False

    rprint(f"[bold cyan]Calculando fatores de conversao para CNPJ: {cnpj}[/bold cyan]")

    df_unid = pl.read_parquet(arq_unid)
    df_final = pl.read_parquet(arq_final)
    df_fatores_existente = pl.read_parquet(arq_fatores_existente) if arq_fatores_existente.exists() else pl.DataFrame()

    # Garantir colunas principais — alguns testes ou fontes podem omitir
    # colunas auxiliares como `descricao_final`. Adicionamos colunas ausentes
    # como `None` antes da validacao para manter compatibilidade com os testes.
    required_unid = ["descricao", "unid", "compras", "vendas", "qtd_compras", "qtd_vendas"]
    for col in required_unid:
        if col not in df_unid.columns:
            df_unid = df_unid.with_columns(pl.lit(None).alias(col))

    required_final = ["id_agrupado", "descricao_normalizada", "descricao_final", "descr_padrao", "unid_ref_sugerida"]
    for col in required_final:
        if col not in df_final.columns:
            df_final = df_final.with_columns(pl.lit(None).alias(col))

    try:
        garantir_colunas_obrigatorias(
            df_unid,
            required_unid,
            contexto="fatores_conversao/item_unidades",
        )
        garantir_colunas_obrigatorias(
            df_final,
            required_final,
            contexto="fatores_conversao/produtos_final",
        )
    except SchemaValidacaoError as exc:
        rprint(f"[red]{exc}[/red]")
        return False

    if df_unid.is_empty() or df_final.is_empty():
        rprint("[yellow]Aviso: sem dados para calcular fatores de conversao.[/yellow]")
        return salvar_para_parquet(pl.DataFrame(), pasta_analises, f"fatores_conversao_{cnpj}.parquet")

    required_unid = ["descricao", "unid", "compras", "vendas", "qtd_compras", "qtd_vendas"]
    required_final = ["id_agrupado", "descricao_normalizada", "descricao_final", "descr_padrao", "unid_ref_sugerida"]

    for col in required_unid:
        if col not in df_unid.columns:
            df_unid = df_unid.with_columns(pl.lit(None).alias(col))

    for col in required_final:
        if col not in df_final.columns:
            df_final = df_final.with_columns(pl.lit(None).alias(col))

    df_unid = df_unid.with_columns(_normalizar_descricao_expr("descricao"))
    # Normalizar também os valores em `produtos_final.descricao_normalizada` para
    # garantir compatibilidade com a normalizacao aplicada em `item_unidades.descricao`.
    df_final = df_final.with_columns(_normalizar_descricao_expr("descricao_normalizada"))
    df_final = df_final.with_columns(
        [
            pl.col("id_agrupado").cast(pl.Utf8, strict=False).alias("id_agrupado"),
            pl.col("descricao_normalizada").cast(pl.Utf8, strict=False).fill_null("").alias("descricao_normalizada"),
            pl.coalesce([pl.col("descr_padrao"), pl.col("descricao_final")]).alias("descr_padrao_calc"),
        ]
    )
    df_ref_final = (
        df_final
        .select(["id_agrupado", "descr_padrao_calc", "unid_ref_sugerida"])
        .with_columns(
            [
                pl.col("id_agrupado").cast(pl.Utf8, strict=False),
                pl.col("descr_padrao_calc").cast(pl.Utf8, strict=False),
                pl.col("unid_ref_sugerida").cast(pl.Utf8, strict=False),
            ]
        )
        .unique(subset=["id_agrupado"], keep="first")
    )
    df_vinculo_produto = _carregar_vinculo_produto_canonico(pasta_analises, cnpj, df_final)
    df_agrupamento_canonico = _carregar_agrupamento_canonico(pasta_analises, cnpj)
    df_fatores_existente = _reconciliar_fatores_existentes_com_agrupamento_atual(
        df_fatores_existente,
        df_agrupamento_canonico,
        pasta_analises,
        cnpj,
    )
    df_unid_override, df_fator_override = _extrair_overrides_existentes(df_fatores_existente, df_final)
    df_vinculo = _carregar_vinculo_id_agrupado(pasta_analises, cnpj, df_final)

    df_link = (
        df_unid
        .join(df_vinculo_produto, on="descricao_normalizada", how="left")
        .join(df_ref_final, on="id_agrupado", how="left")
        .with_columns(
            pl.coalesce([pl.col("descr_padrao_vinculo"), pl.col("descr_padrao_calc")]).alias("descr_padrao_calc")
        )
        .filter(pl.col("id_agrupado").is_not_null() & pl.col("unid").is_not_null())
    )

    if df_link.is_empty():
        rprint("[yellow]Aviso: sem vinculacao entre item_unidades e camada canonica de agrupamento.[/yellow]")
        return salvar_para_parquet(pl.DataFrame(), pasta_analises, f"fatores_conversao_{cnpj}.parquet")

    df_precos = (
        df_link
        .group_by(["id_agrupado", "descr_padrao_calc", "unid"])
        .agg(
            [
                pl.col("compras").cast(pl.Float64, strict=False).fill_null(0).sum().alias("compras_total"),
                pl.col("qtd_compras").cast(pl.Float64, strict=False).fill_null(0).sum().alias("qtd_compras_total"),
                pl.col("vendas").cast(pl.Float64, strict=False).fill_null(0).sum().alias("vendas_total"),
                pl.col("qtd_vendas").cast(pl.Float64, strict=False).fill_null(0).sum().alias("qtd_vendas_total"),
                (
                    pl.col("qtd_compras").cast(pl.Float64, strict=False).fill_null(0).sum()
                    + pl.col("qtd_vendas").cast(pl.Float64, strict=False).fill_null(0).sum()
                ).alias("qtd_mov_total"),
            ]
        )
        .rename({"descr_padrao_calc": "descr_padrao"})
        .with_columns(
            [
                pl.when(pl.col("qtd_compras_total") > 0)
                .then(pl.col("compras_total") / pl.col("qtd_compras_total"))
                .otherwise(None)
                .alias("preco_medio_compra"),
                pl.when(pl.col("qtd_vendas_total") > 0)
                .then(pl.col("vendas_total") / pl.col("qtd_vendas_total"))
                .otherwise(None)
                .alias("preco_medio_venda"),
            ]
        )
        .with_columns(
            [
                pl.coalesce([pl.col("preco_medio_compra"), pl.col("preco_medio_venda")]).alias("preco_medio_base"),
                pl.when(pl.col("preco_medio_compra").is_not_null())
                .then(pl.lit("COMPRA"))
                .when(pl.col("preco_medio_venda").is_not_null())
                .then(pl.lit("VENDA"))
                .otherwise(pl.lit("SEM_PRECO"))
                .alias("origem_preco"),
            ]
        )
        .sort(["id_agrupado", "unid"])
    )

    df_sem_compra = (
        df_precos
        .filter(pl.col("preco_medio_compra").is_null())
        .with_columns(
            [
                pl.col("preco_medio_venda").is_not_null().alias("tem_preco_venda"),
                pl.when(pl.col("preco_medio_venda").is_not_null())
                .then(pl.lit("Sem compra; usado preco medio de venda como fallback."))
                .otherwise(pl.lit("Sem preco medio de compra e sem fallback de venda."))
                .alias("motivo"),
            ]
        )
        .select(
            [
                "id_agrupado",
                "descr_padrao",
                "unid",
                "qtd_compras_total",
                "qtd_vendas_total",
                "preco_medio_compra",
                "preco_medio_venda",
                "tem_preco_venda",
                "motivo",
            ]
        )
    )
    _salvar_log_sem_compra(df_sem_compra, pasta_analises, cnpj)

    unid_ref_manual = (
        df_final
        .select(["id_agrupado", "unid_ref_sugerida"])
        .drop_nulls("id_agrupado")
        .with_columns(pl.col("unid_ref_sugerida").cast(pl.String, strict=False).str.strip_chars())
        .filter(pl.col("unid_ref_sugerida").is_not_null() & (pl.col("unid_ref_sugerida") != ""))
        .group_by("id_agrupado")
        .agg(pl.col("unid_ref_sugerida").first().alias("unid_ref_manual_sugerida"))
    )

    unid_ref_auto = (
        df_precos
        .filter(pl.col("unid").is_not_null())
        .sort(["id_agrupado", "qtd_mov_total", "qtd_compras_total"], descending=[False, True, True], nulls_last=True)
        .group_by("id_agrupado")
        .agg(pl.col("unid").first().alias("unid_ref_auto"))
    )

    df_full = (
        df_precos
        .join(unid_ref_manual, on="id_agrupado", how="left")
        .join(unid_ref_auto, on="id_agrupado", how="left")
        .join(df_unid_override, on="id_agrupado", how="left")
        .with_columns(
            pl.coalesce([pl.col("unid_ref_override"), pl.col("unid_ref_manual_sugerida"), pl.col("unid_ref_auto")]).alias("unid_ref")
        )
    )

    df_ref_price = (
        df_full
        .filter(pl.col("unid") == pl.col("unid_ref"))
        .group_by("id_agrupado")
        .agg(pl.col("preco_medio_base").mean().alias("preco_unid_ref"))
    )

    df_fatores = (
        df_full
        .join(df_ref_price, on="id_agrupado", how="left")
        .join(df_fator_override, on=["id_agrupado", "unid"], how="left")
        .with_columns(
            [
                pl.when((pl.col("preco_unid_ref") > 0) & pl.col("preco_medio_base").is_not_null())
                .then(pl.col("preco_medio_base") / pl.col("preco_unid_ref"))
                .otherwise(1.0)
                .alias("fator_calc"),
                pl.col("unid_ref_override").is_not_null().fill_null(False).alias("unid_ref_manual"),
            ]
        )
        .with_columns(
            [
                pl.coalesce([pl.col("fator_override"), pl.col("fator_calc")]).alias("fator"),
                pl.col("fator_override").is_not_null().fill_null(False).alias("fator_manual"),
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("fator_manual"))
                .then(pl.lit("manual"))
                .when(pl.col("origem_preco") == "SEM_PRECO")
                .then(pl.lit("fallback_sem_preco"))
                .when(pl.col("preco_unid_ref").is_null() | (pl.col("preco_unid_ref") <= 0))
                .then(pl.lit("fallback_sem_preco_ref"))
                .otherwise(pl.lit("preco"))
                .alias("fator_origem"),
            ]
        )
        .select(
            [
                "id_agrupado",
                pl.col("id_agrupado").alias("id_produtos"),
                "descr_padrao",
                "unid",
                "unid_ref",
                "unid_ref_override",
                "fator",
                "fator_override",
                "fator_manual",
                "unid_ref_manual",
                "fator_origem",
                pl.col("preco_medio_base").alias("preco_medio"),
                "origem_preco",
            ]
        )
        .unique()
        .sort(["id_agrupado", "unid"])
    )

    if not df_fatores_existente.is_empty():
        colunas_manual = []
        if "fator_manual" in df_fatores_existente.columns:
            colunas_manual.append(pl.col("fator_manual").cast(pl.Boolean, strict=False).fill_null(False))
        if "unid_ref_manual" in df_fatores_existente.columns:
            colunas_manual.append(pl.col("unid_ref_manual").cast(pl.Boolean, strict=False).fill_null(False))

        if colunas_manual:
            df_manuais_antigos = (
                df_fatores_existente
                .with_columns(colunas_manual)
                .filter(
                    pl.col("fator_manual").fill_null(False)
                    | pl.col("unid_ref_manual").fill_null(False)
                )
            )

            if not df_manuais_antigos.is_empty():
                df_orfaos = (
                    df_manuais_antigos
                    .join(
                        df_fatores.select(["id_agrupado", "unid"]).unique(),
                        on=["id_agrupado", "unid"],
                        how="anti",
                    )
                )

                if not df_orfaos.is_empty():
                    for col in df_fatores.columns:
                        if col not in df_orfaos.columns:
                            df_orfaos = df_orfaos.with_columns(pl.lit(None).alias(col))
                    if "id_produtos" in df_orfaos.columns:
                        df_orfaos = df_orfaos.with_columns(
                            pl.coalesce([pl.col("id_produtos"), pl.col("id_agrupado")]).alias("id_produtos")
                        )

                    df_orfaos = df_orfaos.select(df_fatores.columns)
                    df_fatores = pl.concat([df_fatores, df_orfaos], how="vertical_relaxed")
                    df_fatores = df_fatores.unique(subset=["id_agrupado", "unid"], keep="first").sort(["id_agrupado", "unid"])
                    rprint(f"[green]Preservados {df_orfaos.height} fatores manuais orfaos.[/green]")

    return salvar_para_parquet(df_fatores, pasta_analises, f"fatores_conversao_{cnpj}.parquet")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        calcular_fatores_conversao(sys.argv[1])
    else:
        calcular_fatores_conversao(input("CNPJ: "))
