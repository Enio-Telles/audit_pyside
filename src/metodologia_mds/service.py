"""Serviços mínimos para aplicar a metodologia MDS (derivação de quantidades)."""
from __future__ import annotations
from pathlib import Path
from typing import Union

try:
    import polars as pl
except Exception as exc:  # pragma: no cover - import error surfaces at runtime
    raise RuntimeError("Polars is required. Install with `pip install polars`.") from exc


class MovimentacaoService:
    """Service com funções puras para derivação de campos da movimentação.

    Métodos públicos:
    - load_parquet(path) -> pl.DataFrame
    - derive_quantities(df) -> pl.DataFrame
    """

    @staticmethod
    def load_parquet(path: Union[str, Path]) -> "pl.DataFrame":
        return pl.read_parquet(str(path))

    @staticmethod
    def derive_quantities(df: "pl.DataFrame") -> "pl.DataFrame":
        """Garante colunas críticas e deriva `quantidade_fisica*` e `estoque_final_declarado`.

        Regras aplicadas (resumo):
        - Se `tipo_operacao` indicar inventário (contendo 'ESTOQUE FINAL' ou começando com '3'),
          então `quantidade_fisica = 0` e `estoque_final_declarado = quantidade_convertida`.
        - Caso contrário, `quantidade_fisica = quantidade_convertida` (cast para Float64).
        - `quantidade_fisica_sinalizada` aplica sinal negativo para saídas (contain 'SAIDA' ou code '2').
        """
        # garantir quantidade_convertida
        if "quantidade_convertida" not in df.columns:
            if "quantidade_original" in df.columns:
                df = df.with_columns(
                    pl.col("quantidade_original").cast(pl.Float64).fill_null(0.0).alias("quantidade_convertida")
                )
            else:
                df = df.with_columns(pl.lit(0.0).cast(pl.Float64).alias("quantidade_convertida"))

        tipo = pl.col("tipo_operacao").cast(pl.Utf8)
        is_estoque_final = tipo.str.contains("ESTOQUE FINAL") | tipo.str.starts_with("3")
        is_saida = tipo.str.contains("SAIDA") | tipo.str.starts_with("2")

        df = df.with_columns(
            pl.when(is_estoque_final)
            .then(pl.lit(0.0))
            .otherwise(pl.col("quantidade_convertida").cast(pl.Float64).fill_null(0.0))
            .alias("quantidade_fisica")
        )

        df = df.with_columns(
            pl.when(is_saida)
            .then(-pl.col("quantidade_fisica"))
            .otherwise(pl.col("quantidade_fisica"))
            .alias("quantidade_fisica_sinalizada")
        )

        df = df.with_columns(
            pl.when(is_estoque_final)
            .then(pl.col("quantidade_convertida"))
            .otherwise(pl.lit(None).cast(pl.Float64))
            .alias("estoque_final_declarado")
        )

        # Compatibilidade: garantir colunas usadas pelo restante do pipeline
        cols = set(df.columns)

        # `q_conv` é a coluna histórica de quantidade convertida usada por vários cálculos
        if "q_conv" not in cols:
            df = df.with_columns(
                pl.col("quantidade_convertida").cast(pl.Float64).fill_null(0.0).alias("q_conv")
            )

        # `q_conv_fisica` deve representar a quantidade física (zero para estoques finais)
        if "q_conv_fisica" not in cols:
            df = df.with_columns(
                pl.when(is_estoque_final)
                .then(pl.lit(0.0))
                .otherwise(pl.col("q_conv").cast(pl.Float64, strict=False).fill_null(0.0))
                .alias("q_conv_fisica")
            )

        # Sinal da quantidade para cálculos de saldo sequencial
        if "__q_conv_sinal__" not in cols:
            df = df.with_columns(
                pl.when(pl.col("tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("0 - ESTOQUE INICIAL"))
                .then(pl.col("q_conv_fisica"))
                .when(pl.col("tipo_operacao") == "1 - ENTRADA")
                .then(pl.col("q_conv_fisica"))
                .when(pl.col("tipo_operacao") == "2 - SAIDAS")
                .then(-pl.col("q_conv_fisica"))
                .otherwise(pl.lit(0.0))
                .alias("__q_conv_sinal__")
            )

        # Preço unitário derivado quando houver valor agregado (evita divisão por zero)
        if "preco_unit" not in cols:
            preco_expr = None
            if "preco_item" in cols:
                preco_expr = pl.col("preco_item").cast(pl.Float64, strict=False).fill_null(0.0)
            elif "Vl_item" in cols:
                preco_expr = pl.col("Vl_item").cast(pl.Float64, strict=False).fill_null(0.0)

            if preco_expr is not None:
                df = df.with_columns(
                    pl.when(pl.col("q_conv").cast(pl.Float64, strict=False).fill_null(0.0) > 0)
                    .then(preco_expr / pl.col("q_conv"))
                    .otherwise(pl.lit(0.0))
                    .alias("preco_unit")
                )

        return df

    @staticmethod
    def apply_conversion_factors(df: "pl.DataFrame", df_prod_final: "pl.DataFrame" | None = None) -> "pl.DataFrame":
        """Aplica fatores de conversão e resolve unidade de referência com preservação de overrides.

        Prioridade para `unidade_referencia`:
            1. `unidade_referencia_override` (quando presente)
            2. `unid_ref` (join de fatores)
            3. `unid_ref_sugerida` (do produto)
            4. coluna de unidade original detectada (p.ex. `Unid`, `unid`)
            5. None

        Prioridade para `fator_conversao`:
            1. `fator_conversao_override` (quando presente)
            2. `fator` (join de fatores físicos)
            3. 1.0 (fallback)

        A função preserva `fator_original` quando `fator` existir e atualiza `fator` com
        o valor efetivo a ser consumido pelo pipeline (para compatibilidade com cálculos
        existentes que usam a coluna `fator`).
        """
        cols = set(df.columns)

        # preservar fator original se existir
        if "fator" in cols and "fator_original" not in cols:
            df = df.with_columns(pl.col("fator").alias("fator_original"))

        # construir expressão para fator_conversao
        if "fator_conversao_override" in cols:
            fator_expr = (
                pl.when(pl.col("fator_conversao_override").is_not_null())
                .then(pl.col("fator_conversao_override").cast(pl.Float64))
            )
            if "fator" in cols:
                fator_expr = fator_expr.when(pl.col("fator").is_not_null()).then(pl.col("fator").cast(pl.Float64))
            fator_expr = fator_expr.otherwise(pl.lit(1.0)).alias("fator_conversao")
        else:
            if "fator" in cols:
                fator_expr = (
                    pl.when(pl.col("fator").is_not_null())
                    .then(pl.col("fator").cast(pl.Float64))
                    .otherwise(pl.lit(1.0))
                ).alias("fator_conversao")
            else:
                fator_expr = pl.lit(1.0).alias("fator_conversao")

        df = df.with_columns(fator_expr)

        # origem do fator
        if "fator_conversao_override" in cols:
            origem_expr = (
                pl.when(pl.col("fator_conversao_override").is_not_null())
                .then(pl.lit("manual"))
                .when(pl.col("fator").is_not_null())
                .then(pl.lit("fisico"))
                .otherwise(pl.lit("fallback_sem_dados"))
            ).alias("fator_conversao_origem")
        else:
            origem_expr = (
                pl.when(pl.col("fator").is_not_null())
                .then(pl.lit("fisico"))
                .otherwise(pl.lit("fallback_sem_dados"))
            ).alias("fator_conversao_origem")

        df = df.with_columns(origem_expr)

        # determinar unidade de referência
        candidate_unit_cols = [
            "unidade_referencia_override",
            "unid_ref",
            "unid_ref_sugerida",
            "unid",
            "Unid",
            "Unidade",
            "unidade",
            "unidade_medida",
            "unid_medida",
        ]
        unit_exprs = []
        for c in candidate_unit_cols:
            if c in cols:
                unit_exprs.append(pl.col(c).cast(pl.Utf8))

        # se for passado df_prod_final e id_agrupado estiver presente, anexar sugestão se faltar
        if df_prod_final is not None and "unid_ref_sugerida" in df_prod_final.columns and "id_agrupado" in cols and "unid_ref_sugerida" not in cols:
            prod_map = df_prod_final.select([pl.col("id_agrupado"), pl.col("unid_ref_sugerida")]).unique()
            df = df.join(prod_map, on="id_agrupado", how="left")
            if "unid_ref_sugerida" in df.columns:
                unit_exprs.append(pl.col("unid_ref_sugerida").cast(pl.Utf8))

        if unit_exprs:
            unidade_expr = pl.coalesce(unit_exprs).alias("unidade_referencia")
        else:
            unidade_expr = pl.lit(None).cast(pl.Utf8).alias("unidade_referencia")

        df = df.with_columns(unidade_expr)

        # normalizar e garantir colunas esperadas
        df = df.with_columns(
            pl.col("fator_conversao").cast(pl.Float64).abs(),
        )

        # atualizar coluna `fator` usada historicamente no pipeline
        df = df.with_columns(pl.col("fator_conversao").alias("fator"))

        # garantir que exista `unid_ref`
        if "unid_ref" not in df.columns:
            df = df.with_columns(pl.col("unidade_referencia").alias("unid_ref"))

        return df
