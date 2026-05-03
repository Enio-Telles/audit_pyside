"""Serviços mínimos para aplicar a metodologia MDS (derivação de quantidades)."""
from __future__ import annotations
from pathlib import Path
from typing import Any, Union

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
        """Carrega um arquivo Parquet e retorna como DataFrame Polars.

        Args:
            path: Caminho para o arquivo Parquet.

        Returns:
            DataFrame com os dados do arquivo.
        """
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
        # aplicar padrões de flags e garantir colunas auxiliares
        df = MovimentacaoService.apply_flag_defaults(df)

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

        # Aplicar arredondamento consistente para quantidades (4 casas decimais)
        df = df.with_columns(
            pl.col("quantidade_fisica").cast(pl.Float64, strict=False).round(4).alias("quantidade_fisica"),
            pl.col("quantidade_fisica_sinalizada").cast(pl.Float64, strict=False).round(4).alias("quantidade_fisica_sinalizada"),
            pl.col("estoque_final_declarado").cast(pl.Float64, strict=False).round(4).alias("estoque_final_declarado"),
            pl.col("q_conv").cast(pl.Float64, strict=False).round(4).alias("q_conv"),
            pl.col("q_conv_fisica").cast(pl.Float64, strict=False).round(4).alias("q_conv_fisica"),
            pl.col("__q_conv_sinal__").cast(pl.Float64, strict=False).round(4).alias("__q_conv_sinal__"),
        )

        # calcular preco_unit de forma centralizada
        df = MovimentacaoService.compute_preco_unit(df)

        # marcar devolucoes e linhas validas para calculo de medias
        df = MovimentacaoService.compute_is_devolucao(df)
        df = MovimentacaoService.mark_valid_for_average(df)

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
            fator_expr: Any = (
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

        # origem do fator (proteger referências quando coluna `fator` não existir)
        if "fator_conversao_override" in cols:
            if "fator" in cols:
                origem_expr = (
                    pl.when(pl.col("fator_conversao_override").is_not_null())
                    .then(pl.lit("manual"))
                    .when(pl.col("fator").is_not_null())
                    .then(pl.lit("fisico"))
                    .otherwise(pl.lit("fallback_sem_dados"))
                ).alias("fator_conversao_origem")
            else:
                origem_expr = (
                    pl.when(pl.col("fator_conversao_override").is_not_null())
                    .then(pl.lit("manual"))
                    .otherwise(pl.lit("fallback_sem_dados"))
                ).alias("fator_conversao_origem")
        else:
            if "fator" in cols:
                origem_expr = (
                    pl.when(pl.col("fator").is_not_null())
                    .then(pl.lit("fisico"))
                    .otherwise(pl.lit("fallback_sem_dados"))
                ).alias("fator_conversao_origem")
            else:
                origem_expr = pl.lit("fallback_sem_dados").alias("fator_conversao_origem")

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
        # normalizar e aplicar arredondamento para fator_conversao
        df = df.with_columns(
            pl.col("fator_conversao").cast(pl.Float64).abs().round(6).alias("fator_conversao"),
        )

        # atualizar coluna `fator` usada historicamente no pipeline
        df = df.with_columns(pl.col("fator_conversao").alias("fator"))

        # garantir que exista `unid_ref`
        if "unid_ref" not in df.columns:
            df = df.with_columns(pl.col("unidade_referencia").alias("unid_ref"))

        return df

    @staticmethod
    def _boolish_expr(coluna: str) -> pl.Expr:
        """Retorna uma expressão que interpreta valores "truthy" comuns como booleanos."""
        return (
            pl.col(coluna)
            .cast(pl.Utf8, strict=False)
            .fill_null("")
            .str.strip_chars()
            .str.to_uppercase()
            .is_in(["1", "TRUE", "T", "S", "SIM", "Y", "YES", "X"])
        )

    @staticmethod
    def apply_flag_defaults(df: "pl.DataFrame") -> "pl.DataFrame":
        """Garante existência das colunas de flags usadas pela metodologia.

        Mantém valores existentes quando presentes; adiciona colunas ausentes como None.
        """
        for col in ["mov_rep", "excluir_estoque", "dev_simples", "dev_venda", "dev_compra", "dev_ent_simples"]:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))
        return df

    @staticmethod
    def compute_is_devolucao(df: "pl.DataFrame") -> "pl.DataFrame":
        """Marca linhas que representam devoluções (venda/compra/simples/entrada simples).

        Considera também o campo `finnfe` igual a '4' como devolução.
        """
        cols = set(df.columns)
        parts = []
        for c in ["dev_simples", "dev_venda", "dev_compra", "dev_ent_simples"]:
            if c in cols:
                parts.append(MovimentacaoService._boolish_expr(c))
            else:
                parts.append(pl.lit(False))

        if "finnfe" in cols:
            finnfe_part = pl.col("finnfe").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars() == "4"
        else:
            finnfe_part = pl.lit(False)

        devol_expr = parts[0] | parts[1] | parts[2] | parts[3] | finnfe_part
        return df.with_columns(devol_expr.alias("__is_devolucao__"))

    @staticmethod
    def mark_valid_for_average(df: "pl.DataFrame") -> "pl.DataFrame":
        """Marca linhas válidas para cálculo de médias (pme/pms).

        Regras: não é devolução; não está marcada como excluir_estoque; q_conv_fisica > 0.
        """
        cols = set(df.columns)
        if "excluir_estoque" in cols:
            excl_expr = MovimentacaoService._boolish_expr("excluir_estoque")
        else:
            excl_expr = pl.lit(False)

        if "q_conv_fisica" in cols:
            qtd_ok = pl.col("q_conv_fisica").cast(pl.Float64, strict=False).fill_null(0.0) > 0
        else:
            qtd_ok = pl.lit(False)

        valid_expr = (~pl.col("__is_devolucao__")) & (~excl_expr) & qtd_ok
        return df.with_columns(valid_expr.alias("__is_valida_media__"))

    @staticmethod
    def compute_preco_unit(df: "pl.DataFrame") -> "pl.DataFrame":
        """Calcula ou normaliza a coluna `preco_unit` (preco por unidade convertida)."""
        cols = set(df.columns)
        if "preco_unit" in cols:
            return df

        preco_expr = None
        if "preco_item" in cols:
            preco_expr = pl.col("preco_item").cast(pl.Float64, strict=False).fill_null(0.0)
        elif "Vl_item" in cols:
            preco_expr = pl.col("Vl_item").cast(pl.Float64, strict=False).fill_null(0.0)

        if preco_expr is None:
            return df

        df = df.with_columns(
            pl.when(pl.col("q_conv").cast(pl.Float64, strict=False).fill_null(0.0) > 0)
            .then(preco_expr / pl.col("q_conv"))
            .otherwise(pl.lit(0.0))
            .alias("preco_unit")
        )
        return df

    @staticmethod
    def apply_neutralizations(
        df: "pl.DataFrame",
        persist_neutralized: bool = False,
        output_dir: Union[str, Path] | None = None,
        cnpj: str | None = None,
    ) -> "pl.DataFrame":
        """Marca linhas neutralizadas por duplicidade ou flag de exclusao.

        Se `persist_neutralized` for True e `output_dir` + `cnpj` forem informados,
        grava as linhas neutralizadas em um parquet para rastreabilidade.
        """
        if df.is_empty() or "Num_item" not in df.columns:
            return df

        cols = set(df.columns)

        candidatos: list[pl.Expr] = []
        if "Chv_nfe" in cols:
            candidatos.append(
                pl.col("Chv_nfe").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
            )

        if "num_doc" in cols:
            col_emitente = next((c for c in ["cnpj_emitente", "cnpj_participante", "co_emitente", "emit_cnpj_cpf"] if c in cols), None)
            col_serie = next((c for c in ["Serie", "serie", "ser"] if c in cols), None)
            partes = []
            if col_emitente:
                partes.append(pl.col(col_emitente).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars())
            if col_serie:
                partes.append(pl.col(col_serie).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars())
            partes.append(pl.col("num_doc").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars())
            candidatos.append(pl.concat_str(partes, separator="|"))

        if "id_linha_origem" in cols:
            candidatos.append(pl.col("id_linha_origem").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars())
        elif "num_doc" in cols:
            candidatos.append(pl.col("num_doc").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars())

        if not candidatos:
            return df

        df = df.with_columns(pl.coalesce(candidatos).fill_null("").alias("__chave_doc__"))
        if df.is_empty() or "Num_item" not in df.columns:
            return df.drop("__chave_doc__")

        id_doc_expr = pl.coalesce([pl.col(c).cast(pl.Utf8, strict=False).str.strip_chars() for c in [c for c in ["Chv_nfe", "num_doc", "id_linha_origem"] if c in cols]]).fill_null("")
        item_expr = pl.col("Num_item").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()

        df = df.with_columns(id_doc_expr.alias("__chave_doc__"))
        repetido_expr = (
            (pl.col("__chave_doc__") != "")
            & (item_expr != "")
            & (pl.len().over(["__chave_doc__", "Num_item"]) > 1)
        )

        if "mov_rep" in cols:
            df = df.with_columns((repetido_expr | MovimentacaoService._boolish_expr("mov_rep")).alias("mov_rep"))
        else:
            df = df.with_columns(repetido_expr.alias("mov_rep"))

        # linha neutra: duplicidade ou flag explicita de exclusao
        excl_expr = MovimentacaoService._boolish_expr("excluir_estoque") if "excluir_estoque" in cols else pl.lit(False)
        linha_neutra = (excl_expr | MovimentacaoService._boolish_expr("mov_rep"))
        df = df.with_columns(linha_neutra.alias("__is_neutralizada__"))

        # persistir artefato opcional
        if persist_neutralized and output_dir and cnpj:
            try:
                df_neu = df.filter(pl.col("__is_neutralizada__"))
                if not df_neu.is_empty():
                    out_path = Path(output_dir) / f"linhas_neutralizadas_duplicidade_{cnpj}.parquet"
                    df_neu.write_parquet(str(out_path))
            except Exception:
                # não interromper o fluxo principal por falha de gravação
                pass

        return df.drop("__chave_doc__")
