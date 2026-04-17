import sys
import re
from pathlib import Path

import polars as pl
from rich import print as rprint

from utilitarios.project_paths import PROJECT_ROOT, TRACEBACK_PATH

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
    from transformacao.co_sefin_class import enriquecer_co_sefin_class
    from transformacao.movimentacao_estoque_pkg.calculo_saldos import (
        _padronizar_tipo_operacao_expr,
        _boolish_expr,
        gerar_eventos_estoque as _gerar_eventos_estoque,
        calcular_saldo_estoque_anual as _calcular_saldo_estoque_anual,
        calcular_saldo_estoque_periodo as _calcular_saldo_estoque_periodo,
    )
    from transformacao.movimentacao_estoque_pkg.mapeamento_fontes import (
        normalizar_descricao_expr as _normalizar_descricao_expr,
        detectar_coluna_descricao as _detectar_coluna_descricao,
        detectar_coluna_unidade as _detectar_coluna_unidade,
        parse_expression as _parse_expression,
        carregar_flags_cfop as _carregar_flags_cfop,
    )
except ImportError as e:
    rprint(f"[red]Erro ao importar modulos:[/red] {e}")
    sys.exit(1)


def marcar_mov_rep_por_chave_item(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty() or "Chv_nfe" not in df.columns or "Num_item" not in df.columns:
        return df

    chave_expr = pl.col("Chv_nfe").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
    item_expr = pl.col("Num_item").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
    repetido_expr = (
        (chave_expr != "")
        & (item_expr != "")
        & (pl.len().over(["Chv_nfe", "Num_item"]) > 1)
    )

    if "mov_rep" in df.columns:
        return df.with_columns(
            (repetido_expr | _boolish_expr("mov_rep").fill_null(False)).alias("mov_rep")
        )

    return df.with_columns(repetido_expr.alias("mov_rep"))


def filtrar_movimentacoes_por_fonte(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty() or "fonte" not in df.columns or "Tipo_operacao" not in df.columns:
        return df

    fonte_expr = pl.col("fonte").cast(pl.Utf8, strict=False).str.to_lowercase()
    tipo_expr = pl.col("Tipo_operacao").cast(pl.Utf8, strict=False)
    filtro_expr = (
        pl.when(fonte_expr == "c170")
        .then(tipo_expr == "1 - ENTRADA")
        .when(fonte_expr.is_in(["nfe", "nfce"]))
        .then(tipo_expr == "2 - SAIDAS")
        .otherwise(pl.lit(True))
    )
    return df.filter(filtro_expr)


def gerar_movimentacao_estoque(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj = re.sub(r"\D", "", cnpj)
    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj

    pasta_analises = pasta_cnpj / "analises" / "produtos"
    pasta_brutos = pasta_cnpj / "arquivos_parquet"
    if not pasta_analises.exists():
        rprint(f"[red]Pasta de analises nao encontrada para o CNPJ: {cnpj}[/red]")
        return False

    arq_prod_final = pasta_analises / f"produtos_final_{cnpj}.parquet"
    arq_fatores = pasta_analises / f"fatores_conversao_{cnpj}.parquet"
    if not arq_prod_final.exists():
        rprint(f"[red]Arquivo necessario nao encontrado:[/red] {arq_prod_final}")
        return False
    if not arq_fatores.exists():
        rprint(f"[red]Arquivo necessario nao encontrado:[/red] {arq_fatores}")
        return False

    map_json = ROOT_DIR / "map_estoque.json"
    import json
    if not map_json.exists():
        rprint("[red]Arquivo map_estoque.json nao encontrado![/red]")
        return False
    with open(map_json, 'r', encoding='utf-8') as f:
        mapeamento = json.load(f)

    rprint(f"\n[bold cyan]Gerando movimentacao_estoque para CNPJ: {cnpj}[/bold cyan]")

    try:
        validar_parquet_essencial(
            arq_prod_final,
            [
                "id_agrupado",
                "descricao_normalizada",
                "descr_padrao",
                "ncm_padrao",
                "cest_padrao",
                "descricao_final",
                "co_sefin_final",
                "unid_ref_sugerida",
            ],
            contexto="movimentacao_estoque/produtos_final",
        )
        validar_parquet_essencial(
            arq_fatores,
            ["id_agrupado", "unid", "unid_ref", "fator"],
            contexto="movimentacao_estoque/fatores_conversao",
        )
    except SchemaValidacaoError as exc:
        rprint(f"[red]{exc}[/red]")
        return False

    df_prod_final = (
        pl.read_parquet(arq_prod_final)
        .select(
            [
                "id_agrupado",
                "descricao_normalizada",
                "descr_padrao",
                "ncm_padrao",
                "cest_padrao",
                "descricao_final",
                "co_sefin_final",
                "unid_ref_sugerida",
            ]
        )
        .unique(subset=["descricao_normalizada"])
    )

    df_fatores = (
        pl.read_parquet(arq_fatores)
        .select(["id_agrupado", "unid", "unid_ref", "fator"])
        .rename({"unid": "__unid_fator__"})
        .unique(subset=["id_agrupado", "__unid_fator__"])
    )
    df_flags_cfop = _carregar_flags_cfop()

    df_parts = []

    def _resolver_arquivo_origem(prefix_sys: str) -> Path | None:
        candidatos = [
            pasta_brutos / f"{prefix_sys}_agr_{cnpj}.parquet",
            pasta_analises / f"{prefix_sys}_agr_{cnpj}.parquet",
            pasta_brutos / f"{prefix_sys}_{cnpj}.parquet",
            pasta_cnpj / f"{prefix_sys}_{cnpj}.parquet",
            pasta_analises / f"{prefix_sys}_enriquecido_{cnpj}.parquet",
            pasta_brutos / f"{prefix_sys}_produtos_{cnpj}.parquet",
            pasta_analises / f"{prefix_sys}_produtos_{cnpj}.parquet",
        ]
        for arquivo in candidatos:
            if arquivo.exists():
                return arquivo
        return None

    def _process_source(prefix_sys: str, key_map: str):
        arquivo = _resolver_arquivo_origem(prefix_sys)
        if arquivo is None:
            return

        df_raw = pl.read_parquet(arquivo)
        col_desc = _detectar_coluna_descricao(df_raw, prefix_sys)
        col_unid = _detectar_coluna_unidade(df_raw, prefix_sys)

        if col_desc and "id_agrupado" not in df_raw.columns:
            df_raw = (
                df_raw
                .with_columns(_normalizar_descricao_expr(col_desc))
                .join(
                    df_prod_final.rename({"descricao_normalizada": "__descricao_normalizada__"}),
                    on="__descricao_normalizada__",
                    how="left",
                )
                .drop("__descricao_normalizada__")
            )

        if col_unid and ("fator" not in df_raw.columns or "unid_ref" not in df_raw.columns):
            df_raw = (
                df_raw
                .with_columns(pl.col(col_unid).cast(pl.String, strict=False).str.strip_chars().alias("__unid_fator__"))
                .join(df_fatores, on=["id_agrupado", "__unid_fator__"], how="left")
                .drop("__unid_fator__")
            )

        if "descricao_final" in df_raw.columns and "descr_padrao" not in df_raw.columns:
            df_raw = df_raw.with_columns(pl.col("descricao_final").alias("descr_padrao"))
        if "co_sefin_final" in df_raw.columns and "co_sefin_agr" not in df_raw.columns:
            df_raw = df_raw.with_columns(pl.col("co_sefin_final").alias("co_sefin_agr"))

        exprs = []
        cols_required = set()
        for m in mapeamento:
            target = m["Campo/tabela"]
            orig = m[key_map]
            try:
                exprs.append(_parse_expression(orig, target))
            except Exception:
                exprs.append(pl.lit(None).alias(target))
            v = str(orig)
            if v and v not in [
                "(vazio)",
                "correspondência com chave NF",
                "icms_orig & icms_cst ou icms_csosn",
                "prod_ceantrib ou caso for nulo -> prod_cean",
                "vl_item-vl_desc",
                "prod_vprod+prod_vfrete+prod_vseg+prod_voutro-prod_vdesc",
                "\"gerado\" ou \"registro\" (se está no bloco_h)",
            ] and not v.startswith('"'):
                cols_required.add(v)

        cols_required.update(["vl_item", "vl_desc", "prod_vprod", "prod_vfrete", "prod_vseg", "prod_voutro", "prod_vdesc", "icms_cst", "icms_orig", "icms_csosn", "prod_cean", "prod_ceantrib", "chv_nfe"])
        for c in cols_required:
            if c not in df_raw.columns:
                df_raw = df_raw.with_columns(pl.lit(None).alias(c))

        df_selecionado = df_raw.select(exprs)

        cols_extras_manter = ["id_agrupado", "ncm_padrao", "cest_padrao", "descr_padrao", "co_sefin_agr", "unid_ref", "fator", "origem_evento_estoque", "evento_sintetico"]
        for c in cols_extras_manter:
            if c in df_raw.columns:
                df_selecionado = df_selecionado.with_columns(df_raw[c])
            elif c == "origem_evento_estoque":
                df_selecionado = df_selecionado.with_columns(pl.lit("registro").alias(c))
            elif c == "evento_sintetico":
                df_selecionado = df_selecionado.with_columns(pl.lit(False).alias(c))
            else:
                df_selecionado = df_selecionado.with_columns(pl.lit(None).alias(c))

        df_selecionado = df_selecionado.with_columns(pl.lit(prefix_sys).alias("fonte"))
        df_parts.append(df_selecionado)

    _process_source("c170", "C170")
    _process_source("bloco_h", "Bloco_h")
    _process_source("nfe", "Nfe")
    _process_source("nfce", "Nfce")

    if not df_parts:
        rprint("[yellow]Nenhuma tabela de origem (enriquecida) foi encontrada.[/yellow]")
        return False

    df_mov = pl.concat(df_parts, how="diagonal_relaxed")
    df_mov = (
        df_mov
        .with_columns(_padronizar_tipo_operacao_expr("Tipo_operacao"))
        .pipe(filtrar_movimentacoes_por_fonte)
    )

    df_mov = _gerar_eventos_estoque(df_mov)

    rprint("[cyan]Enriquecendo campos co_sefin...[/cyan]")
    df_final = enriquecer_co_sefin_class(df_mov, cnpj)
    if not df_flags_cfop.is_empty() and "Cfop" in df_final.columns:
        df_final = (
            df_final
            .with_columns(pl.col("Cfop").cast(pl.Utf8, strict=False).str.replace_all(r"\D", "").alias("Cfop"))
            .join(df_flags_cfop, on="Cfop", how="left")
        )

    if "origem_evento_estoque" not in df_final.columns:
        df_final = df_final.with_columns(pl.lit("registro").alias("origem_evento_estoque"))
    if "evento_sintetico" not in df_final.columns:
        df_final = df_final.with_columns(pl.lit(False).alias("evento_sintetico"))

    for col in ["mov_rep", "excluir_estoque", "dev_simples", "dev_venda", "dev_compra", "dev_ent_simples"]:
        if col not in df_final.columns:
            df_final = df_final.with_columns(pl.lit(None).alias(col))
    df_final = marcar_mov_rep_por_chave_item(df_final)

    df_final = (
        df_final
        .with_columns(
            [
                pl.coalesce(
                    [
                        pl.col("Dt_e_s").cast(pl.Date, strict=False),
                        pl.col("Dt_doc").cast(pl.Date, strict=False),
                    ]
                ).alias("__data_ord__"),
                pl.col("nsu").cast(pl.Int64, strict=False).alias("__nsu_ord__"),
                pl.when(pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("0 - ESTOQUE INICIAL"))
                .then(pl.lit(0))
                .when(pl.col("Tipo_operacao") == "1 - ENTRADA")
                .then(pl.lit(1))
                .when(pl.col("Tipo_operacao") == "2 - SAIDAS")
                .then(pl.lit(2))
                .when(pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("3 - ESTOQUE FINAL"))
                .then(pl.lit(3))
                .otherwise(pl.lit(9))
                .alias("__ord_tipo__"),
            ]
        )
        .sort(
            ["id_agrupado", "__data_ord__", "__ord_tipo__", "__nsu_ord__", "Dt_doc", "Dt_e_s"],
            descending=[False, False, False, False, False, False],
            nulls_last=True,
        )
        .with_row_index("ordem_operacoes", offset=1)
        .drop(["__data_ord__", "__nsu_ord__", "__ord_tipo__"], strict=False)
    )

    data_ref_expr = pl.coalesce(
        [
            pl.col("Dt_e_s").cast(pl.Date, strict=False),
            pl.col("Dt_doc").cast(pl.Date, strict=False),
        ]
    )
    qtd_bruta_expr = pl.col("Qtd").cast(pl.Float64, strict=False).fill_null(0.0).abs()
    fator_expr = pl.col("fator").cast(pl.Float64, strict=False).fill_null(1.0).abs()
    q_conv_base_expr = (qtd_bruta_expr * fator_expr)
    linha_neutra_expr = (
        _boolish_expr("excluir_estoque").fill_null(False)
        | _boolish_expr("mov_rep").fill_null(False)
    )
    infprot_valido_expr = (
        pl.col("infprot_cstat")
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.strip_chars()
        .is_in(["", "100", "150"])
    )
    q_conv_valido_expr = (
        pl.when(infprot_valido_expr & ~linha_neutra_expr)
        .then(q_conv_base_expr)
        .otherwise(pl.lit(0.0))
    )

    df_final = (
        df_final
        .with_columns(
            [
                data_ref_expr.alias("__data_ref_calc__"),
                data_ref_expr.dt.year().alias("__ano_saldo__"),
                q_conv_base_expr.alias("__q_conv_base__"),
            ]
        )
        .with_columns(
            pl.when(pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("0"))
            .then(1)
            .otherwise(0)
            .cum_sum()
            .over("id_agrupado")
            .alias("periodo_inventario")
        )
        .with_columns(
            [
                pl.when(
                    pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("3 - ESTOQUE FINAL")
                )
                .then(q_conv_valido_expr)
                .otherwise(pl.lit(0.0))
                .alias("__qtd_decl_final_audit__"),
                pl.when(
                    pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("0 - ESTOQUE INICIAL")
                )
                .then(q_conv_valido_expr)
                .when(pl.col("Tipo_operacao") == "1 - ENTRADA")
                .then(q_conv_valido_expr)
                .when(pl.col("Tipo_operacao") == "2 - SAIDAS")
                .then(q_conv_valido_expr)
                .when(
                    pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("3 - ESTOQUE FINAL")
                )
                .then(q_conv_valido_expr)
                .otherwise(pl.lit(0.0))
                .alias("q_conv"),
                pl.when(
                    pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("0 - ESTOQUE INICIAL")
                )
                .then(q_conv_valido_expr)
                .when(pl.col("Tipo_operacao") == "1 - ENTRADA")
                .then(q_conv_valido_expr)
                .when(pl.col("Tipo_operacao") == "2 - SAIDAS")
                .then(q_conv_valido_expr)
                .otherwise(pl.lit(0.0))
                .alias("q_conv_fisica"),
                pl.when(
                    pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("0 - ESTOQUE INICIAL")
                )
                .then(q_conv_valido_expr)
                .when(pl.col("Tipo_operacao") == "1 - ENTRADA")
                .then(q_conv_valido_expr)
                .when(pl.col("Tipo_operacao") == "2 - SAIDAS")
                .then(-q_conv_valido_expr)
                .otherwise(pl.lit(0.0))
                .alias("__q_conv_sinal__"),
            ]
        )
        .with_columns(
            pl.when(pl.col("q_conv") > 0)
            .then(pl.col("preco_item").cast(pl.Float64, strict=False).fill_null(0.0) / pl.col("q_conv"))
            .otherwise(pl.lit(0.0))
            .alias("preco_unit")
        )
        .group_by("id_agrupado", "__ano_saldo__", maintain_order=True)
        .map_groups(_calcular_saldo_estoque_anual)
        .group_by("id_agrupado", "periodo_inventario", maintain_order=True)
        .map_groups(_calcular_saldo_estoque_periodo)
        .drop(["__data_ref_calc__", "__q_conv_base__"], strict=False)
    )

    saida = pasta_analises / f"mov_estoque_{cnpj}.parquet"
    ok = salvar_para_parquet(df_final, pasta_analises, saida.name)
    if ok:
        rprint(f"[green]Sucesso! {df_final.height} registros salvos.[/green]")

    return ok


if __name__ == "__main__":
    try:
        if len(sys.argv) > 1:
            gerar_movimentacao_estoque(sys.argv[1])
        else:
            c = input("CNPJ: ")
            gerar_movimentacao_estoque(c)
    except Exception as e:
        from transformacao.auxiliares.logs import setup_logging
        setup_logging().error("Erro na geracao de movimentacao de estoque", exc_info=e)
        raise
