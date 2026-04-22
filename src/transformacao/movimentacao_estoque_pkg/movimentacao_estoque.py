import json
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
    from utilitarios.codigo_fonte import (
        expr_gerar_codigo_fonte,
        expr_normalizar_codigo_fonte,
    )
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
    from metodologia_mds.service import MovimentacaoService
except ImportError as e:
    rprint(f"[red]Erro ao importar modulos:[/red] {e}")
    sys.exit(1)


def marcar_mov_rep_por_chave_item(df: pl.DataFrame) -> pl.DataFrame:
    """Marca duplicidade com chave documental forte antes de recorrer a fallbacks fracos."""
    if df.is_empty() or "Num_item" not in df.columns:
        return df

    candidatos: list[pl.Expr] = []
    if "Chv_nfe" in df.columns:
        candidatos.append(pl.col("Chv_nfe").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars())

    if "num_doc" in df.columns:
        col_emitente = next((c for c in ["cnpj_emitente", "cnpj_participante", "co_emitente", "emit_cnpj_cpf"] if c in df.columns), None)
        col_serie = next((c for c in ["Serie", "serie", "ser"] if c in df.columns), None)
        partes = []
        if col_emitente:
            partes.append(pl.col(col_emitente).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars())
        if col_serie:
            partes.append(pl.col(col_serie).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars())
        partes.append(pl.col("num_doc").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars())
        candidatos.append(pl.concat_str(partes, separator="|"))

    if "id_linha_origem" in df.columns:
        candidatos.append(pl.col("id_linha_origem").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars())
    elif "num_doc" in df.columns:
        candidatos.append(pl.col("num_doc").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars())

    if not candidatos:
        return df

    if "Num_item" not in df.columns:
        return df

    # Chave de documento com fallback multinível para capturar C170 sem NF-e vinculada
    # Construir candidatos robustos: preferir Chv_nfe, depois emitente|serie|num_doc,
    # depois id_linha_origem e por fim num_doc simples.
    id_candidates: list[pl.Expr] = []
    # Candidate 1: Chv_nfe, but treat empty string as null so coalesce skips it
    if "Chv_nfe" in df.columns:
        val = pl.col("Chv_nfe").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
        id_candidates.append(pl.when(val == "").then(pl.lit(None)).otherwise(val))

    # Candidate 2: emitente|serie|num_doc (or num_doc alone) - prefer when emitente present
    if "num_doc" in df.columns:
        col_emitente = next((c for c in ["cnpj_emitente", "cnpj_participante", "co_emitente", "emit_cnpj_cpf"] if c in df.columns), None)
        col_serie = next((c for c in ["Serie", "serie", "ser"] if c in df.columns), None)
        partes = []
        if col_emitente:
            partes.append(pl.col(col_emitente).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars())
        if col_serie:
            partes.append(pl.col(col_serie).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars())
        partes.append(pl.col("num_doc").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars())
        val2 = pl.concat_str(partes, separator="|")
        id_candidates.append(pl.when(val2 == "").then(pl.lit(None)).otherwise(val2))

    # Candidate 3: id_linha_origem or fallback num_doc
    if "id_linha_origem" in df.columns:
        val3 = pl.col("id_linha_origem").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
        id_candidates.append(pl.when(val3 == "").then(pl.lit(None)).otherwise(val3))
    elif "num_doc" in df.columns:
        val4 = pl.col("num_doc").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
        id_candidates.append(pl.when(val4 == "").then(pl.lit(None)).otherwise(val4))

    if not id_candidates:
        return df

    id_doc_expr = pl.coalesce(id_candidates).fill_null("")
    df = df.with_columns(id_doc_expr.alias("__chave_doc__"))

    item_expr = pl.col("Num_item").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()

    # Compute group sizes per (__chave_doc__, Num_item) robustly and mark duplicates
    try:
        grp = df.group_by(["__chave_doc__", "Num_item"]).agg(pl.len().alias("__grp_count__"))
        df = df.join(grp, on=["__chave_doc__", "Num_item"], how="left")
        repetido_expr = (
            (pl.col("__chave_doc__") != "")
            & (item_expr != "")
            & (pl.col("__grp_count__").fill_null(0) > 1)
        )
    except Exception:
        # Fallback to window expression if groupby/join fails for any reason
        repetido_expr = (
            (pl.col("__chave_doc__") != "")
            & (item_expr != "")
            & (pl.len().over(["__chave_doc__", "Num_item"]) > 1)
        )

    # Debugging helper: when DEBUG_MOV_REP env var is set, print intermediate values
    try:
        import os

        if os.environ.get("DEBUG_MOV_REP"):
            df_debug = df.with_columns(repetido_expr.alias("_rep_debug"))
            print("DEBUG __chave_doc__:", df_debug["__chave_doc__"].to_list())
            print("DEBUG Num_item:", df_debug["Num_item"].to_list())
            print("DEBUG __grp_count__:", df_debug.get_column("__grp_count__").to_list() if "__grp_count__" in df_debug.columns else [])
            print("DEBUG _rep_debug:", df_debug["_rep_debug"].to_list())
    except Exception:
        pass

    if "mov_rep" in df.columns:
        df = df.with_columns((repetido_expr | _boolish_expr("mov_rep").fill_null(False)).alias("mov_rep"))
    else:
        df = df.with_columns(repetido_expr.alias("mov_rep"))

    # Clean up helper column
    if "__grp_count__" in df.columns:
        df = df.drop("__grp_count__")

    return df.drop("__chave_doc__")


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


def _construir_vinculos_produto(arq_prod_final: Path, arq_mapa: Path):
    df_prod_base = pl.read_parquet(arq_prod_final)
    df_prod_por_id = (
        df_prod_base
        .select([
            pl.col("id_agrupado").cast(pl.Utf8, strict=False),
            pl.col("descr_padrao").cast(pl.Utf8, strict=False),
            pl.col("ncm_padrao").cast(pl.Utf8, strict=False),
            pl.col("cest_padrao").cast(pl.Utf8, strict=False),
            pl.col("descricao_final").cast(pl.Utf8, strict=False),
            pl.col("co_sefin_final").cast(pl.Utf8, strict=False),
            pl.col("unid_ref_sugerida").cast(pl.Utf8, strict=False),
        ])
        .rename({"co_sefin_final": "co_sefin_agr"})
        .unique(subset=["id_agrupado"])
    )

    df_desc = (
        df_prod_base
        .select([
            pl.col("descricao_normalizada").cast(pl.Utf8, strict=False),
            pl.col("id_agrupado").cast(pl.Utf8, strict=False),
            pl.col("descr_padrao").cast(pl.Utf8, strict=False),
            pl.col("ncm_padrao").cast(pl.Utf8, strict=False),
            pl.col("cest_padrao").cast(pl.Utf8, strict=False),
            pl.col("descricao_final").cast(pl.Utf8, strict=False),
            pl.col("co_sefin_final").cast(pl.Utf8, strict=False),
            pl.col("unid_ref_sugerida").cast(pl.Utf8, strict=False),
        ])
        .group_by("descricao_normalizada")
        .agg([
            pl.col("id_agrupado").n_unique().alias("__qtd_ids__"),
            pl.col("id_agrupado").first().alias("id_agrupado"),
            pl.col("descr_padrao").drop_nulls().first().alias("descr_padrao"),
            pl.col("ncm_padrao").drop_nulls().first().alias("ncm_padrao"),
            pl.col("cest_padrao").drop_nulls().first().alias("cest_padrao"),
            pl.col("descricao_final").drop_nulls().first().alias("descricao_final"),
            pl.col("co_sefin_final").drop_nulls().first().alias("co_sefin_agr"),
            pl.col("unid_ref_sugerida").drop_nulls().first().alias("unid_ref_sugerida"),
        ])
        .filter(pl.col("__qtd_ids__") == 1)
        .drop("__qtd_ids__")
        .with_columns(pl.lit("descricao_normalizada").alias("origem_vinculo_produto"))
    )

    df_codigo = pl.DataFrame(schema={"codigo_fonte": pl.Utf8, "id_agrupado": pl.Utf8, "descr_padrao": pl.Utf8, "ncm_padrao": pl.Utf8, "cest_padrao": pl.Utf8, "descricao_final": pl.Utf8, "co_sefin_agr": pl.Utf8, "unid_ref_sugerida": pl.Utf8, "origem_vinculo_produto": pl.Utf8})
    if arq_mapa.exists():
        df_codigo = (
            pl.read_parquet(arq_mapa)
            .select([
                expr_normalizar_codigo_fonte("codigo_fonte"),
                pl.col("id_agrupado").cast(pl.Utf8, strict=False),
            ])
            .drop_nulls(["codigo_fonte", "id_agrupado"])
            .group_by("codigo_fonte")
            .agg([
                pl.col("id_agrupado").n_unique().alias("__qtd_ids__"),
                pl.col("id_agrupado").first().alias("id_agrupado"),
            ])
            .filter(pl.col("__qtd_ids__") == 1)
            .drop("__qtd_ids__")
            .join(df_prod_por_id, on="id_agrupado", how="left")
            .with_columns(pl.lit("codigo_fonte").alias("origem_vinculo_produto"))
        )

    return df_codigo, df_desc
def _salvar_log_vinculo_produto(resumo: dict, pasta_analises: Path, cnpj: str) -> None:
    """Persiste resumo do vínculo de produto para rastreabilidade."""
    caminho = pasta_analises / f"log_vinculo_produto_estoque_{cnpj}.json"
    try:
        with open(caminho, "w", encoding="utf-8") as f:
            json.dump(resumo, f, ensure_ascii=False, indent=2)
    except Exception as exc:
        rprint(f"[yellow]Aviso: falha ao salvar log de vínculo: {exc}[/yellow]")


def _df_vazio_vinculo_produto() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "descricao_normalizada": pl.Utf8,
            "id_agrupado": pl.Utf8,
            "descr_padrao": pl.Utf8,
            "ncm_padrao": pl.Utf8,
            "cest_padrao": pl.Utf8,
            "descricao_final": pl.Utf8,
            "co_sefin_final": pl.Utf8,
            "unid_ref_sugerida": pl.Utf8,
            "origem_vinculo_produto": pl.Utf8,
            }
        )


def _construir_vinculo_unico_por_descricao(df: pl.DataFrame, origem: str) -> tuple[pl.DataFrame, dict]:
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
                pl.col("descr_padrao").cast(pl.Utf8, strict=False).alias("descr_padrao"),
                pl.col("ncm_padrao").cast(pl.Utf8, strict=False).alias("ncm_padrao"),
                pl.col("cest_padrao").cast(pl.Utf8, strict=False).alias("cest_padrao"),
                pl.col("descricao_final").cast(pl.Utf8, strict=False).alias("descricao_final"),
                pl.col("co_sefin_final").cast(pl.Utf8, strict=False).alias("co_sefin_final"),
                pl.col("unid_ref_sugerida").cast(pl.Utf8, strict=False).alias("unid_ref_sugerida"),
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
                pl.col("descr_padrao").drop_nulls().first().alias("descr_padrao"),
                pl.col("ncm_padrao").drop_nulls().first().alias("ncm_padrao"),
                pl.col("cest_padrao").drop_nulls().first().alias("cest_padrao"),
                pl.col("descricao_final").drop_nulls().first().alias("descricao_final"),
                pl.col("co_sefin_final").drop_nulls().first().alias("co_sefin_final"),
                pl.col("unid_ref_sugerida").drop_nulls().first().alias("unid_ref_sugerida"),
            ]
        )
    )
    resumo = {
        "origem": origem,
        "qtd_descricoes_unicas": int(df_grouped.filter(pl.col("__qtd_ids__") == 1).height),
        "qtd_descricoes_ambiguas": int(df_grouped.filter(pl.col("__qtd_ids__") > 1).height),
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
    df_prod_final: pl.DataFrame,
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
                    df_prod_final
                    .select(
                        [
                            "id_agrupado",
                            "descr_padrao",
                            "ncm_padrao",
                            "cest_padrao",
                            "descricao_final",
                            "co_sefin_final",
                            "unid_ref_sugerida",
                        ]
                    )
                    .unique(subset=["id_agrupado"], keep="first"),
                    on="id_agrupado",
                    how="left",
                )
            )
            df_map, resumo_map = _construir_vinculo_unico_por_descricao(df_map_raw, "map_produto_agrupado")
            resumo["map_produto_agrupado"] = resumo_map
            if not df_map.is_empty():
                bases.append(df_map.with_columns(pl.lit(0).alias("__ordem_vinculo__")))

    df_final_base, resumo_final = _construir_vinculo_unico_por_descricao(df_prod_final, "produtos_final")
    resumo["produtos_final"] = resumo_final
    if not df_final_base.is_empty():
        bases.append(df_final_base.with_columns(pl.lit(1).alias("__ordem_vinculo__")))

    if not bases:
        _salvar_log_vinculo_produto(
            {**resumo, "resultado": {"qtd_descricoes_vinculadas": 0}},
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
    _salvar_log_vinculo_produto({**resumo, "resultado": {"qtd_descricoes_vinculadas": int(df_vinculo.height)}}, pasta_analises, cnpj)
    return df_vinculo


# ===========================================================================
# Funcoes de calculo de saldo para map_groups
# ===========================================================================


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
    arq_mapa = pasta_analises / f"map_produto_agrupado_{cnpj}.parquet"
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
    )

    # Construir vinculo canonico: prioriza map_produto_agrupado_<cnpj>.parquet
    df_vinculo_produto = _carregar_vinculo_produto_canonico(pasta_analises, cnpj, df_prod_final)

    df_fatores = (
        pl.read_parquet(arq_fatores)
        .select(["id_agrupado", "unid", "unid_ref", "fator"])
        .rename({"unid": "__unid_fator__"})
        .unique(subset=["id_agrupado", "__unid_fator__"])
    )
    df_vinculo_codigo, df_vinculo_desc = _construir_vinculos_produto(arq_prod_final, arq_mapa)
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

        if "id_agrupado" not in df_raw.columns:
            if "codigo_fonte" in df_raw.columns:
                df_raw = df_raw.with_columns(expr_normalizar_codigo_fonte("codigo_fonte"))
            else:
                for cand in ["Cod_item", "cod_item", "prod_cprod", "codigo_produto", "codigo_produto_original"]:
                    if cand in df_raw.columns:
                        df_raw = df_raw.with_columns([pl.lit(cnpj).alias("__cnpj_ref__"), expr_gerar_codigo_fonte("__cnpj_ref__", cand, alias="codigo_fonte")]).drop("__cnpj_ref__", strict=False)
                        break

            if "codigo_fonte" in df_raw.columns and not df_vinculo_codigo.is_empty():
                df_raw = df_raw.join(df_vinculo_codigo, on="codigo_fonte", how="left")

            if col_desc and not df_vinculo_desc.is_empty():
                df_raw = df_raw.with_columns(_normalizar_descricao_expr(col_desc))
                sem_match = df_raw.filter(pl.col("id_agrupado").is_null()) if "id_agrupado" in df_raw.columns else df_raw
                if not sem_match.is_empty():
                    sem_match = (
                        sem_match.drop([c for c in ["id_agrupado", "descr_padrao", "ncm_padrao", "cest_padrao", "descricao_final", "co_sefin_agr", "unid_ref_sugerida", "origem_vinculo_produto"] if c in sem_match.columns])
                        .join(df_vinculo_desc.rename({"descricao_normalizada": "__descricao_normalizada__"}), on="__descricao_normalizada__", how="left")
                    )
                    com_match = df_raw.filter(pl.col("id_agrupado").is_not_null()) if "id_agrupado" in df_raw.columns else pl.DataFrame()
                    df_raw = pl.concat([com_match, sem_match], how="diagonal_relaxed")
                if "__descricao_normalizada__" in df_raw.columns:
                    df_raw = df_raw.drop("__descricao_normalizada__", strict=False)
        if col_desc and "id_agrupado" not in df_raw.columns:
            df_raw = (
                df_raw
                .with_columns(_normalizar_descricao_expr(col_desc))
                .join(
                    df_vinculo_produto.rename({"descricao_normalizada": "__descricao_normalizada__"}),
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
        cols_extras_manter = ["id_agrupado", "ncm_padrao", "cest_padrao", "descr_padrao", "co_sefin_agr", "unid_ref", "fator", "origem_vinculo_produto"]

        cols_extras_manter = ["id_agrupado", "ncm_padrao", "cest_padrao", "descr_padrao", "co_sefin_agr", "unid_ref", "fator", "origem_vinculo_produto", "origem_evento_estoque", "evento_sintetico"]
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

    if "mov_rep" in df_final.columns:
        df_neutralizadas = df_final.filter(_boolish_expr("mov_rep").fill_null(False))
        if df_neutralizadas.height > 0:
            salvar_para_parquet(
                df_neutralizadas,
                pasta_analises,
                f"linhas_neutralizadas_duplicidade_{cnpj}.parquet",
            )

    df_final = (
        df_final
        .with_columns(
            [
                pl.coalesce([
                    pl.col("Dt_e_s").cast(pl.Date, strict=False),
                    pl.col("Dt_doc").cast(pl.Date, strict=False),
                ]).alias("__data_ord__"),
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
        .sort(["id_agrupado", "__data_ord__", "__ord_tipo__", "__nsu_ord__", "Dt_doc", "Dt_e_s"], descending=[False, False, False, False, False, False], nulls_last=True)
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
    # Aplicar fatores de conversão centralizados e preservar overrides
    try:
        df_final = MovimentacaoService.apply_conversion_factors(df_final, df_prod_final)
    except Exception as exc:
        rprint(f"[yellow]Aviso: falha ao aplicar fatores de conversao: {exc}[/yellow]")
    linha_neutra_expr = (_boolish_expr("excluir_estoque").fill_null(False) | _boolish_expr("mov_rep").fill_null(False))
    # garantir coluna de protoco de autorizacao quando ausente (compatibilidade retroativa)
    if "infprot_cstat" not in df_final.columns:
        df_final = df_final.with_columns(pl.lit("").alias("infprot_cstat"))
    infprot_valido_expr = (
        pl.col("infprot_cstat").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars().is_in(["", "100", "150"])
    )
    q_conv_valido_expr = pl.when(infprot_valido_expr & ~linha_neutra_expr).then(q_conv_base_expr).otherwise(pl.lit(0.0))

    df_final = (
        df_final
        .with_columns([
            data_ref_expr.alias("__data_ref_calc__"),
            data_ref_expr.dt.year().alias("__ano_saldo__"),
            q_conv_base_expr.alias("__q_conv_base__"),
        ])
        .with_columns(
            pl.when(pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("0"))
            .then(1)
            .otherwise(0)
            .cum_sum()
            .over("id_agrupado")
            .alias("periodo_inventario")
        )
        .with_columns([
            pl.when(pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("3 - ESTOQUE FINAL"))
            .then(q_conv_valido_expr)
            .otherwise(pl.lit(0.0))
            .alias("__qtd_decl_final_audit__"),
            pl.when(pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("0 - ESTOQUE INICIAL"))
            .then(q_conv_valido_expr)
            .when(pl.col("Tipo_operacao") == "1 - ENTRADA")
            .then(q_conv_valido_expr)
            .when(pl.col("Tipo_operacao") == "2 - SAIDAS")
            .then(q_conv_valido_expr)
            .when(pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("3 - ESTOQUE FINAL"))
            .then(q_conv_valido_expr)
            .otherwise(pl.lit(0.0))
            .alias("q_conv"),
            pl.when(pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("0 - ESTOQUE INICIAL"))
            .then(q_conv_valido_expr)
            .when(pl.col("Tipo_operacao") == "1 - ENTRADA")
            .then(q_conv_valido_expr)
            .when(pl.col("Tipo_operacao") == "2 - SAIDAS")
            .then(q_conv_valido_expr)
            .otherwise(pl.lit(0.0))
            .alias("q_conv_fisica"),
            pl.when(pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("0 - ESTOQUE INICIAL"))
            .then(q_conv_valido_expr)
            .when(pl.col("Tipo_operacao") == "1 - ENTRADA")
            .then(q_conv_valido_expr)
            .when(pl.col("Tipo_operacao") == "2 - SAIDAS")
            .then(-q_conv_valido_expr)
            .otherwise(pl.lit(0.0))
            .alias("__q_conv_sinal__"),
        ])
        .with_columns(
            pl.when(pl.col("q_conv") > 0)
            .then(pl.col("preco_item").cast(pl.Float64, strict=False).fill_null(0.0) / pl.col("q_conv"))
            .otherwise(pl.lit(0.0))
            .alias("preco_unit")
        )
        .with_columns([
            # alinhar colunas esperadas pelo serviço de metodologia
            pl.col("q_conv").alias("quantidade_convertida"),
            pl.col("Tipo_operacao").alias("tipo_operacao"),
        ])
        .pipe(MovimentacaoService.derive_quantities)
        .group_by("id_agrupado", "__ano_saldo__", maintain_order=True)
        .map_groups(_calcular_saldo_estoque_anual)
        .group_by("id_agrupado", "periodo_inventario", maintain_order=True)
        .map_groups(_calcular_saldo_estoque_periodo)
        .with_columns([
            pl.when(pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("3 - ESTOQUE FINAL"))
            .then(pl.col("saldo_estoque_anual") - pl.col("__qtd_decl_final_audit__"))
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias("delta_decl_final_anual"),
            pl.when(pl.col("Tipo_operacao").cast(pl.Utf8, strict=False).str.starts_with("3 - ESTOQUE FINAL"))
            .then(pl.col("saldo_estoque_periodo") - pl.col("__qtd_decl_final_audit__"))
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias("delta_decl_final_periodo"),
        ])
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
