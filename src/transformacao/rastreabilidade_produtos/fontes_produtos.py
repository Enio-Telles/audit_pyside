"""
fontes_produtos.py

Gera arquivos derivados das fontes brutas com a coluna `id_agrupado`
vinculada prioritariamente por `codigo_fonte` e, como fallback controlado,
por `descricao_normalizada`.

Saidas (em arquivos_parquet):
- c170_agr_<cnpj>.parquet
- bloco_h_agr_<cnpj>.parquet
- nfe_agr_<cnpj>.parquet
- nfce_agr_<cnpj>.parquet
- nfe_agr_fora_escopo_canonico_<cnpj>.parquet
- nfce_agr_fora_escopo_canonico_<cnpj>.parquet

Regra de consistencia:
- idealmente, toda linha deve possuir `id_agrupado`.
- quando existirem linhas sem `id_agrupado`, a rotina gera um arquivo de auditoria
  (`<fonte>_agr_sem_id_agrupado_<cnpj>.parquet`) e exclui essas linhas da saída
  principal; o pipeline NÃO falha.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import cast

import polars as pl
from rich import print as rprint

from utilitarios.project_paths import CFOP_BI_PATH, PROJECT_ROOT

ROOT_DIR = PROJECT_ROOT
SRC_DIR = ROOT_DIR / "src"
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"

try:
    from utilitarios.codigo_fonte import expr_gerar_codigo_fonte, expr_normalizar_codigo_fonte
    from utilitarios.salvar_para_parquet import salvar_para_parquet
    from utilitarios.text import expr_normalizar_descricao
    from utilitarios.validacao_schema import (
        SchemaValidacaoError,
        validar_parquet_essencial,
    )
    from utilitarios.schemas_agregacao import (
        COLUNAS_OBRIGATORIAS_FONTES_AGR,
        COLUNAS_RASTREABILIDADE_FONTES,
    )
except ImportError as e:
    rprint(f"[red]Erro ao importar modulos utilitarios:[/red] {e}")
    sys.exit(1)


def _normalizar_descricao_expr(col: str) -> pl.Expr:
    return cast(pl.Expr, expr_normalizar_descricao(col).alias("__descricao_normalizada__"))


_TOKENS_SAIDA: frozenset[str] = frozenset({"1", "S", "SAIDA", "SA\u00cdDA"})


def _tipo_operacao_indica_saida(col_name: str) -> pl.Expr:
    """Retorna expressao booleana que indica saida do emitente."""

    return (
        pl.col(col_name)
        .cast(pl.String, strict=False)
        .str.strip_chars()
        .str.to_uppercase()
        .str.extract(r"^([^\s\-/]+)")
        .fill_null("")
        .is_in(list(_TOKENS_SAIDA))
    )


def _deduplicar_colunas_preservando_ordem(colunas: list[str]) -> list[str]:
    return list(dict.fromkeys(colunas))


def _detectar_coluna_descricao(df: pl.DataFrame, fonte: str) -> str | None:
    candidatos = {
        "c170": ["descr_item", "descricao", "prod_xprod"],
        "bloco_h": ["descricao_produto", "descr_item", "descricao", "prod_xprod"],
        "nfe": ["prod_xprod", "descricao", "descr_item"],
        "nfce": ["prod_xprod", "descricao", "descr_item"],
    }
    for col in candidatos.get(fonte, []):
        if col in df.columns:
            return col
    return None


def _predicado_fora_escopo_canonico(
    df_src: pl.DataFrame, fonte: str, cnpj: str, cfops_mercantis: set[str] | None = None
) -> pl.Expr | None:
    """Marca como fora_escopo_canonico apenas terceiro->terceiro ou CFOP nao mercantil."""

    if fonte not in {"nfe", "nfce"}:
        return None

    criterios: list[pl.Expr] = []

    col_tp = next((c for c in ["tipo_operacao", "co_tp_nf", "tp_nf"] if c in df_src.columns), None)

    if "co_emitente" in df_src.columns and col_tp is not None:
        co_emitente = pl.col("co_emitente").cast(pl.String, strict=False).str.strip_chars()
        emitente_terceiro = co_emitente.is_not_null() & (co_emitente != "") & (co_emitente != cnpj)
        criterios.append(emitente_terceiro & _tipo_operacao_indica_saida(col_tp))

    if cfops_mercantis and "co_cfop" in df_src.columns:
        cfop_expr = pl.col("co_cfop").cast(pl.String, strict=False).str.strip_chars()
        criterios.append(
            cfop_expr.is_not_null() & (cfop_expr != "") & (~cfop_expr.is_in(cfops_mercantis))
        )

    if not criterios:
        return None

    predicado = criterios[0]
    for criterio in criterios[1:]:
        predicado = predicado | criterio
    return predicado


def _separar_fora_escopo_canonico(
    df_src: pl.DataFrame,
    fonte: str,
    cnpj: str,
    cfops_mercantis: set[str] | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame | None]:
    predicado = _predicado_fora_escopo_canonico(df_src, fonte, cnpj, cfops_mercantis)
    if predicado is None:
        return df_src, None

    df_fora_escopo = df_src.filter(predicado)
    if df_fora_escopo.is_empty():
        return df_src, None

    df_fora_escopo = df_fora_escopo.with_columns(
        pl.lit("fora_escopo_canonico").alias("motivo_fora_escopo_canonico")
    )
    return df_src.filter(~predicado), df_fora_escopo


def _salvar_auditoria_fora_escopo(
    df_fora_escopo: pl.DataFrame,
    pasta_analises: Path,
    fonte: str,
    cnpj: str,
) -> bool:
    if df_fora_escopo.is_empty():
        return True

    nome_arquivo = f"{fonte}_agr_fora_escopo_canonico_{cnpj}.parquet"
    ok: bool = bool(salvar_para_parquet(df_fora_escopo, pasta_analises, nome_arquivo))
    if ok:
        rprint(
            f"[yellow]Aviso: {fonte} possui {df_fora_escopo.height} linhas fora do escopo canonico. "
            f"Detalhes em {nome_arquivo}.[/yellow]"
        )
    return ok


def _candidatos_cfop_bi() -> list[Path]:
    return [
        CFOP_BI_PATH,
        DADOS_DIR / "referencias" / "referencias" / "cfop" / "cfop_bi.parquet",
        ROOT_DIR / "referencias" / "cfop" / "cfop_bi.parquet",
    ]


def _salvar_agr_vazio(
    pasta_brutos: Path,
    fonte: str,
    cnpj: str,
    schema: pl.Schema | None = None,
) -> bool:
    """Grava parquet vazio quando todas as linhas foram para fora_escopo_canonico.

    Substitui qualquer arquivo stale de run anterior, garantindo que o estado
    em disco reflita o run atual.
    """
    if schema is None:
        colunas = _deduplicar_colunas_preservando_ordem(
            list(COLUNAS_OBRIGATORIAS_FONTES_AGR) + list(COLUNAS_RASTREABILIDADE_FONTES)
        )
        df_vazio = pl.DataFrame({col: pl.Series([], dtype=pl.Utf8) for col in colunas})
    else:
        df_vazio = pl.DataFrame(schema=schema)
    return bool(salvar_para_parquet(df_vazio, pasta_brutos, f"{fonte}_agr_{cnpj}.parquet"))


def _carregar_cfops_mercantis() -> pl.DataFrame | None:
    caminho = next((p for p in _candidatos_cfop_bi() if p.exists()), None)
    if caminho is None:
        return None

    df = pl.read_parquet(caminho)
    if "co_cfop" not in df.columns or "operacao_mercantil" not in df.columns:
        return None

    return (
        df.select(
            [
                pl.col("co_cfop").cast(pl.String).str.strip_chars().alias("co_cfop"),
                pl.col("operacao_mercantil")
                .cast(pl.String)
                .str.strip_chars()
                .alias("operacao_mercantil"),
            ]
        )
        .filter(pl.col("operacao_mercantil") == "X")
        .unique(subset=["co_cfop"])
    )


# Colunas usadas pelas etapas downstream (c170_xml, etc.) para fontes grandes.
# None = carregar todas as colunas (comportamento original para fontes menores).
# Nfce pode ter 54 M+ linhas e 147 colunas; o pruning reduz a carga de ~1.9 GB para ~600 MB.
_COLUNAS_FONTE: dict[str, list[str] | None] = {
    "nfce": [
        "nsu",
        "chave_acesso",
        "prod_nitem",
        "prod_cprod",
        "prod_cean",
        "prod_ceantrib",
        "prod_ncm",
        "prod_cest",
        "prod_xprod",
        "co_cfop",
        "prod_ucom",
        "prod_qcom",
        "prod_vprod",
        "prod_vfrete",
        "prod_vseg",
        "prod_voutro",
        "prod_vdesc",
        "co_emitente",
        "co_tp_nf",
        "tp_nf",
        "tipo_operacao",
        "icms_orig",
        "icms_cst",
        "icms_csosn",
        "icms_picms",
        "icms_vbc",
        "icms_vicms",
        "icms_vbcst",
        "icms_vicmsst",
        "icms_picmsst",
        "ide_co_mod",
        "ide_serie",
        "nnf",
        "dhemi",
        "dhsaient",
        "co_uf_emit",
        "co_uf_dest",
        "co_finnfe",
    ],
}

# Fontes com volume > este limiar (linhas) sao processadas em batches anuais
# para evitar OOM em maquinas com RAM limitada.
_LIMIAR_BATCH_LINHAS = 5_000_000


def _ler_primeiro(
    arq_dir: Path, prefix: str, colunas: list[str] | None = None
) -> pl.DataFrame | None:
    # Tenta prefixo com separador primeiro para evitar que "nfe" case com "nfce".
    # Fallback aceita maiusculas (NFe_*.parquet) mas exige separador apos o prefixo.
    arquivos = sorted(arq_dir.glob(f"{prefix}_*.parquet"))
    if not arquivos:
        arquivos = sorted(arq_dir.glob(f"{prefix.upper()}_*.parquet"))
    if not arquivos:
        return None
    if colunas is None:
        return pl.read_parquet(arquivos[0])
    schema_cols = set(pl.scan_parquet(arquivos[0]).collect_schema().names())
    cols_existentes = _deduplicar_colunas_preservando_ordem(
        [c for c in colunas if c in schema_cols]
    )
    return pl.scan_parquet(arquivos[0]).select(cols_existentes).collect()


def _resolver_arquivo_fonte(arq_dir: Path, prefix: str) -> Path | None:
    arquivos = sorted(arq_dir.glob(f"{prefix}_*.parquet"))
    if not arquivos:
        arquivos = sorted(arq_dir.glob(f"{prefix.upper()}_*.parquet"))
    return arquivos[0] if arquivos else None


def _processar_fonte_em_batches_anuais(
    arquivo: Path,
    colunas: list[str] | None,
    df_mapa: pl.DataFrame,
    df_attrs: pl.DataFrame,
    pasta_analises: Path,
    pasta_brutos: Path,
    cnpj: str,
    fonte: str,
    cfops_mercantis: set[str] | None = None,
) -> bool:
    """Processa fonte com muitas linhas ano a ano, concatenando no parquet de saida."""
    schema_cols = set(pl.scan_parquet(arquivo).collect_schema().names())
    if colunas is not None:
        cols_sel = _deduplicar_colunas_preservando_ordem([c for c in colunas if c in schema_cols])
    else:
        cols_sel = list(schema_cols)

    if "dhemi" not in schema_cols:
        rprint(
            f"[yellow]{fonte}: sem coluna dhemi — batch anual impossivel; carregando completo.[/yellow]"
        )
        df_src = pl.scan_parquet(arquivo).select(cols_sel).collect()
        col_desc = _detectar_coluna_descricao(df_src, fonte)
        if not col_desc:
            return False
        exprs = _preservar_colunas_rastreabilidade(df_src, fonte=fonte)
        if exprs:
            df_src = df_src.with_columns(exprs)
        df_src, df_fora_escopo = _separar_fora_escopo_canonico(
            df_src,
            fonte=fonte,
            cnpj=cnpj,
            cfops_mercantis=cfops_mercantis,
        )
        if df_fora_escopo is not None and not _salvar_auditoria_fora_escopo(
            df_fora_escopo, pasta_analises, fonte, cnpj
        ):
            return False
        if df_src.is_empty():
            if df_fora_escopo is not None:
                schema_canonico = df_fora_escopo.drop("motivo_fora_escopo_canonico").schema
                ok = _salvar_agr_vazio(pasta_brutos, fonte, cnpj, schema=schema_canonico)
                if ok:
                    rprint(
                        f"[yellow]Aviso: todas as linhas de {fonte} foram para "
                        f"fora_escopo_canonico. {fonte}_agr_{cnpj}.parquet gravado vazio.[/yellow]"
                    )
                return ok
            return False
        df_out = _anexar_id_agrupado_por_codigo_ou_descricao(
            df_src, df_mapa, df_attrs, col_desc, pasta_analises, cnpj
        )
        return bool(salvar_para_parquet(df_out, pasta_brutos, f"{fonte}_agr_{cnpj}.parquet"))

    anos = (
        pl.scan_parquet(arquivo)
        .select(pl.col("dhemi").dt.year().alias("ano"))
        .unique()
        .collect()["ano"]
        .drop_nulls()
        .sort()
        .to_list()
    )
    rprint(f"[cyan]{fonte}: processamento em batches para anos {anos}[/cyan]")

    batches: list[pl.DataFrame] = []
    batches_fora_escopo: list[pl.DataFrame] = []
    for ano in anos:
        rprint(f"  [dim]batch {ano}...[/dim]", end=" ")
        df_ano = (
            pl.scan_parquet(arquivo)
            .filter(pl.col("dhemi").dt.year() == ano)
            .select(cols_sel)
            .collect()
        )
        rprint(f"{df_ano.height:,} linhas")

        col_desc = _detectar_coluna_descricao(df_ano, fonte)
        if not col_desc:
            continue

        exprs = _preservar_colunas_rastreabilidade(df_ano, fonte=fonte)
        if exprs:
            df_ano = df_ano.with_columns(exprs)
        df_ano, df_fora_escopo = _separar_fora_escopo_canonico(
            df_ano,
            fonte=fonte,
            cnpj=cnpj,
            cfops_mercantis=cfops_mercantis,
        )
        if df_fora_escopo is not None and not df_fora_escopo.is_empty():
            batches_fora_escopo.append(df_fora_escopo)

        if df_ano.is_empty():
            continue

        df_out_ano = _anexar_id_agrupado_por_codigo_ou_descricao(
            df_src=df_ano,
            df_mapa=df_mapa,
            df_attrs=df_attrs,
            col_desc=col_desc,
            pasta_analises=pasta_analises,
            cnpj=cnpj,
        )
        batches.append(df_out_ano)
        del df_ano, df_out_ano

    if batches_fora_escopo:
        df_fora_escopo_total = pl.concat(batches_fora_escopo, how="diagonal_relaxed")
        if not _salvar_auditoria_fora_escopo(df_fora_escopo_total, pasta_analises, fonte, cnpj):
            return False

    if not batches:
        if batches_fora_escopo:
            schema_canonico = df_fora_escopo_total.drop("motivo_fora_escopo_canonico").schema
            ok = _salvar_agr_vazio(pasta_brutos, fonte, cnpj, schema=schema_canonico)
            rprint(
                f"[yellow]Aviso: todas as linhas de {fonte} foram para "
                f"fora_escopo_canonico. {fonte}_agr_{cnpj}.parquet gravado vazio.[/yellow]"
            )
            return ok
        return False

    df_final = pl.concat(batches, how="diagonal_relaxed")
    del batches

    faltantes = df_final.filter(pl.col("id_agrupado").is_null())
    if faltantes.height > 0:
        salvar_para_parquet(
            faltantes, pasta_analises, f"{fonte}_agr_sem_id_agrupado_{cnpj}.parquet"
        )
        rprint(f"[yellow]Aviso: {fonte} possui {faltantes.height} linhas sem id_agrupado.[/yellow]")
        df_final = df_final.filter(pl.col("id_agrupado").is_not_null())

    if df_final.is_empty():
        ok = _salvar_agr_vazio(pasta_brutos, fonte, cnpj, schema=df_final.schema)
        rprint(
            f"[yellow]Aviso: {fonte} — todas as linhas restantes eram sem id_agrupado. "
            f"{fonte}_agr_{cnpj}.parquet gravado vazio.[/yellow]"
        )
        return ok

    if (
        "descricao_normalizada" not in df_final.columns
        and "__descricao_normalizada__" in df_final.columns
    ):
        df_final = df_final.rename({"__descricao_normalizada__": "descricao_normalizada"})
    else:
        df_final = df_final.drop("__descricao_normalizada__", strict=False)

    for col in COLUNAS_RASTREABILIDADE_FONTES:
        if col not in df_final.columns:
            df_final = df_final.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))

    return bool(salvar_para_parquet(df_final, pasta_brutos, f"{fonte}_agr_{cnpj}.parquet"))


def _construir_mapas(df_mapa: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    df_codigo = (
        df_mapa.filter(pl.col("codigo_fonte").is_not_null() & (pl.col("codigo_fonte") != ""))
        .group_by("codigo_fonte")
        .agg(pl.col("id_agrupado").drop_nulls().unique().sort().alias("ids"))
    )
    df_codigo_unico = (
        df_codigo.filter(pl.col("ids").list.len() == 1)
        .with_columns(pl.col("ids").list.first().alias("id_agrupado_codigo"))
        .select(["codigo_fonte", "id_agrupado_codigo"])
    )

    df_desc = (
        df_mapa.filter(
            pl.col("descricao_normalizada").is_not_null() & (pl.col("descricao_normalizada") != "")
        )
        .group_by("descricao_normalizada")
        .agg(pl.col("id_agrupado").drop_nulls().unique().sort().alias("ids"))
    )
    df_desc_unico = (
        df_desc.filter(pl.col("ids").list.len() == 1)
        .with_columns(pl.col("ids").list.first().alias("id_agrupado_desc"))
        .select(["descricao_normalizada", "id_agrupado_desc"])
    )
    df_desc_ambiguo = (
        df_desc.filter(pl.col("ids").list.len() > 1)
        .with_columns(pl.lit(True).alias("descricao_ambigua"))
        .select(["descricao_normalizada", "descricao_ambigua", "ids"])
    )
    return df_codigo_unico, df_desc_unico, df_desc_ambiguo


def _preservar_colunas_rastreabilidade(
    df_src: pl.DataFrame, fonte: str | None = None
) -> list[pl.Expr]:
    exprs: list[pl.Expr] = []
    if "codigo_fonte" not in df_src.columns:
        col_codigo = None
        for cand in ["codigo_produto", "codigo_produto_original", "cod_item", "prod_cprod"]:
            if cand in df_src.columns:
                col_codigo = cand
                break

        col_desc = None
        if fonte:
            col_desc = _detectar_coluna_descricao(df_src, fonte)
        else:
            for cand in ["prod_xprod", "descricao", "descr_item", "descricao_produto"]:
                if cand in df_src.columns:
                    col_desc = cand
                    break

        col_cnpj = (
            "cnpj"
            if "cnpj" in df_src.columns
            else "__cnpj_ref__"
            if "__cnpj_ref__" in df_src.columns
            else None
        )

        if col_codigo:
            # Se tivermos CNPJ e Descricao, geramos o codigo_fonte completo de 3 partes
            if col_cnpj and col_desc:
                exprs.append(
                    expr_gerar_codigo_fonte(col_cnpj, col_codigo, col_desc).alias("codigo_fonte")
                )
            elif col_cnpj:
                exprs.append(expr_gerar_codigo_fonte(col_cnpj, col_codigo).alias("codigo_fonte"))
            else:
                exprs.append(pl.col(col_codigo).cast(pl.Utf8, strict=False).alias("codigo_fonte"))

    if "id_linha_origem" in df_src.columns:
        exprs.append(pl.col("id_linha_origem").cast(pl.Utf8, strict=False))
    return exprs


def _construir_mapas_descricao(df_mapa: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    agrupado = (
        df_mapa.filter(
            pl.col("descricao_normalizada").is_not_null() & (pl.col("descricao_normalizada") != "")
        )
        .group_by("descricao_normalizada")
        .agg(pl.col("id_agrupado").drop_nulls().unique().sort().alias("ids_agrupados"))
    )

    df_univoco = (
        agrupado.filter(pl.col("ids_agrupados").list.len() == 1)
        .with_columns(pl.col("ids_agrupados").list.first().alias("id_agrupado_desc"))
        .select(["descricao_normalizada", "id_agrupado_desc"])
    )

    df_ambiguo = (
        agrupado.filter(pl.col("ids_agrupados").list.len() > 1)
        .with_columns(pl.lit(True).alias("descricao_ambigua"))
        .select(["descricao_normalizada", "descricao_ambigua", "ids_agrupados"])
    )
    return df_univoco, df_ambiguo


def _anexar_id_agrupado_por_codigo_ou_descricao(
    df_src: pl.DataFrame,
    df_mapa: pl.DataFrame,
    df_attrs: pl.DataFrame,
    col_desc: str,
    pasta_analises: "Path | None" = None,
    cnpj: str | None = None,
) -> pl.DataFrame:
    # 1. Gerar codigo_fonte de 3 partes para o dado de origem (se possivel/necessario)
    col_cod = (
        "cod_item"
        if "cod_item" in df_src.columns
        else ("prod_cprod" if "prod_cprod" in df_src.columns else None)
    )
    if col_cod and col_desc:
        df_src = df_src.with_columns(
            expr_gerar_codigo_fonte(pl.lit(cnpj), pl.col(col_cod), pl.col(col_desc)).alias(
                "codigo_fonte"
            )
        )

    df_base = df_src.with_columns(_normalizar_descricao_expr(col_desc))

    df_mapa_codigo_raw = df_mapa.filter(
        pl.col("codigo_fonte").is_not_null() & (pl.col("codigo_fonte") != "")
    ).select(["codigo_fonte", pl.col("id_agrupado").alias("id_agrupado_codigo")])
    colisoes = (
        df_mapa_codigo_raw.group_by("codigo_fonte")
        .agg(pl.col("id_agrupado_codigo").n_unique().alias("n_ids"))
        .filter(pl.col("n_ids") > 1)
    )
    if colisoes.height > 0:
        rprint(
            f"[yellow]Aviso: {colisoes.height} codigo_fonte(s) mapeiam para múltiplos "
            f"id_agrupado; mantendo apenas o primeiro match.[/yellow]"
        )
        if pasta_analises is not None and cnpj is not None:
            salvar_para_parquet(
                colisoes,
                pasta_analises,
                f"audit_codigo_fonte_colisao_{cnpj}.parquet",
            )
    df_mapa_codigo = df_mapa_codigo_raw.unique(subset=["codigo_fonte"])
    df_mapa_desc, df_mapa_desc_ambiguo = _construir_mapas_descricao(df_mapa)

    if "codigo_fonte" in df_base.columns:
        df_base = df_base.join(df_mapa_codigo, on="codigo_fonte", how="left")
    else:
        df_base = df_base.with_columns(pl.lit(None, dtype=pl.Utf8).alias("codigo_fonte"))
        df_base = df_base.with_columns(pl.lit(None, dtype=pl.Utf8).alias("id_agrupado_codigo"))

    df_base = (
        df_base.join(
            df_mapa_desc,
            left_on="__descricao_normalizada__",
            right_on="descricao_normalizada",
            how="left",
        )
        .join(
            df_mapa_desc_ambiguo.rename({"descricao_normalizada": "__descricao_normalizada__"}),
            on="__descricao_normalizada__",
            how="left",
        )
        .with_columns(
            [
                pl.coalesce(["id_agrupado_codigo", "id_agrupado_desc"]).alias("id_agrupado"),
                pl.when(pl.col("id_agrupado_codigo").is_not_null())
                .then(pl.lit("codigo_fonte"))
                .when(pl.col("id_agrupado_desc").is_not_null())
                .then(pl.lit("descricao_normalizada"))
                .otherwise(pl.lit(None, dtype=pl.Utf8))
                .alias("origem_vinculo_agrupamento"),
            ]
        )
        .with_columns(
            pl.when(pl.col("id_agrupado").is_not_null())
            .then(pl.lit(None, dtype=pl.Utf8))
            .when(
                pl.col("codigo_fonte").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
                != ""
            )
            .then(pl.lit("codigo_fonte_sem_mapeamento"))
            .when(pl.col("descricao_ambigua").fill_null(False))
            .then(pl.lit("descricao_normalizada_ambigua"))
            .when(
                pl.col("__descricao_normalizada__")
                .cast(pl.Utf8, strict=False)
                .fill_null("")
                .str.strip_chars()
                == ""
            )
            .then(pl.lit("sem_codigo_fonte_sem_descricao"))
            .otherwise(pl.lit("descricao_normalizada_sem_match"))
            .alias("motivo_sem_id_agrupado")
        )
        .drop(["id_agrupado_codigo", "id_agrupado_desc"], strict=False)
        .join(df_attrs, on="id_agrupado", how="left")
    )

    return df_base


def gerar_fontes_produtos(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    """Gera os arquivos de fontes agregadas com ``id_agrupado`` para um CNPJ.

    Processa as fontes brutas (C170, Bloco H, NF-e, NF-Ce) vinculando cada
    linha ao seu ``id_agrupado`` a partir de ``map_produto_agrupado`` e, como
    fallback controlado, pela ``descricao_normalizada``. Salva quatro arquivos
    em ``arquivos_parquet/``:

    - ``c170_agr_<cnpj>.parquet``
    - ``bloco_h_agr_<cnpj>.parquet``
    - ``nfe_agr_<cnpj>.parquet``
    - ``nfce_agr_<cnpj>.parquet``

    Linhas sem ``id_agrupado`` sao exportadas em arquivos de auditoria
    separados (``<fonte>_agr_sem_id_agrupado_<cnpj>.parquet``) e excluidas
    da saida principal sem interromper o pipeline.

    Args:
        cnpj: CPF ou CNPJ do contribuinte (somente digitos ou formatado).
        pasta_cnpj: Raiz do diretorio do CNPJ. Se ``None``, usa o padrao
            ``dados/CNPJ/<cnpj>``.

    Returns:
        ``True`` se todos os arquivos foram gerados com sucesso; ``False`` em
        caso de arquivos de agregacao ausentes ou falha ao salvar.

    Raises:
        ValueError: Se ``cnpj`` nao for um CPF (11 digitos) nem CNPJ (14 digitos).
    """
    cnpj = re.sub(r"\D", "", cnpj or "")
    if len(cnpj) not in {11, 14}:
        raise ValueError("CPF/CNPJ invalido.")

    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj

    pasta_analises = pasta_cnpj / "analises" / "produtos"
    pasta_brutos = pasta_cnpj / "arquivos_parquet"

    arq_prod_final = pasta_analises / f"produtos_final_{cnpj}.parquet"
    arq_mapa = pasta_analises / f"map_produto_agrupado_{cnpj}.parquet"

    if not arq_prod_final.exists() or not arq_mapa.exists():
        rprint(
            "[red]Arquivos de agregacao nao encontrados (produtos_final / map_produto_agrupado).[/red]"
        )
        return False
    if not pasta_brutos.exists():
        rprint("[red]Pasta de arquivos_parquet nao encontrada.[/red]")
        return False

    try:
        validar_parquet_essencial(
            arq_prod_final,
            [
                "id_agrupado",
                "descricao_normalizada",
                "descr_padrao",
                "ncm_padrao",
                "cest_padrao",
                "co_sefin_final",
                "unid_ref_sugerida",
            ],
            contexto="fontes_produtos/produtos_final",
        )
        validar_parquet_essencial(
            arq_mapa,
            ["id_agrupado", "descricao_normalizada", "codigo_fonte"],
            contexto="fontes_produtos/map_produto_agrupado",
        )
    except SchemaValidacaoError as exc:
        rprint(f"[red]{exc}[/red]")
        return False

    df_mapa = (
        pl.read_parquet(arq_mapa)
        .select(
            [
                pl.col("id_agrupado").cast(pl.Utf8, strict=False),
                expr_normalizar_codigo_fonte("codigo_fonte"),
                pl.col("descricao_normalizada").cast(pl.Utf8, strict=False),
            ]
        )
        .unique()
    )
    df_codigo_unico, df_desc_unico, df_desc_ambiguo = _construir_mapas(df_mapa)

    df_prod_final = pl.read_parquet(arq_prod_final)
    cols_attrs = [
        "id_agrupado",
        "descr_padrao",
        "ncm_padrao",
        "cest_padrao",
        "co_sefin_final",
        "unid_ref_sugerida",
    ]
    if "versao_agrupamento" in df_prod_final.columns:
        cols_attrs.append("versao_agrupamento")
    df_attrs = (
        df_prod_final.select(cols_attrs)
        .rename({"co_sefin_final": "co_sefin_agr"})
        .unique(subset=["id_agrupado"])
    )

    cfop_mercantil = _carregar_cfops_mercantis()
    cfops_mercantis = None
    if cfop_mercantil is not None and "co_cfop" in cfop_mercantil.columns:
        cfops_mercantis = set(
            cfop_mercantil.get_column("co_cfop").drop_nulls().cast(pl.String).to_list()
        )

    fontes = ["c170", "bloco_h", "nfe", "nfce"]
    gerou_algum = False

    for fonte in fontes:
        arq_fonte = _resolver_arquivo_fonte(pasta_brutos, fonte)
        if arq_fonte is None:
            continue

        n_linhas = pl.scan_parquet(arq_fonte).select(pl.len()).collect().item()
        rprint(f"[dim]{fonte}: {n_linhas:,} linhas[/dim]")

        if n_linhas > _LIMIAR_BATCH_LINHAS:
            ok = _processar_fonte_em_batches_anuais(
                arquivo=arq_fonte,
                colunas=_COLUNAS_FONTE.get(fonte),
                df_mapa=df_mapa,
                df_attrs=df_attrs,
                pasta_analises=pasta_analises,
                pasta_brutos=pasta_brutos,
                cnpj=cnpj,
                fonte=fonte,
                cfops_mercantis=cfops_mercantis,
            )
            if ok:
                gerou_algum = True
            continue

        df_src = _ler_primeiro(pasta_brutos, fonte, _COLUNAS_FONTE.get(fonte))
        if df_src is None or df_src.is_empty():
            continue

        col_desc = _detectar_coluna_descricao(df_src, fonte)
        if not col_desc:
            rprint(f"[yellow]Fonte {fonte} ignorada: sem coluna de descricao reconhecida.[/yellow]")
            continue

        exprs_rastreabilidade = _preservar_colunas_rastreabilidade(df_src, fonte=fonte)
        if exprs_rastreabilidade:
            df_src = df_src.with_columns(exprs_rastreabilidade)

        df_src, df_fora_escopo = _separar_fora_escopo_canonico(
            df_src,
            fonte=fonte,
            cnpj=cnpj,
            cfops_mercantis=cfops_mercantis,
        )
        if df_fora_escopo is not None and not _salvar_auditoria_fora_escopo(
            df_fora_escopo, pasta_analises, fonte, cnpj
        ):
            return False

        if df_src.is_empty():
            if df_fora_escopo is not None:
                schema_canonico = df_fora_escopo.drop("motivo_fora_escopo_canonico").schema
                ok = _salvar_agr_vazio(pasta_brutos, fonte, cnpj, schema=schema_canonico)
                rprint(
                    f"[yellow]Aviso: todas as linhas de {fonte} foram para "
                    f"fora_escopo_canonico. {fonte}_agr_{cnpj}.parquet gravado vazio.[/yellow]"
                )
                if ok:
                    gerou_algum = True
            continue

        df_out = _anexar_id_agrupado_por_codigo_ou_descricao(
            df_src=df_src,
            df_mapa=df_mapa,
            df_attrs=df_attrs,
            col_desc=col_desc,
            pasta_analises=pasta_analises,
            cnpj=cnpj,
        )

        faltantes = df_out.filter(pl.col("id_agrupado").is_null())
        if faltantes.height > 0:
            nome_log = f"{fonte}_agr_sem_id_agrupado_{cnpj}.parquet"
            salvar_para_parquet(faltantes, pasta_analises, nome_log)
            rprint(
                f"[yellow]Aviso: {fonte} possui {faltantes.height} linhas sem id_agrupado. "
                f"Detalhes em {nome_log}. Essas linhas serao excluidas da saida {fonte}_agr.[/yellow]"
            )
            df_out = df_out.filter(pl.col("id_agrupado").is_not_null())
            if df_out.is_empty():
                continue

        if (
            "descricao_normalizada" not in df_out.columns
            and "__descricao_normalizada__" in df_out.columns
        ):
            df_out = df_out.rename({"__descricao_normalizada__": "descricao_normalizada"})
        else:
            df_out = df_out.drop("__descricao_normalizada__", strict=False)

        colunas_presentes = set(df_out.columns)
        colunas_faltando = [
            c for c in COLUNAS_OBRIGATORIAS_FONTES_AGR if c not in colunas_presentes
        ]
        if colunas_faltando:
            rprint(
                f"[yellow]Fonte {fonte}: colunas obrigatorias faltando na saida: {colunas_faltando}[/yellow]"
            )

        for col in COLUNAS_RASTREABILIDADE_FONTES:
            if col not in df_out.columns:
                df_out = df_out.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))

        df_out = df_out.drop("__descricao_normalizada__", strict=False)
        nome_saida = f"{fonte}_agr_{cnpj}.parquet"
        ok = salvar_para_parquet(df_out, pasta_brutos, nome_saida)
        if not ok:
            return False
        gerou_algum = True

    return gerou_algum


if __name__ == "__main__":
    if len(sys.argv) > 1:
        gerar_fontes_produtos(sys.argv[1])
    else:
        gerar_fontes_produtos(input("CNPJ: "))
