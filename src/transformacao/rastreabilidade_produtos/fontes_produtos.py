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

import polars as pl
from rich import print as rprint

from utilitarios.project_paths import PROJECT_ROOT

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
    return (
        pl.col(col)
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.to_uppercase()
        .str.replace_all(r"[ÁÀÂÃÄ]", "A")
        .str.replace_all(r"[ÉÈÊË]", "E")
        .str.replace_all(r"[ÍÌÎÏ]", "I")
        .str.replace_all(r"[ÓÒÔÕÖ]", "O")
        .str.replace_all(r"[ÚÙÛÜ]", "U")
        .str.replace_all(r"Ç", "C")
        .str.replace_all(r"Ñ", "N")
        .str.strip_chars()
        .str.replace_all(r"\s+", " ")
        .alias("__descricao_normalizada__")
    )
    return expr_normalizar_descricao(col).alias("__descricao_normalizada__")


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


def _ler_primeiro(arq_dir: Path, prefix: str) -> pl.DataFrame | None:
    arquivos = sorted(arq_dir.glob(f"{prefix}_*.parquet"))
    if not arquivos:
        arquivos = sorted(arq_dir.glob(f"{prefix}*.parquet"))
    if not arquivos:
        return None
    return pl.read_parquet(arquivos[0])


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


def _preservar_colunas_rastreabilidade(df_src: pl.DataFrame) -> list[pl.Expr]:
    exprs: list[pl.Expr] = []
    if "codigo_fonte" not in df_src.columns:
        col_codigo = None
        for cand in ["codigo_produto", "codigo_produto_original", "cod_item"]:
            if cand in df_src.columns:
                col_codigo = cand
                break

        if "cnpj" in df_src.columns and col_codigo:
            exprs.append(
                pl.concat_str(
                    [
                        pl.col("cnpj").cast(pl.Utf8, strict=False),
                        pl.lit("|"),
                        pl.col(col_codigo).cast(pl.Utf8, strict=False),
                    ]
                ).alias("codigo_fonte")
            )
        elif col_codigo:
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

    fontes = ["c170", "bloco_h", "nfe", "nfce"]
    gerou_algum = False

    for fonte in fontes:
        df_src = _ler_primeiro(pasta_brutos, fonte)
        if df_src is None or df_src.is_empty():
            continue

        col_desc = _detectar_coluna_descricao(df_src, fonte)
        if not col_desc:
            rprint(f"[yellow]Fonte {fonte} ignorada: sem coluna de descricao reconhecida.[/yellow]")
            continue

        exprs_rastreabilidade = _preservar_colunas_rastreabilidade(df_src)
        if exprs_rastreabilidade:
            df_src = df_src.with_columns(exprs_rastreabilidade)

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
                rprint(
                    f"[yellow]Fonte {fonte}: todas as linhas foram excluidas (sem correspondencia). Pulando.[/yellow]"
                )
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
