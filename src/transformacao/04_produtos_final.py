"""
04_produtos_final.py

Objetivo: inicializar a camada de agrupamento manual e gerar a tabela final
de produtos recalculavel a partir de descricao_produtos.

Saidas:
- produtos_agrupados_<cnpj>.parquet
- map_produto_agrupado_<cnpj>.parquet
- produtos_final_<cnpj>.parquet
"""

from __future__ import annotations

import re
import sys
from collections import Counter
from pathlib import Path

import polars as pl
from rich import print as rprint

ROOT_DIR = Path(r"c:\funcoes - Copia")
SRC_DIR = ROOT_DIR / "src"
UTILITARIOS_DIR = SRC_DIR / "utilitarios"
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"

for _dir in (SRC_DIR, UTILITARIOS_DIR):
    dir_str = str(_dir)
    if dir_str not in sys.path:
        sys.path.insert(0, dir_str)

try:
    from salvar_para_parquet import salvar_para_parquet
    from descricao_produtos import descricao_produtos
    from id_agrupados import gerar_id_agrupados
except ImportError as e:
    rprint(f"[red]Erro ao importar modulos:[/red] {e}")
    sys.exit(1)


def _gerar_id_agrupado(seq: int) -> str:
    return f"id_agrupado_{seq}"


def _serie_limpa_lista(values: list | None) -> list[str]:
    if not values:
        return []
    out: list[str] = []
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            out.append(text)
    return sorted(set(out))


def _calcular_atributos_padrao(df_base: pl.DataFrame) -> dict[str, str | None]:
    if df_base.is_empty():
        return {
            "descr_padrao": None,
            "ncm_padrao": None,
            "cest_padrao": None,
            "gtin_padrao": None,
            "co_sefin_padrao": None,
        }

    resultado: dict[str, str | None] = {}
    for origem, destino in [
        ("ncm", "ncm_padrao"),
        ("cest", "cest_padrao"),
        ("gtin", "gtin_padrao"),
        ("co_sefin_item", "co_sefin_padrao"),
    ]:
        valores = [str(v).strip() for v in df_base.get_column(origem).drop_nulls().to_list() if str(v).strip()]
        resultado[destino] = Counter(valores).most_common(1)[0][0] if valores else None

    descs = (
        df_base
        .with_columns(
            [
                pl.col("descricao").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars().alias("descricao"),
                pl.when(pl.col("ncm").cast(pl.String, strict=False).str.strip_chars() != "").then(1).otherwise(0).alias("__has_ncm"),
                pl.when(pl.col("cest").cast(pl.String, strict=False).str.strip_chars() != "").then(1).otherwise(0).alias("__has_cest"),
                pl.when(pl.col("gtin").cast(pl.String, strict=False).str.strip_chars() != "").then(1).otherwise(0).alias("__has_gtin"),
            ]
        )
        .filter(pl.col("descricao") != "")
        .group_by(["descricao"])
        .agg(
            [
                pl.len().alias("count"),
                pl.col("__has_ncm").max().alias("has_ncm"),
                pl.col("__has_cest").max().alias("has_cest"),
                pl.col("__has_gtin").max().alias("has_gtin"),
            ]
        )
    )

    candidatos = descs.to_dicts()

    def _score(row: dict) -> tuple[int, int, int]:
        preenchidos = int(row.get("has_ncm", 0)) + int(row.get("has_cest", 0)) + int(row.get("has_gtin", 0))
        return (int(row.get("count", 0)), preenchidos, len(str(row.get("descricao", ""))))

    candidatos.sort(key=_score, reverse=True)
    resultado["descr_padrao"] = candidatos[0]["descricao"] if candidatos else None
    return resultado


def produtos_agrupados(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
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

    rprint(f"[bold cyan]Gerando produtos_agrupados/final para CNPJ: {cnpj}[/bold cyan]")

    df_descricoes = pl.read_parquet(arq_descricoes)
    df_item_unid = pl.read_parquet(arq_item_unid)

    if df_descricoes.is_empty():
        rprint("[yellow]descricao_produtos esta vazio.[/yellow]")
        return False

    for col in ["lista_unid", "fontes", "lista_co_sefin", "lista_id_item_unid", "lista_id_item"]:
        if col not in df_descricoes.columns:
            df_descricoes = df_descricoes.with_columns(pl.lit([]).cast(pl.List(pl.String)).alias(col))

    registros_mestra: list[dict] = []
    registros_ponte: list[dict] = []

    for seq, row in enumerate(df_descricoes.to_dicts(), start=1):
        id_agrupado = _gerar_id_agrupado(seq)
        desc_norm = row.get("descricao_normalizada")

        if desc_norm:
            df_base = df_item_unid.filter(pl.col("descricao").is_not_null()).with_columns(
                pl.col("descricao")
                .cast(pl.Utf8, strict=False)
                .str.to_uppercase()
                .str.replace_all(r"\s+", " ")
                .alias("__descricao_upper")
            )
            df_base = df_base.filter(pl.col("__descricao_upper") == desc_norm).drop("__descricao_upper")
        else:
            df_base = df_item_unid.filter(pl.lit(False))

        padrao = _calcular_atributos_padrao(df_base)
        lista_co_sefin = _serie_limpa_lista(row.get("lista_co_sefin"))
        lista_unidades = _serie_limpa_lista(row.get("lista_unid"))
        fontes = _serie_limpa_lista(row.get("fontes"))

        registros_mestra.append(
            {
                "id_agrupado": id_agrupado,
                "lista_chave_produto": [row.get("id_descricao")] if row.get("id_descricao") else [],
                "descr_padrao": padrao.get("descr_padrao") or row.get("descricao"),
                "ncm_padrao": padrao.get("ncm_padrao"),
                "cest_padrao": padrao.get("cest_padrao"),
                "gtin_padrao": padrao.get("gtin_padrao"),
                "lista_co_sefin": lista_co_sefin,
                "co_sefin_padrao": padrao.get("co_sefin_padrao"),
                "lista_unidades": lista_unidades,
                "co_sefin_divergentes": len(lista_co_sefin) > 1,
                "fontes": fontes,
            }
        )

        if row.get("id_descricao"):
            registros_ponte.append(
                {
                    "chave_produto": row["id_descricao"],
                    "id_agrupado": id_agrupado,
                }
            )

    df_mestra = pl.DataFrame(registros_mestra)
    df_ponte = pl.DataFrame(registros_ponte)

    ok_mestra = salvar_para_parquet(df_mestra, pasta_analises, f"produtos_agrupados_{cnpj}.parquet")
    ok_ponte = salvar_para_parquet(df_ponte, pasta_analises, f"map_produto_agrupado_{cnpj}.parquet")
    if not (ok_mestra and ok_ponte):
        return False

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


def gerar_produtos_final(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    return produtos_agrupados(cnpj, pasta_cnpj)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        produtos_agrupados(sys.argv[1])
    else:
        produtos_agrupados(input("CNPJ: "))
