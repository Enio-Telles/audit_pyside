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
from utilitarios.project_paths import PROJECT_ROOT

import polars as pl
from rich import print as rprint

ROOT_DIR = PROJECT_ROOT
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
    from text import expr_normalizar_descricao
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
        valores = [
            str(v).strip()
            for v in df_base.get_column(origem).drop_nulls().to_list()
            if str(v).strip()
        ]
        resultado[destino] = Counter(valores).most_common(1)[0][0] if valores else None

    descs = (
        df_base.with_columns(
            [
                pl.col("descricao")
                .cast(pl.Utf8, strict=False)
                .fill_null("")
                .str.strip_chars()
                .alias("descricao"),
                pl.when(
                    pl.col("ncm").cast(pl.String, strict=False).str.strip_chars() != ""
                )
                .then(1)
                .otherwise(0)
                .alias("__has_ncm"),
                pl.when(
                    pl.col("cest").cast(pl.String, strict=False).str.strip_chars() != ""
                )
                .then(1)
                .otherwise(0)
                .alias("__has_cest"),
                pl.when(
                    pl.col("gtin").cast(pl.String, strict=False).str.strip_chars() != ""
                )
                .then(1)
                .otherwise(0)
                .alias("__has_gtin"),
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
        preenchidos = (
            int(row.get("has_ncm") or 0)
            + int(row.get("has_cest") or 0)
            + int(row.get("has_gtin") or 0)
        )
        return (
            int(row.get("count") or 0),
            preenchidos,
            len(str(row.get("descricao") or "")),
        )

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

    # LazyFrame para leitura
    lf_descricoes = pl.scan_parquet(arq_descricoes)
    lf_item_unid = pl.scan_parquet(arq_item_unid)

    # Garante colunas obrigatórias
    for col in [
        "lista_unid",
        "fontes",
        "lista_co_sefin",
        "lista_id_item_unid",
        "lista_id_item",
    ]:
        if col not in lf_descricoes.schema:
            lf_descricoes = lf_descricoes.with_columns(
                pl.lit([]).cast(pl.List(pl.String)).alias(col)
            )

    # Materializa para processamento manual (pois há lógica Python pura)
    df_descricoes = lf_descricoes.collect()
    df_item_unid = lf_item_unid.collect()

    if df_descricoes.is_empty():
        rprint("[yellow]descricao_produtos esta vazio.[/yellow]")
        return False

    # Vetorizado: agregue informações de `df_item_unid` por `descricao_normalizada`
    # para evitar filtrar `df_item_unid` repetidamente dentro de um loop.
    df_item_unid_norm = df_item_unid.with_columns(
        expr_normalizar_descricao("descricao").alias("__descricao_norm")
    )

    # Agrega estatísticas por (__descricao_norm, descricao) para eleger descr_padrao
    grouped_descrs = (
        df_item_unid_norm.with_columns(
            [
                pl.when(
                    pl.col("ncm").cast(pl.Utf8, strict=False).str.strip_chars() != ""
                )
                .then(1)
                .otherwise(0)
                .alias("__has_ncm"),
                pl.when(
                    pl.col("cest").cast(pl.Utf8, strict=False).str.strip_chars() != ""
                )
                .then(1)
                .otherwise(0)
                .alias("__has_cest"),
                pl.when(
                    pl.col("gtin").cast(pl.Utf8, strict=False).str.strip_chars() != ""
                )
                .then(1)
                .otherwise(0)
                .alias("__has_gtin"),
            ]
        )
        .group_by(
            ["__descricao_norm", "descricao"]
        )  # por descricao original dentro do grupo normalizado
        .agg(
            [
                pl.count().alias("count"),
                pl.col("__has_ncm").max().alias("has_ncm"),
                pl.col("__has_cest").max().alias("has_cest"),
                pl.col("__has_gtin").max().alias("has_gtin"),
            ]
        )
    )

    # Compacta candidatos em uma lista por descricao_normalizada
    candidatos_por_norm = grouped_descrs.group_by("__descricao_norm").agg(
        [
            pl.struct(["descricao", "count", "has_ncm", "has_cest", "has_gtin"])
            .list()
            .alias("candidatos")
        ]
    )

    # Agrega listas auxiliares (ncm, cest, gtin, co_sefin, unidades, fontes)
    listas_por_norm = df_item_unid_norm.group_by("__descricao_norm").agg(
        [
            pl.col("ncm").drop_nulls().list().alias("lista_ncm_raw"),
            pl.col("cest").drop_nulls().list().alias("lista_cest_raw"),
            pl.col("gtin").drop_nulls().list().alias("lista_gtin_raw"),
            pl.col("co_sefin_item").drop_nulls().list().alias("lista_co_sefin_raw"),
            pl.col("lista_unid").list().alias("lista_unid_raw"),
            pl.col("fontes").list().alias("fontes_raw"),
        ]
    )

    # Junta as agregações em um único frame por norma
    agregados = candidatos_por_norm.join(
        listas_por_norm, on="__descricao_norm", how="outer"
    )

    # Junta com as descricoes originais para produzir uma linha por descricao (sem re-filtrar df_item_unid)
    df_join = df_descricoes.join(
        agregados,
        left_on="descricao_normalizada",
        right_on="__descricao_norm",
        how="left",
    )

    registros_mestra: list[dict] = []
    registros_ponte: list[dict] = []

    def _score_cand(cand: dict) -> tuple[int, int, int]:
        preenchidos = (
            int(cand.get("has_ncm") or 0)
            + int(cand.get("has_cest") or 0)
            + int(cand.get("has_gtin") or 0)
        )
        return (
            int(cand.get("count") or 0),
            preenchidos,
            len(str(cand.get("descricao") or "")),
        )

    for seq, row in enumerate(df_join.to_dicts(), start=1):
        id_agrupado = _gerar_id_agrupado(seq)

        # candidatos (pode ser None)
        candidatos = row.get("candidatos") or []
        descr_padrao = None
        if candidatos:
            try:
                candidatos.sort(key=_score_cand, reverse=True)
                descr_padrao = candidatos[0].get("descricao")
            except Exception:
                descr_padrao = None

        # padroes por campo a partir das listas agregadas
        def _most_common_from_list(values):
            if not values:
                return None
            flat = [str(v).strip() for v in values if v is not None and str(v).strip()]
            if not flat:
                return None
            return Counter(flat).most_common(1)[0][0]

        ncm_padrao = _most_common_from_list(row.get("lista_ncm_raw") or [])
        cest_padrao = _most_common_from_list(row.get("lista_cest_raw") or [])
        gtin_padrao = _most_common_from_list(row.get("lista_gtin_raw") or [])
        co_sefin_padrao = _most_common_from_list(row.get("lista_co_sefin_raw") or [])

        lista_co_sefin = _serie_limpa_lista(row.get("lista_co_sefin"))
        lista_unidades = _serie_limpa_lista(row.get("lista_unid"))
        fontes = _serie_limpa_lista(row.get("fontes"))

        registros_mestra.append(
            {
                "id_agrupado": id_agrupado,
                "lista_chave_produto": (
                    [row.get("id_descricao")] if row.get("id_descricao") else []
                ),
                "descr_padrao": descr_padrao or row.get("descricao"),
                "ncm_padrao": ncm_padrao,
                "cest_padrao": cest_padrao,
                "gtin_padrao": gtin_padrao,
                "lista_co_sefin": lista_co_sefin,
                "co_sefin_padrao": co_sefin_padrao,
                "lista_unidades": lista_unidades,
                "co_sefin_divergentes": len(lista_co_sefin) > 1,
                "fontes": fontes,
            }
        )

        if row.get("id_descricao"):
            registros_ponte.append(
                {"chave_produto": row["id_descricao"], "id_agrupado": id_agrupado}
            )

    df_mestra = pl.DataFrame(registros_mestra)
    df_ponte = pl.DataFrame(registros_ponte)

    ok_mestra = salvar_para_parquet(
        df_mestra, pasta_analises, f"produtos_agrupados_{cnpj}.parquet"
    )
    ok_ponte = salvar_para_parquet(
        df_ponte, pasta_analises, f"map_produto_agrupado_{cnpj}.parquet"
    )
    if not (ok_mestra and ok_ponte):
        return False

    # Para o join final, volta para LazyFrame para eficiência
    lf_descricoes_final = pl.scan_parquet(arq_descricoes)
    lf_map = (
        pl.from_pandas(df_mestra)
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
    # Convert to LazyFrame for join
    lf_map = lf_map.lazy()
    lf_final = (
        lf_descricoes_final.lazy()
        .join(lf_map, on="id_descricao", how="left")
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
    df_final = lf_final.collect()
    ok_final = salvar_para_parquet(
        df_final, pasta_analises, f"produtos_final_{cnpj}.parquet"
    )
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
