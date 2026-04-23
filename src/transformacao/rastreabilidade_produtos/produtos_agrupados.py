from __future__ import annotations

"""
produtos_agrupados.py

Objetivo: gerar a tabela produtos_agrupados (mestra) e map_produto_agrupado (ponte).
Campos Mestra: id_agrupado, descr_padrao, ncm_padrao, cest_padrao, gtin_padrao, lista_co_sefin, co_sefin_padrao, lista_unidades, co_sefin_divergentes.
Campos Ponte: chave_produto, id_agrupado.
"""

import re
import sys
from collections import Counter
from pathlib import Path
from utilitarios.project_paths import PROJECT_ROOT

import polars as pl
from rich import print as rprint

ROOT_DIR = PROJECT_ROOT
SRC_DIR = ROOT_DIR / "src"
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"


try:
    from utilitarios.salvar_para_parquet import salvar_para_parquet
    from utilitarios.text import expr_normalizar_descricao
except ImportError as e:
    rprint(f"[red]Erro ao importar modulos utilitarios:[/red] {e}")
    sys.exit(1)


def _expr_normalizar_descricao(coluna: str) -> pl.Expr:
    return expr_normalizar_descricao(coluna)


def _primeira_descricao_valida(df: pl.DataFrame) -> str | None:
    if "descricao" not in df.columns:
        return None

    first_val = (
        df.select(pl.col("descricao").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars())
        .filter(pl.col("descricao") != "")
        .limit(1)
    )

    if first_val.is_empty():
        return None

    return first_val.item()


def _gerar_id_agrupado(seq: int) -> str:
    """Gera um ID de agrupamento sequencial."""
    return f"PROD_MSTR_{seq:05d}"


def calcular_atributos_padrao(df_itens_base: pl.DataFrame) -> dict:
    """
    Calcula atributos padrao pela maior ocorrencia.

    Tie-breaker:
    1) Maior quantidade de campos preenchidos (NCM, CEST, GTIN)
    2) Maior tamanho da descricao
    """
    if df_itens_base.is_empty():
        return {}

    res: dict[str, str | None] = {}
    for col in ["ncm", "cest", "gtin", "co_sefin_item"]:
        if col not in df_itens_base.columns:
            continue

        counts = (
            df_itens_base.select(
                pl.col(col).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
            )
            .filter(pl.col(col) != "")
            .get_column(col)
            .value_counts()
            .sort("count", descending=True)
        )

        if not counts.is_empty():
            res[f"{col}_padrao"] = counts[col][0]
        else:
            res[f"{col}_padrao"] = None

    if "co_sefin_item_padrao" in res:
        res["co_sefin_padrao"] = res.pop("co_sefin_item_padrao")

    if "descricao" not in df_itens_base.columns:
        return res

    descs = (
        df_itens_base.with_columns(
            pl.col("descricao")
            .cast(pl.Utf8, strict=False)
            .fill_null("")
            .str.strip_chars()
            .alias("descricao")
        )
        .filter(pl.col("descricao") != "")
        .group_by(["descricao", "ncm", "cest", "gtin"])
        .agg(pl.len().alias("count"))
    )

    if descs.is_empty():
        res["descr_padrao"] = _primeira_descricao_valida(df_itens_base)
        return res

    descs = descs.with_columns(
        filled=(
            pl.when(pl.col("ncm").is_not_null() & (pl.col("ncm") != "")).then(1).otherwise(0)
            + pl.when(pl.col("cest").is_not_null() & (pl.col("cest") != "")).then(1).otherwise(0)
            + pl.when(pl.col("gtin").is_not_null() & (pl.col("gtin") != "")).then(1).otherwise(0)
        ),
        len_desc=pl.col("descricao").str.len_chars(),
    ).sort(["count", "filled", "len_desc"], descending=True)

    if not descs.is_empty():
        res["descr_padrao"] = descs["descricao"][0]
    else:
        res["descr_padrao"] = _primeira_descricao_valida(df_itens_base)

    return res


def inicializar_produtos_agrupados(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    """Cria a tabela inicial mestre e ponte de agrupamentos."""
    cnpj = re.sub(r"\D", "", cnpj)
    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj

    pasta_analises = pasta_cnpj / "analises" / "produtos"
    arq_produtos = pasta_analises / f"produtos_{cnpj}.parquet"
    arq_base = pasta_analises / f"produtos_unidades_{cnpj}.parquet"

    if not arq_produtos.exists():
        rprint("[red]produtos.parquet nao encontrado.[/red]")
        return False

    rprint(f"[bold cyan]Inicializando produtos_agrupados para CNPJ: {cnpj}[/bold cyan]")

    df_prod = pl.read_parquet(arq_produtos)
    df_base = pl.read_parquet(arq_base)

    # Otimizacao: Agrupamento em tempo linear usando Union-Find/Mapeamento
    # Em vez de O(N^2) no fits(), usamos tabelas de hash para GTIN e Descricao+NCM

    rows = df_prod.to_dicts()
    parent = list(range(len(rows)))

    def find(i):
        if parent[i] == i:
            return i
        parent[i] = find(parent[i])
        return parent[i]

    def union(i, j):
        root_i = find(i)
        root_j = find(j)
        if root_i != root_j:
            parent[root_i] = root_j

    gtin_to_idx = {}
    desc_ncm_to_idx = {}

    for idx, row in enumerate(rows):
        # 1. Agrupar por GTIN
        list_gtin = row.get("lista_gtin") or []
        for gtin in list_gtin:
            if gtin in gtin_to_idx:
                union(idx, gtin_to_idx[gtin])
            else:
                gtin_to_idx[gtin] = idx

        # 2. Agrupar por Descricao + NCM
        desc_norm = row.get("descricao_normalizada")
        if desc_norm:
            list_ncm = row.get("lista_ncm") or [None]
            for ncm in list_ncm:
                key = (desc_norm, ncm)
                if key in desc_ncm_to_idx:
                    union(idx, desc_ncm_to_idx[key])
                else:
                    desc_ncm_to_idx[key] = idx

    # Coletar grupos
    grupos_indices = {}
    for i in range(len(rows)):
        root = find(i)
        if root not in grupos_indices:
            grupos_indices[root] = []
        grupos_indices[root].append(rows[i])

    grupos = list(grupos_indices.values())

    # Bolt: Pre-calculate normalization outside the loop to avoid redundant operations for each group
    # Optimized: Using native implementation of _expr_normalizar_descricao to ensure vectorization
    df_base_opt = df_base.with_columns(
        _expr_normalizar_descricao("descricao").alias("__descricao_norm")
    )
    part_dict = df_base_opt.partition_by("__descricao_norm", as_dict=True)

    registros_mestra = []
    registros_ponte = []

    seq = 1
    for g in grupos:
        id_grp = _gerar_id_agrupado(seq)
        seq += 1

        desc_norms = [r.get("descricao_normalizada") for r in g if r.get("descricao_normalizada")]

        if desc_norms:
            group_dfs = []
            for d in set(desc_norms):
                if (d,) in part_dict:
                    group_dfs.append(part_dict[(d,)])

            if group_dfs:
                if len(group_dfs) > 1:
                    df_base_filtered = pl.concat(group_dfs, how="vertical_relaxed")
                else:
                    df_base_filtered = group_dfs[0]
                df_base_filtered = df_base_filtered.drop("__descricao_norm")
            else:
                df_base_filtered = df_base.filter(pl.lit(False))
        else:
            df_base_filtered = df_base.filter(pl.lit(False))

        padrao = calcular_atributos_padrao(df_base_filtered)

        lista_sefin = list(set([item for r in g for item in (r.get("lista_co_sefin") or [])]))
        lista_unidades = list(set([item for r in g for item in (r.get("lista_unid") or [])]))
        lista_ncm = sorted(set([item for r in g for item in (r.get("lista_ncm") or []) if item]))
        lista_cest = sorted(set([item for r in g for item in (r.get("lista_cest") or []) if item]))
        lista_gtin = sorted(set([item for r in g for item in (r.get("lista_gtin") or []) if item]))
        # A lista principal deve conter apenas descricoes-base do grupo.
        lista_descricoes = sorted(set([r.get("descricao") for r in g if r.get("descricao")]))
        # Complementos permanecem auditaveis em coluna propria.
        lista_desc_compl = sorted(
            set([item for r in g for item in (r.get("lista_desc_compl") or []) if item])
        )
        divergentes = len(lista_sefin) > 1

        registros_mestra.append(
            {
                "id_agrupado": id_grp,
                "descr_padrao": padrao.get("descr_padrao") or g[0].get("descricao"),
                "ncm_padrao": padrao.get("ncm_padrao"),
                "cest_padrao": padrao.get("cest_padrao"),
                "gtin_padrao": padrao.get("gtin_padrao"),
                "lista_ncm": lista_ncm,
                "lista_cest": lista_cest,
                "lista_gtin": lista_gtin,
                "lista_descricoes": lista_descricoes,
                "lista_desc_compl": lista_desc_compl,
                "lista_co_sefin": lista_sefin,
                "co_sefin_padrao": padrao.get("co_sefin_padrao"),
                "co_sefin_agr": ", ".join(sorted([str(s) for s in lista_sefin])),
                "lista_unidades": lista_unidades,
                "co_sefin_divergentes": divergentes,
            }
        )

        for r in g:
            chave = r.get("chave_produto") or r.get("chave_item")
            if chave:
                registros_ponte.append({"chave_produto": chave, "id_agrupado": id_grp})

    if not registros_mestra:
        return False

    df_mestra = pl.DataFrame(registros_mestra)
    df_ponte = pl.DataFrame(registros_ponte)

    ok1 = salvar_para_parquet(df_mestra, pasta_analises, f"produtos_agrupados_{cnpj}.parquet")
    ok2 = salvar_para_parquet(df_ponte, pasta_analises, f"map_produto_agrupado_{cnpj}.parquet")
    return ok1 and ok2


if __name__ == "__main__":
    if len(sys.argv) > 1:
        inicializar_produtos_agrupados(sys.argv[1])
    else:
        c = input("CNPJ: ")
        inicializar_produtos_agrupados(c)
