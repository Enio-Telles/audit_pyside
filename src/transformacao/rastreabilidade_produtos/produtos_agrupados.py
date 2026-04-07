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

import polars as pl
from rich import print as rprint

ROOT_DIR = Path(r"c:\funcoes - Copia")
SRC_DIR = ROOT_DIR / "src"
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"


try:
    from utilitarios.salvar_para_parquet import salvar_para_parquet
    from utilitarios.text import remove_accents
except ImportError as e:
    rprint(f"[red]Erro ao importar modulos utilitarios:[/red] {e}")
    sys.exit(1)


def _primeira_descricao_valida(df: pl.DataFrame) -> str | None:
    if "descricao" not in df.columns:
        return None
    vals = (
        df.select(
            pl.col("descricao")
            .cast(pl.Utf8, strict=False)
            .fill_null("")
            .str.strip_chars()
            .alias("descricao")
        )
        .filter(pl.col("descricao") != "")
        .get_column("descricao")
        .to_list()
    )
    return vals[0] if vals else None


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
        vals = [str(v) for v in df_itens_base[col].drop_nulls().to_list() if str(v).strip()]
        res[f"{col}_padrao"] = Counter(vals).most_common(1)[0][0] if vals else None

    if "co_sefin_item_padrao" in res:
        res["co_sefin_padrao"] = res.pop("co_sefin_item_padrao")

    descs = (
        df_itens_base
        .with_columns(
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

    def _calc_score(row: dict) -> tuple[int, int, int]:
        filled = sum(1 for c in ["ncm", "cest", "gtin"] if row.get(c) and str(row.get(c)).strip())
        len_desc = len(str(row.get("descricao", "")))
        return (int(row.get("count", 0)), filled, len_desc)

    sorted_descs = sorted(descs.to_dicts(), key=_calc_score, reverse=True)
    res["descr_padrao"] = sorted_descs[0]["descricao"] if sorted_descs else _primeira_descricao_valida(df_itens_base)

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
    df_base = pl.read_parquet(arq_base).with_columns(
        pl.col("descricao")
        .map_elements(remove_accents, return_dtype=pl.String)
        .str.to_uppercase()
        .str.strip_chars()
        .str.replace_all(r"\s+", " ")
        .fill_null("")
        .alias("descricao_normalizada_temp")
    )

    grupos = []
    
    def fits(row, g_rows):
        for r in g_rows:
            if row.get("lista_gtin") and r.get("lista_gtin"):
                if set(row["lista_gtin"]) & set(r["lista_gtin"]):
                    return True
            if row.get("descricao_normalizada") == r.get("descricao_normalizada"):
                if row.get("lista_ncm") and r.get("lista_ncm"):
                    if set(row["lista_ncm"]) & set(r["lista_ncm"]):
                        return True
                elif not row.get("lista_ncm") and not r.get("lista_ncm"):
                    return True
        return False

    for row in df_prod.to_dicts():
        found = False
        for g in grupos:
            if fits(row, g):
                g.append(row)
                found = True
                break
        if not found:
            grupos.append([row])

    registros_mestra = []
    registros_ponte = []
    
    seq = 1
    for g in grupos:
        id_grp = _gerar_id_agrupado(seq)
        seq += 1
        
        desc_norms = [r.get("descricao_normalizada") for r in g if r.get("descricao_normalizada")]
        
        if desc_norms:
            df_base_filtered = df_base.filter(
                pl.col("descricao_normalizada_temp").is_in(desc_norms)
            )
        else:
            df_base_filtered = df_base.filter(pl.lit(False))

        df_base_filtered = df_base_filtered.drop("descricao_normalizada_temp", strict=False)

        padrao = calcular_atributos_padrao(df_base_filtered)

        lista_sefin = list(set([item for r in g for item in (r.get("lista_co_sefin") or [])]))
        lista_unidades = list(set([item for r in g for item in (r.get("lista_unid") or [])]))
        lista_ncm = sorted(set([item for r in g for item in (r.get("lista_ncm") or []) if item]))
        lista_cest = sorted(set([item for r in g for item in (r.get("lista_cest") or []) if item]))
        lista_gtin = sorted(set([item for r in g for item in (r.get("lista_gtin") or []) if item]))
        lista_descricoes = sorted(
            set(
                [r.get("descricao") for r in g if r.get("descricao")]
                + [item for r in g for item in (r.get("lista_desc_compl") or []) if item]
            )
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
                registros_ponte.append({
                    "chave_produto": chave,
                    "id_agrupado": id_grp
                })

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
