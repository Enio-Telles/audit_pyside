"""
fontes_produtos.py

Gera arquivos derivados das fontes brutas com a coluna `id_agrupado`
vinculada pela descricao_normalizada da cadeia nova.

Saidas (em arquivos_parquet):
- c170_agr_<cnpj>.parquet
- bloco_h_agr_<cnpj>.parquet
- nfe_agr_<cnpj>.parquet
- nfce_agr_<cnpj>.parquet

Regra de consistencia:
- toda linha precisa sair com `id_agrupado`
- se houver qualquer linha sem `id_agrupado`, a rotina falha
"""

from __future__ import annotations

import re
import sys
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
    from utilitarios.validacao_schema import (
        SchemaValidacaoError,
        validar_parquet_essencial,
    )
except ImportError as e:
    rprint(f"[red]Erro ao importar modulos utilitarios:[/red] {e}")
    sys.exit(1)


def _norm(text: str | None) -> str:
    if text is None:
        return ""
    return re.sub(r"\s+", " ", (remove_accents(text) or "").upper().strip())


def _normalizar_descricao_expr(col: str) -> pl.Expr:
    # Optimization: Replace .map_elements with native Polars string operations to preserve vectorization
    # and improve performance for large datasets.
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


def gerar_fontes_produtos(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj = re.sub(r"\D", "", cnpj or "")
    if len(cnpj) not in {11, 14}:
        raise ValueError("CPF/CNPJ invalido.")

    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj

    pasta_analises = pasta_cnpj / "analises" / "produtos"
    pasta_brutos = pasta_cnpj / "arquivos_parquet"

    arq_prod_final = pasta_analises / f"produtos_final_{cnpj}.parquet"
    if not arq_prod_final.exists():
        rprint("[red]produtos_final.parquet nao encontrado.[/red]")
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
                "co_sefin_final",
                "unid_ref_sugerida",
            ]
        )
        .rename({"descricao_normalizada": "__descricao_normalizada__", "co_sefin_final": "co_sefin_agr"})
        .unique(subset=["__descricao_normalizada__"])
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

        df_out = (
            df_src
            .with_columns(_normalizar_descricao_expr(col_desc))
            .join(df_prod_final, on="__descricao_normalizada__", how="left")
        )

        faltantes = df_out.filter(pl.col("id_agrupado").is_null())
        if faltantes.height > 0:
            nome_log = f"{fonte}_agr_sem_id_agrupado_{cnpj}.parquet"
            salvar_para_parquet(faltantes, pasta_analises, nome_log)
            rprint(
                f"[red]Erro: {fonte} possui {faltantes.height} linhas sem id_agrupado. "
                f"Verifique {nome_log}.[/red]"
            )
            return False

        df_out = df_out.drop("__descricao_normalizada__")

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
