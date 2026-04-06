"""
produtos_unidades.py

Objetivo: Gerar a tabela base de movimentacoes por unidade.
Campos: codigo, codigo_fonte, descricao, descr_compl, tipo_item, ncm, cest, co_sefin_item, gtin, unid, compras, vendas.
Fontes: Tabelas NFe, NFCe, C170 e bloco_h.
"""

import sys
import re
from pathlib import Path
from utilitarios.project_paths import PROJECT_ROOT
import polars as pl
from rich import print as rprint

ROOT_DIR = PROJECT_ROOT
SRC_DIR = ROOT_DIR / "src"
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"
REFS_DIR = DADOS_DIR / "referencias"


try:
    from utilitarios.salvar_para_parquet import salvar_para_parquet
    from utilitarios.encontrar_arquivo_cnpj import encontrar_arquivo
    from aux_leitura_notas import ler_nfe_nfce, ler_c170, ler_bloco_h
except ImportError as e:
    rprint(f"[red]Erro ao importar modulos utilitarios:[/red] {e}")
    sys.exit(1)


def _inferir_co_sefin(df: pl.DataFrame) -> pl.DataFrame:
    """Inferencia de co_sefin_item: CEST+NCM, depois CEST, depois NCM."""
    def _candidatos_ref_dir() -> list[Path]:
        return [
            REFS_DIR / "referencias" / "CO_SEFIN",
            REFS_DIR / "CO_SEFIN",
            ROOT_DIR / "referencias" / "CO_SEFIN",
        ]

    def _resolver_ref(nome_arquivo: str) -> Path | None:
        for base in _candidatos_ref_dir():
            p = base / nome_arquivo
            if p.exists():
                return p
        return None

    path_cn = _resolver_ref("sitafe_cest_ncm.parquet")
    path_c = _resolver_ref("sitafe_cest.parquet")
    path_n = _resolver_ref("sitafe_ncm.parquet")
    if not any([path_cn, path_c, path_n]):
        return df.with_columns(pl.lit(None, pl.String).alias("co_sefin_item"))

    def _limpar_expr(col: str) -> pl.Expr:
        return pl.col(col).cast(pl.String).str.replace_all(r"\D", "").str.strip_chars()

    df_join = df.with_columns([_limpar_expr("ncm").alias("_ncm_j"), _limpar_expr("cest").alias("_cest_j")])

    if path_cn is not None:
        ref_cn = pl.read_parquet(path_cn).select(
            [
                _limpar_expr("it_nu_cest").alias("ref_cest"),
                _limpar_expr("it_nu_ncm").alias("ref_ncm"),
                pl.col("it_co_sefin").cast(pl.String).alias("co_sefin_cn"),
            ]
        )
        df_join = df_join.join(ref_cn, left_on=["_cest_j", "_ncm_j"], right_on=["ref_cest", "ref_ncm"], how="left")
    else:
        df_join = df_join.with_columns(pl.lit(None, pl.String).alias("co_sefin_cn"))

    if path_c is not None:
        ref_c = pl.read_parquet(path_c).select(
            [
                _limpar_expr("cest").alias("ref_cest_only"),
                pl.col("co-sefin").cast(pl.String).alias("co_sefin_c"),
            ]
        )
        df_join = df_join.join(ref_c, left_on="_cest_j", right_on="ref_cest_only", how="left")
    else:
        df_join = df_join.with_columns(pl.lit(None, pl.String).alias("co_sefin_c"))

    if path_n is not None:
        ref_n = pl.read_parquet(path_n).select(
            [
                _limpar_expr("ncm").alias("ref_ncm_only"),
                pl.col("co-sefin").cast(pl.String).alias("co_sefin_n"),
            ]
        )
        df_join = df_join.join(ref_n, left_on="_ncm_j", right_on="ref_ncm_only", how="left")
    else:
        df_join = df_join.with_columns(pl.lit(None, pl.String).alias("co_sefin_n"))

    return (
        df_join
        .with_columns(pl.coalesce([pl.col("co_sefin_cn"), pl.col("co_sefin_c"), pl.col("co_sefin_n")]).alias("co_sefin_item"))
        .drop(["_ncm_j", "_cest_j", "co_sefin_cn", "co_sefin_c", "co_sefin_n"])
    )


def gerar_produtos_unidades(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj = re.sub(r"\D", "", cnpj)
    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj

    arq_dir = pasta_cnpj / "arquivos_parquet"
    if not arq_dir.exists():
        rprint(f"[red]Pasta de parquets nao encontrada: {arq_dir}[/red]")
        return False

    rprint(f"[bold cyan]Gerando produtos_unidades para CNPJ: {cnpj}[/bold cyan]")

    # 1. Filtro CFOP Mercantil 'X'
    cfop_candidates = [
        REFS_DIR / "referencias" / "cfop" / "cfop_bi.parquet",
        REFS_DIR / "cfop" / "cfop_bi.parquet",
        ROOT_DIR / "referencias" / "cfop" / "cfop_bi.parquet",
    ]
    cfop_bi_path = next((p for p in cfop_candidates if p.exists()), cfop_candidates[0])
    cfop_df = None
    if cfop_bi_path.exists():
        cfop_df = (
            pl.read_parquet(cfop_bi_path)
            .filter(pl.col("operacao_mercantil") == "X")
            .select(["co_cfop"])
            .with_columns(pl.col("co_cfop").cast(pl.String))
        )

    # 2. Leitura de fontes
    def _res(prefix: str):
        return encontrar_arquivo(arq_dir, prefix, cnpj) or encontrar_arquivo(pasta_cnpj, prefix, cnpj)

    fragmentos: list[pl.DataFrame] = []

    # NFe / NFCe (Vendas)
    for fonte in ["NFe", "NFCe"]:
        df = ler_nfe_nfce(_res(fonte), cnpj, fonte, cfop_df)
        if df is not None:
            df = df.rename({"valor_saida": "vendas", "valor_entrada": "compras"})
            fragmentos.append(df)

    # C170 (Compras e Saidas internas)
    df_c170 = ler_c170(_res("c170_simplificada") or _res("c170"), cfop_df)
    if df_c170 is not None:
        df_c170 = df_c170.rename({"valor_saida": "vendas", "valor_entrada": "compras"})
        fragmentos.append(df_c170)

    # Bloco H (Inventario): usa valor_item e quantidade para enriquecer base de custo por unidade
    df_bloco_h = ler_bloco_h(_res("bloco_h"))
    if df_bloco_h is not None:
        df_bloco_h = df_bloco_h.rename({"valor_saida": "vendas", "valor_entrada": "compras"})
        fragmentos.append(df_bloco_h)

    if not fragmentos:
        rprint("[red]Nenhuma fonte encontrada.[/red]")
        return False

    df_total = pl.concat(fragmentos, how="diagonal_relaxed")

    # Consolidar colunas nulas
    cols = ["codigo", "codigo_fonte", "descricao", "descr_compl", "tipo_item", "ncm", "cest", "gtin", "unidade"]
    for c in cols:
        if c not in df_total.columns:
            df_total = df_total.with_columns(pl.lit(None, pl.String).alias(c))

    # 3. Agrupamento por (item + unidade)
    df_grouped = (
        df_total.group_by(["codigo", "codigo_fonte", "descricao", "descr_compl", "tipo_item", "ncm", "cest", "gtin", "unidade"])
        .agg(
            [
                pl.col("compras").fill_null(0).sum().alias("compras"),
                pl.col("vendas").fill_null(0).sum().alias("vendas"),
                pl.col("quantidade_entrada").fill_null(0).sum().alias("qtd_compras"),
                pl.col("quantidade_saida").fill_null(0).sum().alias("qtd_vendas"),
            ]
        )
        .rename({"unidade": "unid"})
    )

    # 4. Inferir co_sefin_item
    df_grouped = _inferir_co_sefin(df_grouped)

    # 5. Salvar
    pasta_saida = pasta_cnpj / "analises" / "produtos"
    ok = salvar_para_parquet(df_grouped, pasta_saida, f"produtos_unidades_{cnpj}.parquet")
    return ok


if __name__ == "__main__":
    if len(sys.argv) > 1:
        gerar_produtos_unidades(sys.argv[1])
    else:
        c = input("CNPJ: ")
        gerar_produtos_unidades(c)


