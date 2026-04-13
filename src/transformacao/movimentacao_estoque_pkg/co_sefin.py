"""
co_sefin.py

Script para inferir o codigo co_sefin_inferido com base no NCM e CEST
utilizando tabelas de referencia sitafe.
"""

import sys
from pathlib import Path
from utilitarios.project_paths import PROJECT_ROOT

import polars as pl
from rich import print as rprint

ROOT_DIR = PROJECT_ROOT
SRC_DIR = ROOT_DIR / "src"
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"
REFS_DIR = DADOS_DIR / "referencias" / "CO_SEFIN"


try:
    from utilitarios.salvar_para_parquet import salvar_para_parquet
    from utilitarios.validar_cnpj import validar_cnpj
except ImportError as e:
    rprint(f"[red]Erro ao importar modulos utilitarios:[/red] {e}")
    sys.exit(1)


def _limpar_expr(coluna: str) -> pl.Expr:
    return pl.col(coluna).cast(pl.String, strict=False).str.replace_all(r"\.", "").str.strip_chars()


def _resolver_ref(nome_arquivo: str) -> Path:
    candidatos = [
        DADOS_DIR / "referencias" / "referencias" / "CO_SEFIN",
        DADOS_DIR / "referencias" / "CO_SEFIN",
        ROOT_DIR / "referencias" / "CO_SEFIN",
    ]
    for base in candidatos:
        caminho = base / nome_arquivo
        if caminho.exists():
            return caminho
    return REFS_DIR / nome_arquivo


def inferir_co_sefin_dataframe(
    df: pl.DataFrame,
    col_ncm: str = "ncm",
    col_cest: str = "cest",
    output_col: str = "co_sefin_inferido",
) -> pl.DataFrame:
    """Infere co_sefin em um DataFrame com a mesma prioridade do pipeline.

    Ordem de fallback:
    1. CEST + NCM
    2. CEST
    3. NCM
    """
    if df.is_empty():
        return df.with_columns(pl.lit(None, dtype=pl.String).alias(output_col))

    ref_cest_ncm_path = _resolver_ref("sitafe_cest_ncm.parquet")
    ref_cest_path = _resolver_ref("sitafe_cest.parquet")
    ref_ncm_path = _resolver_ref("sitafe_ncm.parquet")

    for p in [ref_cest_ncm_path, ref_cest_path, ref_ncm_path]:
        if not p.exists():
            raise FileNotFoundError(f"Tabela de referencia nao encontrada: {p}")

    df_base = df
    if col_ncm not in df_base.columns:
        df_base = df_base.with_columns(pl.lit(None, dtype=pl.String).alias(col_ncm))
    if col_cest not in df_base.columns:
        df_base = df_base.with_columns(pl.lit(None, dtype=pl.String).alias(col_cest))

    ref_cn = (
        pl.read_parquet(ref_cest_ncm_path)
        .select(["it_nu_cest", "it_nu_ncm", "it_co_sefin"])
        .with_columns(
            [
                _limpar_expr("it_nu_cest").alias("ref_cest"),
                _limpar_expr("it_nu_ncm").alias("ref_ncm"),
                pl.col("it_co_sefin").cast(pl.String, strict=False),
            ]
        )
        .drop(["it_nu_cest", "it_nu_ncm"])
    )

    ref_c = (
        pl.read_parquet(ref_cest_path)
        .select(["cest", "co-sefin"])
        .with_columns(_limpar_expr("cest").alias("ref_cest"))
        .drop("cest")
        .rename({"co-sefin": "co_sefin_cest"})
    )

    ref_n = (
        pl.read_parquet(ref_ncm_path)
        .select(["ncm", "co-sefin"])
        .with_columns(_limpar_expr("ncm").alias("ref_ncm"))
        .drop("ncm")
        .rename({"co-sefin": "co_sefin_ncm"})
    )

    df_join = df_base.with_columns(
        [
            _limpar_expr(col_ncm).alias("_ncm_join"),
            _limpar_expr(col_cest).alias("_cest_join"),
        ]
    )
    df_join = df_join.join(
        ref_cn,
        left_on=["_cest_join", "_ncm_join"],
        right_on=["ref_cest", "ref_ncm"],
        how="left",
    )
    df_join = df_join.join(ref_c, left_on="_cest_join", right_on="ref_cest", how="left")
    df_join = df_join.join(ref_n, left_on="_ncm_join", right_on="ref_ncm", how="left")

    return (
        df_join.with_columns(
            pl.coalesce(["it_co_sefin", "co_sefin_cest", "co_sefin_ncm"])
            .cast(pl.String, strict=False)
            .alias(output_col)
        )
        .drop(["_ncm_join", "_cest_join", "it_co_sefin", "co_sefin_cest", "co_sefin_ncm"], strict=False)
    )


def co_sefin(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    import re

    cnpj = re.sub(r"[^0-9]", "", cnpj)
    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj

    pasta_produtos = pasta_cnpj / "analises" / "produtos"
    arquivos_alvo = [
        pasta_produtos / f"tabela_itens_caracteristicas_{cnpj}.parquet",
        pasta_produtos / f"tab_itens_caract_normalizada_{cnpj}.parquet",
    ]

    rprint(f"\n[bold cyan]Inferindo co_sefin_inferido para CNPJ: {cnpj}[/bold cyan]")

    sucesso_total = True
    encontrado_qualquer = False
    for arq in arquivos_alvo:
        if not arq.exists():
            rprint(f"[yellow]Aviso: Arquivo nao encontrado: {arq.resolve()}[/yellow]")
            continue

        encontrado_qualquer = True
        rprint(f"[cyan]Processando {arq.name}...[/cyan]")

        try:
            df_result = inferir_co_sefin_dataframe(pl.read_parquet(arq), col_ncm="ncm", col_cest="cest")
        except Exception as exc:
            rprint(f"[red]Erro ao inferir co_sefin em {arq.name}:[/red] {exc}")
            sucesso_total = False
            continue

        ok = salvar_para_parquet(df_result, arq.parent, arq.name)
        if not ok:
            sucesso_total = False

    return sucesso_total if encontrado_qualquer else False


if __name__ == "__main__":
    import re

    if len(sys.argv) > 1:
        cnpj_arg = sys.argv[1]
    else:
        try:
            cnpj_arg = input("Informe o CNPJ: ").strip()
        except (KeyboardInterrupt, EOFError):
            rprint("\n[yellow]Cancelado.[/yellow]")
            sys.exit(0)

    cnpj_arg = re.sub(r"[^0-9]", "", cnpj_arg)

    if not validar_cnpj(cnpj_arg):
        rprint(f"[red]CNPJ invalido: {cnpj_arg}[/red]")
        sys.exit(1)

    sucesso = co_sefin(cnpj_arg)
    sys.exit(0 if sucesso else 1)


