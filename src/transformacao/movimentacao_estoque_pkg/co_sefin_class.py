from pathlib import Path
import polars as pl
from rich import print as rprint

ROOT_DIR = Path(r"c:\funcoes - Copia")
DADOS_DIR = ROOT_DIR / "dados"
REFS_DIR = DADOS_DIR / "referencias"

def _resolver_ref(nome_arquivo: str) -> Path | None:
    candidatos = [
        REFS_DIR / "referencias" / "CO_SEFIN",
        REFS_DIR / "CO_SEFIN",
        ROOT_DIR / "referencias" / "CO_SEFIN",
    ]
    for base in candidatos:
        p = base / nome_arquivo
        if p.exists():
            return p
    return None


def _garantir_colunas(df: pl.DataFrame, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    for coluna, dtype in schema.items():
        if coluna not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=dtype).alias(coluna))
    return df


def _resolver_produtos_agrupados(cnpj: str) -> Path:
    cnpj_limpo = "".join(filter(str.isdigit, cnpj))
    return DADOS_DIR / "CNPJ" / cnpj_limpo / "analises" / "produtos" / f"produtos_agrupados_{cnpj_limpo}.parquet"


def _carregar_co_sefin_padrao(cnpj: str) -> pl.DataFrame | None:
    path_agrupado = _resolver_produtos_agrupados(cnpj)
    if not path_agrupado.exists():
        return None

    df_agr = pl.read_parquet(path_agrupado)
    if "id_agrupado" not in df_agr.columns or "co_sefin_padrao" not in df_agr.columns:
        rprint(
            f"[yellow]Aviso: {path_agrupado.name} nao possui as colunas "
            "'id_agrupado' e 'co_sefin_padrao'.[/yellow]"
        )
        return None

    return (
        df_agr
        .select(
            [
                pl.col("id_agrupado").cast(pl.String, strict=False),
                pl.col("co_sefin_padrao").cast(pl.String, strict=False),
            ]
        )
        .unique(subset=["id_agrupado"], keep="first")
    )

def gerar_co_sefin_final(df: pl.DataFrame) -> pl.DataFrame:
    """Gera o co_sefin_final com base no ncm_padrao e cest_padrao."""
    path_cn = _resolver_ref("sitafe_cest_ncm.parquet")
    path_c = _resolver_ref("sitafe_cest.parquet")
    path_n = _resolver_ref("sitafe_ncm.parquet")

    if not any([path_cn, path_c, path_n]):
        rprint("[yellow]Aviso: Arquivos de referencia Sefin nao encontrados. co_sefin_final sera nulo.[/yellow]")
        return df.with_columns(pl.lit(None, pl.String).alias("co_sefin_final"))

    def _limpar_expr(col: str) -> pl.Expr:
        return pl.col(col).cast(pl.String, strict=False).str.replace_all(r"\D", "").str.strip_chars()

    df = _garantir_colunas(df, {"ncm_padrao": pl.String, "cest_padrao": pl.String})
    df_join = df.with_columns(
        [
            _limpar_expr("ncm_padrao").alias("_ncm_j"),
            _limpar_expr("cest_padrao").alias("_cest_j"),
        ]
    )

    if path_cn is not None:
        ref_cn = pl.read_parquet(path_cn).select(
            [
                _limpar_expr("it_nu_cest").alias("ref_cest"),
                _limpar_expr("it_nu_ncm").alias("ref_ncm"),
                pl.col("it_co_sefin").cast(pl.String, strict=False).alias("co_sefin_cn"),
            ]
        )
        df_join = df_join.join(ref_cn, left_on=["_cest_j", "_ncm_j"], right_on=["ref_cest", "ref_ncm"], how="left")
    else:
        df_join = df_join.with_columns(pl.lit(None, pl.String).alias("co_sefin_cn"))

    if path_c is not None:
        ref_c = pl.read_parquet(path_c).select(
            [
                _limpar_expr("cest").alias("ref_cest_only"),
                pl.col("co-sefin").cast(pl.String, strict=False).alias("co_sefin_c"),
            ]
        )
        df_join = df_join.join(ref_c, left_on="_cest_j", right_on="ref_cest_only", how="left")
    else:
        df_join = df_join.with_columns(pl.lit(None, pl.String).alias("co_sefin_c"))

    if path_n is not None:
        ref_n = pl.read_parquet(path_n).select(
            [
                _limpar_expr("ncm").alias("ref_ncm_only"),
                pl.col("co-sefin").cast(pl.String, strict=False).alias("co_sefin_n"),
            ]
        )
        df_join = df_join.join(ref_n, left_on="_ncm_j", right_on="ref_ncm_only", how="left")
    else:
        df_join = df_join.with_columns(pl.lit(None, pl.String).alias("co_sefin_n"))

    return (
        df_join
        .with_columns(
            pl.coalesce([pl.col("co_sefin_cn"), pl.col("co_sefin_c"), pl.col("co_sefin_n")]).alias("co_sefin_final")
        )
        .drop(["_ncm_j", "_cest_j", "co_sefin_cn", "co_sefin_c", "co_sefin_n"])
    )

def enriquecer_co_sefin_class(df_movimentacao: pl.DataFrame, cnpj: str = None) -> pl.DataFrame:
    """
    Enriquece a movimentacao de estoque com campos baseados na classificacao co_sefin.
    Utiliza co_sefin_padrao do produtos_agrupados como principal chave de classificação.
    """
    if df_movimentacao.height == 0:
        return df_movimentacao

    campos_incluir = [
        "it_pc_interna",
        "it_in_st",
        "it_pc_mva",
        "it_in_mva_ajustado",
        "it_in_isento_icms",
        "it_in_reducao",
        "it_pc_reducao",
        "it_in_combustivel",
        "it_in_pmpf",
        "it_in_reducao_credito",
    ]

    cols_a_dropar = [c for c in campos_incluir if c in df_movimentacao.columns] + ["co_sefin_agr"]
    cols_a_dropar = [c for c in cols_a_dropar if c in df_movimentacao.columns]
    if cols_a_dropar:
        df_movimentacao = df_movimentacao.drop(cols_a_dropar)

    df_mov = gerar_co_sefin_final(df_movimentacao)

    if cnpj and "id_agrupado" in df_mov.columns:
        df_agr = _carregar_co_sefin_padrao(cnpj)
        if df_agr is not None:
            df_mov = df_mov.join(df_agr, on="id_agrupado", how="left")
            rprint(f"[green]  Usando co_sefin_padrao de produtos_agrupados para {''.join(filter(str.isdigit, cnpj))}[/green]")
    elif cnpj:
        rprint("[yellow]Aviso: coluna id_agrupado nao encontrada. Mantendo fallback por NCM/CEST.[/yellow]")

    if "co_sefin_padrao" not in df_mov.columns:
        df_mov = df_mov.with_columns(pl.lit(None, pl.String).alias("co_sefin_padrao"))

    df_mov = df_mov.with_columns(
        pl.coalesce([pl.col("co_sefin_padrao"), pl.col("co_sefin_final")]).alias("__co_sefin_lookup__")
    )

    # 2. Carregar sitafe_produto_sefin_aux.parquet
    caminho_aux = _resolver_ref("sitafe_produto_sefin_aux.parquet")
    if not caminho_aux or not caminho_aux.exists():
        rprint("[yellow]Aviso: sitafe_produto_sefin_aux.parquet nao encontrado.[/yellow]")
        return df_mov.with_columns(pl.col("__co_sefin_lookup__").alias("co_sefin_agr")).drop(
            ["__co_sefin_lookup__", "co_sefin_final", "co_sefin_padrao"],
            strict=False,
        )

    df_aux = pl.read_parquet(caminho_aux)
    df_aux = _garantir_colunas(df_aux, {campo: pl.String for campo in campos_incluir})
    df_aux = df_aux.with_columns(
        [
            pl.col("it_da_inicio").cast(pl.String, strict=False).str.strptime(pl.Date, "%Y%m%d", strict=False).alias("da_inicio"),
            pl.col("it_da_final").cast(pl.String, strict=False).str.strptime(pl.Date, "%Y%m%d", strict=False).alias("da_final"),
            pl.col("it_co_sefin").cast(pl.String, strict=False).alias("it_co_sefin"),
        ]
    )

    df_mov = _garantir_colunas(df_mov, {"Dt_doc": pl.Date, "Dt_e_s": pl.Date})
    df_mov = df_mov.with_columns(
        [
            pl.col("Dt_doc").cast(pl.Date, strict=False).alias("_dt_doc_date"),
            pl.col("Dt_e_s").cast(pl.Date, strict=False).alias("_dt_es_date"),
        ]
    )
    df_mov = df_mov.with_columns(
        pl.max_horizontal([pl.col("_dt_doc_date"), pl.col("_dt_es_date")]).alias("dt_referencia")
    )

    # 4. Join e Filtro por Data
    col_id = "__unique_row_id"
    df_mov_id = df_mov.with_row_index(col_id)
    
    # Left join para explodir rows que possam ter periodos diferentes (deduplicaremos com filter)
    df_joined = df_mov_id.join(df_aux, left_on="__co_sefin_lookup__", right_on="it_co_sefin", how="left")
    
    # Correcao da condicao: handles null da_final e null da_inicio
    cond_dentro_do_prazo = (
        (pl.col("da_inicio").is_null() | (pl.col("dt_referencia") >= pl.col("da_inicio"))) & 
        (pl.col("da_final").is_null() | (pl.col("dt_referencia") <= pl.col("da_final")))
    )
    
    df_filtered = (
        df_joined
        .filter(cond_dentro_do_prazo)
        .with_columns(
            [
                pl.col("da_inicio").is_not_null().alias("__tem_inicio__"),
                pl.col("da_final").is_not_null().alias("__tem_final__"),
            ]
        )
        .sort(
            [col_id, "__tem_inicio__", "da_inicio", "__tem_final__", "da_final"],
            descending=[False, True, True, True, True],
            nulls_last=True,
        )
        .unique(subset=[col_id], keep="first")
    )
    
    # 5. Tratamento de Órfãos (Fallbacks)
    # Se sobrou alguem sem match de data, pegamos o registro SITAFE mais recente para aquele CO_SEFIN
    df_aux_latest = (
        df_aux
        .with_columns(
            [
                pl.col("da_inicio").is_not_null().alias("__tem_inicio__"),
                pl.col("da_final").is_not_null().alias("__tem_final__"),
            ]
        )
        .sort(
            ["it_co_sefin", "__tem_inicio__", "da_inicio", "__tem_final__", "da_final"],
            descending=[False, True, True, True, True],
            nulls_last=True,
        )
        .unique(subset=["it_co_sefin"], keep="first")
    )
    
    orphans = df_mov_id.join(df_filtered.select(col_id), on=col_id, how="anti")
    orphans_filled = orphans.join(df_aux_latest, left_on="__co_sefin_lookup__", right_on="it_co_sefin", how="left")
    
    # 6. Finalização e Concat
    df_filtered = df_filtered.with_columns(pl.col("__co_sefin_lookup__").alias("co_sefin_agr"))
    orphans_filled = orphans_filled.with_columns(pl.col("__co_sefin_lookup__").alias("co_sefin_agr"))
    
    todas_cols_finais = list(df_mov_id.columns) + campos_incluir + ["co_sefin_agr"]
    todas_cols_finais = list(dict.fromkeys(todas_cols_finais))
    
    df_final = pl.concat(
        [
            df_filtered.select(todas_cols_finais),
            orphans_filled.select(todas_cols_finais)
        ],
        how="vertical_relaxed"
    )

    return df_final.drop(
        [
            "_dt_doc_date",
            "_dt_es_date",
            "dt_referencia",
            col_id,
            "da_inicio",
            "da_final",
            "it_co_sefin",
            "co_sefin_final",
            "co_sefin_padrao",
            "__co_sefin_lookup__",
            "__tem_inicio__",
            "__tem_final__",
        ],
        strict=False,
    )

if __name__ == '__main__':
    # Teste isolado
    print("Módulo co_sefin_class carregado com sucesso.")
