import sys
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

def gerar_co_sefin_final(df: pl.DataFrame) -> pl.DataFrame:
    """Gera o co_sefin_final com base no ncm_padrao e cest_padrao."""
    path_cn = _resolver_ref("sitafe_cest_ncm.parquet")
    path_c = _resolver_ref("sitafe_cest.parquet")
    path_n = _resolver_ref("sitafe_ncm.parquet")

    if not any([path_cn, path_c, path_n]):
        rprint("[yellow]Aviso: Arquivos de referencia Sefin nao encontrados. co_sefin_final sera nulo.[/yellow]")
        return df.with_columns(pl.lit(None, pl.String).alias("co_sefin_final"))

    def _limpar_expr(col: str) -> pl.Expr:
        return pl.col(col).cast(pl.String).str.replace_all(r"\D", "").str.strip_chars()

    # Prepara as chaves de join limitadas aos padroes
    df_join = df.with_columns([
        _limpar_expr("ncm_padrao").alias("_ncm_j"),
        _limpar_expr("cest_padrao").alias("_cest_j")
    ])

    if path_cn is not None:
        ref_cn = pl.read_parquet(path_cn).select([
            _limpar_expr("it_nu_cest").alias("ref_cest"),
            _limpar_expr("it_nu_ncm").alias("ref_ncm"),
            pl.col("it_co_sefin").cast(pl.String).alias("co_sefin_cn"),
        ])
        df_join = df_join.join(ref_cn, left_on=["_cest_j", "_ncm_j"], right_on=["ref_cest", "ref_ncm"], how="left")
    else:
        df_join = df_join.with_columns(pl.lit(None, pl.String).alias("co_sefin_cn"))

    if path_c is not None:
        ref_c = pl.read_parquet(path_c).select([
            _limpar_expr("cest").alias("ref_cest_only"),
            pl.col("co-sefin").cast(pl.String).alias("co_sefin_c"),
        ])
        df_join = df_join.join(ref_c, left_on="_cest_j", right_on="ref_cest_only", how="left")
    else:
        df_join = df_join.with_columns(pl.lit(None, pl.String).alias("co_sefin_c"))

    if path_n is not None:
        ref_n = pl.read_parquet(path_n).select([
            _limpar_expr("ncm").alias("ref_ncm_only"),
            pl.col("co-sefin").cast(pl.String).alias("co_sefin_n"),
        ])
        df_join = df_join.join(ref_n, left_on="_ncm_j", right_on="ref_ncm_only", how="left")
    else:
        df_join = df_join.with_columns(pl.lit(None, pl.String).alias("co_sefin_n"))

    return (
        df_join
        .with_columns(pl.coalesce(["co_sefin_cn", "co_sefin_c", "co_sefin_n"]).alias("co_sefin_final"))
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
        "it_pc_interna", "it_in_st", "it_pc_mva", "it_in_mva_ajustado", 
        "it_in_isento_icms", "it_pc_reducao", "it_in_combustivel", "it_in_pmpf", "it_in_reducao_credito"
    ]
    
    # Se ja existem, dropamos do mov para nao dar DuplicateError e deixar o join trazer o correto.
    cols_a_dropar = [c for c in campos_incluir if c in df_movimentacao.columns] + ["co_sefin_agr"]
    cols_a_dropar = [c for c in cols_a_dropar if c in df_movimentacao.columns]
    if cols_a_dropar:
        df_movimentacao = df_movimentacao.drop(cols_a_dropar)

    # 1. Resolver co_sefin_padrao
    # Prioridade 1: Buscar do produtos_agrupados_{cnpj}.parquet se o CNPJ for informado
    df_mov = df_movimentacao
    sefin_source_col = "co_sefin_final" # Fallback local se nao achar o agrupado
    
    if cnpj:
        cnpj_limpo = "".join(filter(str.isdigit, cnpj))
        path_agrupado = DADOS_DIR / "CNPJ" / cnpj_limpo / "analises" / "produtos" / f"produtos_agrupados_{cnpj_limpo}.parquet"
        if path_agrupado.exists():
            df_agr = pl.read_parquet(path_agrupado).select(["id_agrupado", "co_sefin_padrao"])
            df_mov = df_mov.join(df_agr, on="id_agrupado", how="left")
            sefin_source_col = "co_sefin_padrao"
            rprint(f"[green]  Usando co_sefin_padrao de produtos_agrupados para {cnpj_limpo}[/green]")
        else:
            rprint(f"[yellow]  Arquivo {path_agrupado.name} nao encontrado. Gerando co_sefin_final via NCM/CEST.[/yellow]")
            df_mov = gerar_co_sefin_final(df_mov)
    else:
        df_mov = gerar_co_sefin_final(df_mov)

    # 2. Carregar sitafe_produto_sefin_aux.parquet
    caminho_aux = _resolver_ref("sitafe_produto_sefin_aux.parquet")
    if not caminho_aux or not caminho_aux.exists():
        rprint("[yellow]Aviso: sitafe_produto_sefin_aux.parquet nao encontrado.[/yellow]")
        return df_mov.with_columns(pl.col(sefin_source_col).alias("co_sefin_agr")) if sefin_source_col in df_mov.columns else df_mov

    df_aux = pl.read_parquet(caminho_aux)

    # Converter datas para formato de Data no df_aux
    df_aux = df_aux.with_columns([
        pl.col("it_da_inicio").cast(pl.String).str.strptime(pl.Date, "%Y%m%d", strict=False).alias("da_inicio"),
        pl.col("it_da_final").cast(pl.String).str.strptime(pl.Date, "%Y%m%d", strict=False).alias("da_final"),
        pl.col("it_co_sefin").cast(pl.String)
    ])

    # 3. Preparar a data de referencia em df_mov
    df_mov = df_mov.with_columns([
        pl.col("Dt_doc").cast(pl.Date, strict=False).alias("_dt_doc_date"),
        pl.col("Dt_e_s").cast(pl.Date, strict=False).alias("_dt_es_date"),
    ])
    df_mov = df_mov.with_columns([
        pl.max_horizontal(["_dt_doc_date", "_dt_es_date"]).alias("dt_referencia")
    ])

    # 4. Join e Filtro por Data
    col_id = "__unique_row_id"
    df_mov_id = df_mov.with_columns(pl.Series(col_id, range(df_mov.height)))
    
    # Left join para explodir rows que possam ter periodos diferentes (deduplicaremos com filter)
    df_joined = df_mov_id.join(df_aux, left_on=sefin_source_col, right_on="it_co_sefin", how="left")
    
    # Correcao da condicao: handles null da_final e null da_inicio
    cond_dentro_do_prazo = (
        (pl.col("da_inicio").is_null() | (pl.col("dt_referencia") >= pl.col("da_inicio"))) & 
        (pl.col("da_final").is_null() | (pl.col("dt_referencia") <= pl.col("da_final")))
    )
    
    df_filtered = (
        df_joined
        .filter(cond_dentro_do_prazo | pl.col("da_inicio").is_null())
        .unique(subset=[col_id], keep="first")
    )
    
    # 5. Tratamento de Órfãos (Fallbacks)
    # Se sobrou alguem sem match de data, pegamos o registro SITAFE mais recente para aquele CO_SEFIN
    df_aux_latest = (
        df_aux
        .sort("it_da_inicio", descending=True)
        .unique(subset=["it_co_sefin"], keep="first")
    )
    
    orphans = df_mov_id.join(df_filtered.select(col_id), on=col_id, how="anti")
    orphans_filled = orphans.join(df_aux_latest, left_on=sefin_source_col, right_on="it_co_sefin", how="left")
    
    # 6. Finalização e Concat
    df_filtered = df_filtered.with_columns(pl.col(sefin_source_col).alias("co_sefin_agr"))
    orphans_filled = orphans_filled.with_columns(pl.col(sefin_source_col).alias("co_sefin_agr"))
    
    todas_cols_finais = list(df_mov_id.columns) + campos_incluir + ["co_sefin_agr"]
    todas_cols_finais = list(dict.fromkeys(todas_cols_finais))
    
    df_final = pl.concat(
        [
            df_filtered.select(todas_cols_finais),
            orphans_filled.select(todas_cols_finais)
        ],
        how="vertical_relaxed"
    )
    
    # Cleanup
    return df_final.drop(["_dt_doc_date", "_dt_es_date", "dt_referencia", col_id, "da_inicio", "da_final", "it_co_sefin", "co_sefin_final", "co_sefin_padrao"], strict=False)

if __name__ == '__main__':
    # Teste isolado
    print("Módulo co_sefin_class carregado com sucesso.")
