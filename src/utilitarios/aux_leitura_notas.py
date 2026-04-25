from pathlib import Path
import polars as pl


def ler_nfe_nfce(
    path: Path | None,
    cnpj: str,
    fonte: str,
    cfop_df: pl.DataFrame | None = None,
    print_status: bool = False,
) -> pl.DataFrame | None:
    """Le NFe ou NFCe, filtra pelo CNPJ emitente e mapeia colunas."""
    if path is None or not path.exists():
        print(f"  [!] {fonte} nao encontrado.")
        return None

    colunas_necesssarias = [
        "co_emitente",
        "prod_cprod",
        "prod_xprod",
        "prod_ncm",
        "prod_ucom",
        "co_cfop",
        "prod_vprod",
        "prod_vfrete",
        "prod_vseg",
        "prod_voutro",
        "prod_vdesc",
        "prod_qcom",
        "ide_dh_emi",
        "codigo_fonte",
    ]

    # R1: Coluna para id_linha_origem (chave_acesso + prod_nitem)
    colunas_necesssarias += ["chave_acesso", "prod_nitem"]

    # Otimização Bolt: pl.read_parquet_schema le a metadata sem alocar o DataFrame na memoria
    schema = pl.read_parquet_schema(path)

    # Identifica coluna de tipo de operacao (0=Entrada, 1=Saida)
    col_tp = next(
        (c for c in ["tipo_operacao", "co_tp_nf", "tp_nf"] if c in schema), None
    )
    if col_tp:
        colunas_necesssarias.append(col_tp)

    opcionais = {
        "prod_cest": "cest_raw",
        "prod_ceantrib": "ceantrib_raw",
        "prod_cean": "cean_raw",
    }
    presentes = {k: v for k, v in opcionais.items() if k in schema}

    selecionar = [c for c in colunas_necesssarias if c in schema] + list(
        presentes.keys()
    )

    lf = pl.scan_parquet(path).filter(pl.col("co_emitente") == cnpj)

    if col_tp:
        lf = lf.filter(pl.col(col_tp).cast(pl.String) == "1")  # Saida

    # Filtro de CFOP Mercantil 'X' se fornecido
    if cfop_df is not None and "co_cfop" in schema:
        lf = lf.with_columns(pl.col("co_cfop").cast(pl.String))
        lf = lf.join(cfop_df.lazy(), on="co_cfop", how="inner")

    df = lf.select(selecionar).collect()

    if df.is_empty():
        return None

    # Calculo do valor final do item (Saida)
    def _val(col: str) -> pl.Expr:
        """Retorna expressão Polars para a coluna, preenchendo nulos com 0 e convertendo para Float64."""
        return pl.col(col).fill_null(0).cast(pl.Float64)

    df = df.with_columns(
        [
            (
                _val("prod_vprod")
                + _val("prod_vfrete")
                + _val("prod_vseg")
                + _val("prod_voutro")
                - _val("prod_vdesc")
            ).alias("valor_saida"),
            _val("prod_qcom").alias("quantidade_saida"),
            pl.lit(0.0).alias("quantidade_entrada"),
            pl.col("ide_dh_emi").cast(pl.String).str.slice(0, 4).alias("ano"),
        ]
    )

    # GTIN
    if "prod_ceantrib" in df.columns and "prod_cean" in df.columns:
        df = df.with_columns(
            pl.when(pl.col("prod_ceantrib").is_null() | (pl.col("prod_ceantrib") == ""))
            .then(pl.col("prod_cean"))
            .otherwise(pl.col("prod_ceantrib"))
            .alias("gtin")
        )
    elif "prod_ceantrib" in df.columns:
        df = df.rename({"prod_ceantrib": "gtin"})
    elif "prod_cean" in df.columns:
        df = df.rename({"prod_cean": "gtin"})
    else:
        df = df.with_columns(pl.lit(None, pl.String).alias("gtin"))

    mapping = {
        "prod_cprod": "codigo",
        "prod_xprod": "descricao",
        "prod_ncm": "ncm",
        "prod_ucom": "unidade",
    }
    df = df.rename({k: v for k, v in mapping.items() if k in df.columns})

    if "prod_cest" in df.columns:
        df = df.rename({"prod_cest": "cest"})

    # Campos inexistentes nesta fonte
    df = df.with_columns(
        [
            pl.lit(None, pl.String).alias("descr_compl"),
            pl.lit(None, pl.String).alias("tipo_item"),
            pl.lit(0.0).alias("valor_entrada"),
        ]
    )

    # R1: Gerar id_linha_origem = chave_acesso|prod_nitem
    col_chave = "chave_acesso" if "chave_acesso" in df.columns else None
    col_nitem = "prod_nitem" if "prod_nitem" in df.columns else None
    if col_chave and col_nitem:
        df = df.with_columns(
            pl.concat_str(
                [pl.col(col_chave), pl.lit("|"), pl.col(col_nitem).cast(pl.String)]
            ).alias("id_linha_origem")
        )
    elif col_chave:
        df = df.with_columns(pl.col(col_chave).alias("id_linha_origem"))

    if print_status:
        print(f"  {fonte}: {len(df):,} linhas (emitente, saidas X)")

    return df


def ler_c170(
    path: Path | None,
    cfop_df: pl.DataFrame | None = None,
    ano_padrao: str = "",
    print_status: bool = False,
) -> pl.DataFrame | None:
    """Le c170_simplificada (ou c170) e mapeia colunas. Processa entradas e saidas."""
    if path is None or not path.exists():
        print("  [!] C170 nao encontrado.")
        return None

    # Otimização Bolt: pl.read_parquet_schema le a metadata sem alocar o DataFrame na memoria
    schema = pl.read_parquet_schema(path)

    col_map = {
        "cod_item": "codigo",
        "codigo_fonte": "codigo_fonte",
        "descr_item": "descricao",
        "descr_compl": "descr_compl",
        "tipo_item": "tipo_item",
        "cod_ncm": "ncm",
        "cest": "cest",
        "cod_barra": "gtin",
        "unid": "unidade",
        "vl_item": "valor_entrada",
        "co_cfop": "co_cfop",
        "ind_oper": "ind_oper",
        "qtd": "quantidade_entrada",
    }

    selecionar = [c for c in col_map.keys() if c in schema]

    lf = pl.scan_parquet(path)

    if cfop_df is not None and "co_cfop" in schema:
        lf = lf.with_columns(pl.col("co_cfop").cast(pl.String))
        lf = lf.join(cfop_df.lazy(), on="co_cfop", how="inner")

    df = lf.select(selecionar).collect().rename({c: col_map[c] for c in selecionar})

    if df.is_empty():
        return None

    def _val(col: str) -> pl.Expr:
        """Retorna expressão para a coluna quando existe no DataFrame; ``0.0`` quando ausente."""
        return (
            pl.col(col).fill_null(0).cast(pl.Float64)
            if col in df.columns
            else pl.lit(0.0)
        )

    df = df.with_columns(
        [
            pl.when(pl.col("ind_oper").cast(pl.String) == "0")
            .then(_val("valor_entrada"))
            .otherwise(0.0)
            .alias("valor_entrada"),
            pl.when(pl.col("ind_oper").cast(pl.String) == "0")
            .then(_val("quantidade_entrada"))
            .otherwise(0.0)
            .alias("quantidade_entrada"),
            pl.when(pl.col("ind_oper").cast(pl.String) == "1")
            .then(_val("valor_entrada"))
            .otherwise(0.0)
            .alias("valor_saida"),
            pl.when(pl.col("ind_oper").cast(pl.String) == "1")
            .then(_val("quantidade_entrada"))
            .otherwise(0.0)
            .alias("quantidade_saida"),
            pl.lit(ano_padrao).alias("ano"),
        ]
    )

    if print_status:
        print(f"  C170: {len(df):,} linhas (entradas e saidas internas X)")

    # R1: Gerar id_linha_origem para C170 (reg_0000_id|num_doc|num_item)
    # Colunas possiveis: reg_0000_id, num_doc (ou num_doc_ref), num_item
    col_reg = "reg_0000_id" if "reg_0000_id" in df.columns else None
    col_doc = "num_doc" if "num_doc" in df.columns else None
    col_item = "num_item" if "num_item" in df.columns else None
    partes_id = [c for c in [col_reg, col_doc, col_item] if c]
    if len(partes_id) >= 2:
        # Pelo menos doc + item estao presentes
        exprs = [pl.col(p).cast(pl.String) for p in partes_id]
        concat = pl.concat_str(exprs, separator="|")
        df = df.with_columns(concat.alias("id_linha_origem"))
    elif col_doc:
        df = df.with_columns(
            pl.concat_str(
                [
                    pl.col(col_doc).cast(pl.String),
                    pl.lit("|"),
                    pl.col("codigo").cast(pl.String),
                ]
            ).alias("id_linha_origem")
        )

    return df


def ler_bloco_h(path: Path | None, print_status: bool = False) -> pl.DataFrame | None:
    """Le Bloco H (inventario) e mapeia colunas para o formato comum."""
    if path is None or not path.exists():
        print("  [!] bloco_h nao encontrado.")
        return None

    # Otimização Bolt: pl.read_parquet_schema le a metadata sem alocar o DataFrame na memoria
    schema = pl.read_parquet_schema(path)

    def _pick(*candidates: str) -> str | None:
        """Retorna o primeiro candidato presente no schema do Parquet; None se nenhum existir."""
        for c in candidates:
            if c in schema:
                return c
        return None

    col_codigo = _pick("codigo_produto", "codigo_produto_original", "cod_item")
    col_codigo_fonte = _pick("codigo_fonte")
    col_desc = _pick("descricao_produto", "descr_item", "descricao")
    col_ncm = _pick("cod_ncm", "ncm")
    col_cest = _pick("cest")
    col_gtin = _pick("cod_barra", "gtin")
    col_tipo = _pick("tipo_item")
    col_unid = _pick("unidade_medida", "unidade_media", "unid", "unidade")
    col_qtd = _pick("quantidade", "qtd")
    col_vl_item = _pick("valor_item", "vl_item", "valor_total_item")
    col_ano = _pick("dt_inv", "data_inventario")

    if col_codigo is None or col_desc is None:
        print("  [!] bloco_h sem colunas minimas (codigo/descricao).")
        return None

    selecionar = [
        c
        for c in [
            col_codigo,
            col_codigo_fonte,
            col_desc,
            col_ncm,
            col_cest,
            col_gtin,
            col_tipo,
            col_unid,
            col_qtd,
            col_vl_item,
            col_ano,
        ]
        if c is not None
    ]

    df = pl.scan_parquet(path).select(selecionar).collect()
    if df.is_empty():
        return None

    def _num(col_name: str | None) -> pl.Expr:
        """Expressão numérica segura: retorna ``0.0`` quando a coluna não existir."""
        if col_name is None or col_name not in df.columns:
            return pl.lit(0.0)
        return pl.col(col_name).fill_null(0).cast(pl.Float64)

    out = df.with_columns(
        [
            pl.col(col_codigo).cast(pl.String).alias("codigo"),
            (
                pl.col(col_codigo_fonte).cast(pl.String)
                if col_codigo_fonte
                else pl.lit(None, pl.String)
            ).alias("codigo_fonte"),
            pl.col(col_desc).cast(pl.String).alias("descricao"),
            pl.lit(None, pl.String).alias("descr_compl"),
            (
                pl.col(col_tipo).cast(pl.String)
                if col_tipo
                else pl.lit(None, pl.String)
            ).alias("tipo_item"),
            (
                pl.col(col_ncm).cast(pl.String) if col_ncm else pl.lit(None, pl.String)
            ).alias("ncm"),
            (
                pl.col(col_cest).cast(pl.String)
                if col_cest
                else pl.lit(None, pl.String)
            ).alias("cest"),
            (
                pl.col(col_gtin).cast(pl.String)
                if col_gtin
                else pl.lit(None, pl.String)
            ).alias("gtin"),
            (
                pl.col(col_unid).cast(pl.String)
                if col_unid
                else pl.lit(None, pl.String)
            ).alias("unidade"),
            # Inventario nao e movimento. Aproveitamos quantidade/valor_item
            # como base de custo para apoiar fator automatico.
            _num(col_vl_item).alias("valor_entrada"),
            pl.lit(0.0).alias("valor_saida"),
            _num(col_qtd).alias("quantidade_entrada"),
            pl.lit(0.0).alias("quantidade_saida"),
            (
                pl.when(pl.col(col_ano).cast(pl.String).str.len_chars() >= 4)
                .then(pl.col(col_ano).cast(pl.String).str.slice(0, 4))
                .otherwise(pl.lit(""))
                if col_ano
                else pl.lit("")
            ).alias("ano"),
        ]
    )

    # R1: Gerar id_linha_origem para Bloco H (chave do inventario)
    # Colunas possiveis: dt_inv, num_inventario, ou usar codigo + dt_inv
    col_dt = "dt_inv" if "dt_inv" in df.columns else None
    col_num_inv = "num_inventario" if "num_inventario" in df.columns else None
    if col_num_inv and col_dt:
        out = out.with_columns(
            pl.concat_str(
                [
                    pl.col(col_num_inv).cast(pl.String),
                    pl.lit("|"),
                    pl.col(col_dt).cast(pl.String),
                ]
            ).alias("id_linha_origem")
        )
    elif col_dt:
        out = out.with_columns(
            pl.concat_str(
                [
                    pl.col("codigo").cast(pl.String),
                    pl.lit("|"),
                    pl.col(col_dt).cast(pl.String),
                ]
            ).alias("id_linha_origem")
        )
    else:
        out = out.with_columns(pl.col("codigo").alias("id_linha_origem"))

    out = out.select(
        [
            "codigo",
            "codigo_fonte",
            "descricao",
            "descr_compl",
            "tipo_item",
            "ncm",
            "cest",
            "gtin",
            "unidade",
            "valor_entrada",
            "valor_saida",
            "quantidade_entrada",
            "quantidade_saida",
            "ano",
        ]
        + (["id_linha_origem"] if "id_linha_origem" in out.columns else [])
    )

    if print_status:
        print(f"  bloco_h: {len(out):,} linhas (inventario)")
    return out
