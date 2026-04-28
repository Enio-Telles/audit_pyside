"""
item_unidades.py

Objetivo: Gerar a tabela base de itens por unidade a partir das fontes
C170, Bloco H, NFe e NFCe.

Campos:
- id_item_unid
- codigo
- descricao
- descr_compl
- tipo_item
- ncm
- cest
- co_sefin_item
- gtin
- unid
- compras
- qtd_compras
- vendas
- qtd_vendas
- fontes
"""

from __future__ import annotations

import re
import sys
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
    from utilitarios.codigo_fonte import expr_gerar_codigo_fonte
except ImportError as e:
    rprint(f"[red]Erro ao importar modulos utilitarios:[/red] {e}")
    sys.exit(1)


def _candidatos_cfop_bi() -> list[Path]:
    """Returns candidate file paths for the CFOP classification reference.

    Returns:
        List of Path candidates for cfop_bi.parquet, checked in order.
    """
    return [
        REFS_DIR / "referencias" / "cfop" / "cfop_bi.parquet",
        REFS_DIR / "cfop" / "cfop_bi.parquet",
        ROOT_DIR / "referencias" / "cfop" / "cfop_bi.parquet",
    ]


def _carregar_cfops_mercantis() -> pl.DataFrame | None:
    """Loads mercantile CFOP codes from the reference Parquet file.

    Returns:
        DataFrame with columns ``co_cfop`` and ``operacao_mercantil`` filtered
        to rows where ``operacao_mercantil == 'X'``, or ``None`` if the
        reference file is absent or lacks the required columns.
    """
    caminho = next((p for p in _candidatos_cfop_bi() if p.exists()), None)
    if caminho is None:
        return None

    df = pl.read_parquet(caminho)
    if "co_cfop" not in df.columns or "operacao_mercantil" not in df.columns:
        return None

    return (
        df.select(
            [
                pl.col("co_cfop").cast(pl.String).str.strip_chars().alias("co_cfop"),
                pl.col("operacao_mercantil")
                .cast(pl.String)
                .str.strip_chars()
                .alias("operacao_mercantil"),
            ]
        )
        .filter(pl.col("operacao_mercantil") == "X")
        .unique(subset=["co_cfop"])
    )


def _inferir_co_sefin(df: pl.DataFrame) -> pl.DataFrame:
    """Infers ``co_sefin_item`` by joining against SEFIN reference tables.

    Attempts three lookups in priority order: CEST+NCM combined, CEST only,
    NCM only.  If no reference file is found, the column is populated with
    ``None``.

    Args:
        df: Input DataFrame containing at least ``ncm`` and ``cest`` columns.

    Returns:
        DataFrame with ``co_sefin_item`` column added and join helper columns
        removed.
    """

    def _candidatos_ref_dir() -> list[Path]:
        """Returns candidate base directories for CO_SEFIN reference files."""
        return [
            REFS_DIR / "referencias" / "CO_SEFIN",
            REFS_DIR / "CO_SEFIN",
            ROOT_DIR / "referencias" / "CO_SEFIN",
        ]

    def _resolver_ref(nome_arquivo: str) -> Path | None:
        """Resolves a reference file path from the CO_SEFIN candidate directories.

        Args:
            nome_arquivo: Filename to look for (e.g. ``sitafe_cest_ncm.parquet``).

        Returns:
            First existing Path, or ``None`` if not found in any candidate.
        """
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
        """Returns a Polars expression that strips all non-digit characters from a column.

        Args:
            col: Column name to clean.

        Returns:
            Polars expression producing a stripped numeric string.
        """
        return (
            pl.col(col).cast(pl.String, strict=False).str.replace_all(r"\D", "").str.strip_chars()
        )

    df_join = df.with_columns(
        [_limpar_expr("ncm").alias("_ncm_j"), _limpar_expr("cest").alias("_cest_j")]
    )

    if path_cn is not None:
        ref_cn = pl.read_parquet(path_cn).select(
            [
                _limpar_expr("it_nu_cest").alias("ref_cest"),
                _limpar_expr("it_nu_ncm").alias("ref_ncm"),
                pl.col("it_co_sefin").cast(pl.String).alias("co_sefin_cn"),
            ]
        )
        df_join = df_join.join(
            ref_cn,
            left_on=["_cest_j", "_ncm_j"],
            right_on=["ref_cest", "ref_ncm"],
            how="left",
        )
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

    return df_join.with_columns(
        pl.coalesce([pl.col("co_sefin_cn"), pl.col("co_sefin_c"), pl.col("co_sefin_n")]).alias(
            "co_sefin_item"
        )
    ).drop(["_ncm_j", "_cest_j", "co_sefin_cn", "co_sefin_c", "co_sefin_n"])


def _resolver_arquivo_base(pasta_cnpj: Path, prefixo: str, cnpj: str) -> Path | None:
    """Finds a raw Parquet file for the given CNPJ by prefix.

    Searches ``pasta_cnpj/arquivos_parquet/`` first, then ``pasta_cnpj/``
    directly.

    Args:
        pasta_cnpj: Root directory for the CNPJ's data.
        prefixo: Filename prefix to search for (e.g. ``'c170'``, ``'nfe'``).
        cnpj: 14-digit CNPJ string used by the filename locator.

    Returns:
        Resolved Path if found, otherwise ``None``.
    """
    pasta_brutos = pasta_cnpj / "arquivos_parquet"
    return encontrar_arquivo(pasta_brutos, prefixo, cnpj) or encontrar_arquivo(
        pasta_cnpj, prefixo, cnpj
    )


def _normalizar_texto(col: str | pl.Expr) -> pl.Expr:
    """Returns a Polars expression that casts a column to String and strips whitespace.

    Args:
        col: Column name or expression to normalize.

    Returns:
        Polars expression producing a trimmed string.
    """
    expr = pl.col(col) if isinstance(col, str) else col
    return expr.cast(pl.String, strict=False).str.strip_chars()


def _agregar_lista_str(col: str, alias: str) -> pl.Expr:
    """Returns a Polars aggregation expression for unique, sorted, non-null strings.

    Args:
        col: Column name to aggregate.
        alias: Output column name.

    Returns:
        Polars expression that collects distinct non-empty values as a sorted
        list, aliased to ``alias``.
    """
    return (
        pl.col(col)
        .cast(pl.String, strict=False)
        .str.strip_chars()
        .replace("", None)
        .drop_nulls()
        .unique()
        .sort()
        .alias(alias)
    )


def _num_expr(col: str | pl.Expr) -> pl.Expr:
    """Returns a Polars expression that casts a column to Float64 with nulls as 0.0.

    Args:
        col: Column name or expression to cast.

    Returns:
        Polars expression producing a Float64 column with no nulls.
    """
    expr = pl.col(col) if isinstance(col, str) else col
    return expr.cast(pl.Float64, strict=False).fill_null(0.0)


def _garantir_colunas(df: pl.DataFrame, colunas: list[str]) -> pl.DataFrame:
    """Ensures a DataFrame contains all required columns, adding null columns if absent.

    Args:
        df: Input DataFrame to check.
        colunas: Column names that must be present.

    Returns:
        DataFrame with any missing columns added as ``pl.String`` null columns.
    """
    for coluna in colunas:
        if coluna not in df.columns:
            df = df.with_columns(pl.lit(None, pl.String).alias(coluna))
    return df


def _ler_c170(
    path: Path | None, cnpj: str, cfop_mercantil: pl.DataFrame | None = None
) -> pl.DataFrame | None:
    """Reads the C170 Parquet file and calculates purchase/sale totals.

    Filters to purchase rows (``ind_oper == '0'``) and optionally joins
    against mercantile CFOP codes to classify ``compras`` values.

    Args:
        path: Path to the C170 Parquet file, or ``None``.
        cnpj: 14-digit CNPJ of the emitter used to identify source.
        cfop_mercantil: Reference DataFrame of mercantile CFOPs, or ``None``.

    Returns:
        DataFrame with columns ``codigo``, ``descricao``, ``ncm``, ``cest``,
        ``gtin``, ``unid``, ``compras``, ``qtd_compras``, ``vendas``,
        ``qtd_vendas``, ``fonte``; or ``None`` if the file is absent or empty.
    """
    if path is None or not path.exists():
        return None

    # OtimizaÃ§Ã£o Bolt: pl.read_parquet_schema le a metadata sem alocar o DataFrame na memoria
    schema = pl.read_parquet_schema(path)
    col_cfop = "co_cfop" if "co_cfop" in schema else "cfop" if "cfop" in schema else None
    selecionar = [
        c
        for c in [
            "codigo_fonte",
            "cod_item",
            "descr_item",
            "descr_compl",
            "tipo_item",
            "cod_ncm",
            "cest",
            "cod_barra",
            "unid",
            "vl_item",
            "qtd",
            "ind_oper",
            col_cfop,
        ]
        if c is not None and c in schema
    ]

    if "ind_oper" not in schema:
        return None

    lf = pl.scan_parquet(path).select(selecionar)
    if col_cfop is not None:
        lf = lf.with_columns(pl.col(col_cfop).cast(pl.String).str.strip_chars().alias("co_cfop"))
        if cfop_mercantil is not None:
            lf = lf.join(
                cfop_mercantil.lazy().with_columns(pl.lit(True).alias("__cfop_mercantil__")),
                on="co_cfop",
                how="left",
            ).with_columns(pl.col("__cfop_mercantil__").fill_null(False))

    df = lf.collect()
    if df.is_empty():
        return None

    return df.with_columns(
        [
            _normalizar_texto("cod_item").alias("codigo"),
            _normalizar_texto("descr_item").alias("descricao"),
            (
                _normalizar_texto("descr_compl").alias("descr_compl")
                if "descr_compl" in df.columns
                else pl.lit(None, pl.String).alias("descr_compl")
            ),
            (
                _normalizar_texto("tipo_item").alias("tipo_item")
                if "tipo_item" in df.columns
                else pl.lit(None, pl.String).alias("tipo_item")
            ),
            (
                _normalizar_texto("cod_ncm").alias("ncm")
                if "cod_ncm" in df.columns
                else pl.lit(None, pl.String).alias("ncm")
            ),
            (
                _normalizar_texto("cest").alias("cest")
                if "cest" in df.columns
                else pl.lit(None, pl.String).alias("cest")
            ),
            (
                _normalizar_texto("cod_barra").alias("gtin")
                if "cod_barra" in df.columns
                else pl.lit(None, pl.String).alias("gtin")
            ),
            (
                _normalizar_texto("unid").alias("unid")
                if "unid" in df.columns
                else pl.lit(None, pl.String).alias("unid")
            ),
            (
                expr_gerar_codigo_fonte(
                    pl.lit(cnpj),
                    pl.col("cod_item") if "cod_item" in df.columns else pl.col("prod_cprod"),
                    pl.col("descr_item") if "descr_item" in df.columns else pl.col("prod_xprod"),
                ).alias("codigo_fonte")
            ),
            pl.when(
                (pl.col("ind_oper").cast(pl.String) == "0")
                & (
                    pl.col("__cfop_mercantil__")
                    if "__cfop_mercantil__" in df.columns
                    else pl.lit(True)
                )
            )
            .then(_num_expr("vl_item"))
            .otherwise(0.0)
            .alias("compras"),
            pl.when(
                (pl.col("ind_oper").cast(pl.String) == "0")
                & (
                    pl.col("__cfop_mercantil__")
                    if "__cfop_mercantil__" in df.columns
                    else pl.lit(True)
                )
            )
            .then(_num_expr("qtd"))
            .otherwise(0.0)
            .alias("qtd_compras"),
            pl.lit(0.0).alias("vendas"),
            pl.lit(0.0).alias("qtd_vendas"),
            pl.lit("c170").alias("fonte"),
        ]
    ).select(
        [
            "codigo",
            "descricao",
            "descr_compl",
            "tipo_item",
            "ncm",
            "cest",
            "gtin",
            "unid",
            "codigo_fonte",
            "compras",
            "qtd_compras",
            "vendas",
            "qtd_vendas",
            "fonte",
        ]
    )


def _ler_bloco_h(path: Path | None, cnpj: str) -> pl.DataFrame | None:
    """Reads the Bloco H Parquet file and prepares the products for grouping.

    Adapts to varying column naming conventions across EFD extractions using
    schema introspection.

    Args:
        path: Path to the Bloco H Parquet file, or ``None``.

    Returns:
        DataFrame with columns ``codigo``, ``descricao``, ``ncm``, ``cest``,
        ``gtin``, ``unid``, ``compras``, ``qtd_compras``, ``vendas``,
        ``qtd_vendas``, ``fonte``; or ``None`` if the file is absent or empty.
    """
    if path is None or not path.exists():
        return None

    # OtimizaÃ§Ã£o Bolt: pl.read_parquet_schema le a metadata sem alocar o DataFrame na memoria
    schema = pl.read_parquet_schema(path)

    def _pick(*candidatas: str) -> str | None:
        """Returns the first candidate column name that exists in the schema."""
        for coluna in candidatas:
            if coluna in schema:
                return coluna
        return None

    col_codigo = _pick("codigo_produto", "codigo_produto_original", "cod_item")
    col_desc = _pick("descricao_produto", "descr_item", "descricao")
    col_descr_compl = _pick("obs_complementar", "descr_compl")
    col_tipo = _pick("tipo_item")
    col_ncm = _pick("cod_ncm", "ncm")
    col_cest = _pick("cest")
    col_gtin = _pick("cod_barra", "gtin")
    col_unid = _pick("unidade_medida", "unidade_media", "unidade", "unid")

    if col_codigo is None or col_desc is None:
        return None

    selecionar = [
        c
        for c in [
            col_codigo,
            col_desc,
            col_descr_compl,
            col_tipo,
            col_ncm,
            col_cest,
            col_gtin,
            col_unid,
        ]
        if c is not None
    ]
    if "codigo_fonte" in schema:
        selecionar.append("codigo_fonte")
    df = pl.scan_parquet(path).select(selecionar).collect()
    if df.is_empty():
        return None

    return df.with_columns(
        [
            _normalizar_texto(col_codigo).alias("codigo"),
            _normalizar_texto(col_desc).alias("descricao"),
            (
                _normalizar_texto(col_descr_compl).alias("descr_compl")
                if col_descr_compl
                else pl.lit(None, pl.String).alias("descr_compl")
            ),
            (
                _normalizar_texto(col_tipo).alias("tipo_item")
                if col_tipo
                else pl.lit(None, pl.String).alias("tipo_item")
            ),
            (
                _normalizar_texto(col_ncm).alias("ncm")
                if col_ncm
                else pl.lit(None, pl.String).alias("ncm")
            ),
            (
                _normalizar_texto(col_cest).alias("cest")
                if col_cest
                else pl.lit(None, pl.String).alias("cest")
            ),
            (
                expr_gerar_codigo_fonte(pl.lit(cnpj), pl.col(col_codigo), pl.col(col_desc)).alias(
                    "codigo_fonte"
                )
            ),
            (
                _normalizar_texto(col_gtin).alias("gtin")
                if col_gtin
                else pl.lit(None, pl.String).alias("gtin")
            ),
            (
                _normalizar_texto(col_unid).alias("unid")
                if col_unid
                else pl.lit(None, pl.String).alias("unid")
            ),
            pl.lit(0.0).alias("compras"),
            pl.lit(0.0).alias("qtd_compras"),
            pl.lit(0.0).alias("vendas"),
            pl.lit(0.0).alias("qtd_vendas"),
            pl.lit("bloco_h").alias("fonte"),
        ]
    ).select(
        [
            "codigo",
            "descricao",
            "descr_compl",
            "tipo_item",
            "ncm",
            "cest",
            "gtin",
            "unid",
            "codigo_fonte",
            "compras",
            "qtd_compras",
            "vendas",
            "qtd_vendas",
            "fonte",
        ]
    )


def _ler_nfe_ou_nfce(
    path: Path | None, cnpj: str, nome_fonte: str, cfop_mercantil: pl.DataFrame | None
) -> pl.DataFrame | None:
    """Reads an NFe or NFCe raw Parquet and returns item-level sales metadata.

    Filters to outbound operations (``tipo_operacao == '1'``) emitted by
    ``cnpj`` and optionally filters by mercantile CFOP codes.

    Args:
        path: Path to the NFe/NFCe Parquet file, or ``None``.
        cnpj: 14-digit CNPJ of the emitter used to identify sales rows.
        nome_fonte: Source label assigned to the ``fonte`` column
            (e.g. ``'nfe'`` or ``'nfce'``).
        cfop_mercantil: Reference DataFrame of mercantile CFOPs, or ``None``.

    Returns:
        DataFrame with columns ``codigo``, ``descricao``, ``ncm``, ``cest``,
        ``gtin``, ``unid``, ``compras``, ``qtd_compras``, ``vendas``,
        ``qtd_vendas``, ``fonte``; or ``None`` if the file is absent or empty.
    """
    if path is None or not path.exists():
        return None

    # OtimizaÃ§Ã£o Bolt: pl.read_parquet_schema le a metadata sem alocar o DataFrame na memoria
    schema = pl.read_parquet_schema(path)
    col_tp = next((c for c in ["tipo_operacao", "co_tp_nf", "tp_nf"] if c in schema), None)
    if "co_emitente" not in schema or col_tp is None:
        return None

    selecionar = [
        c
        for c in [
            "codigo_fonte",
            "co_emitente",
            col_tp,
            "prod_cprod",
            "prod_xprod",
            "prod_ncm",
            "prod_cest",
            "prod_ceantrib",
            "prod_cean",
            "prod_ucom",
            "co_cfop",
            "prod_vprod",
            "prod_vfrete",
            "prod_vseg",
            "prod_voutro",
            "prod_vdesc",
            "prod_qcom",
            "__tipo_digit",
            "__co_emitente_str__",
        ]
        if c in schema
    ]
    selecionar.extend(["__tipo_digit", "__co_emitente_str__"])
    selecionar = list(dict.fromkeys(selecionar))

    lf_base = pl.scan_parquet(path).with_columns(
        [
            pl.col(col_tp)
            .cast(pl.String, strict=False)
            .str.extract(r"(\d+)")
            .alias("__tipo_digit"),
            pl.col("co_emitente").cast(pl.String, strict=False).alias("__co_emitente_str__"),
        ]
    )

    lf_selected = lf_base.select(selecionar)

    if "co_cfop" in schema:
        lf_selected = lf_selected.with_columns(
            pl.col("co_cfop").cast(pl.String).str.strip_chars().alias("co_cfop")
        )
        if cfop_mercantil is not None:
            lf_selected = lf_selected.join(
                cfop_mercantil.lazy().with_columns(pl.lit(True).alias("__cfop_mercantil__")),
                on="co_cfop",
                how="left",
            ).with_columns(pl.col("__cfop_mercantil__").fill_null(False))
        else:
            lf_selected = lf_selected.with_columns(pl.lit(True).alias("__cfop_mercantil__"))
    else:
        lf_selected = lf_selected.with_columns(pl.lit(True).alias("__cfop_mercantil__"))

    df = lf_selected.collect()

    if df.is_empty():
        return None

    gtin_expr = (
        pl.coalesce(
            [
                pl.col("prod_ceantrib").cast(pl.String, strict=False),
                pl.col("prod_cean").cast(pl.String, strict=False),
            ]
        )
        if "prod_ceantrib" in df.columns or "prod_cean" in df.columns
        else pl.lit(None, pl.String)
    )

    return df.with_columns(
        [
            (
                _normalizar_texto("prod_cprod").alias("codigo")
                if "prod_cprod" in df.columns
                else pl.lit(None, pl.String).alias("codigo")
            ),
            (
                _normalizar_texto("prod_xprod").alias("descricao")
                if "prod_xprod" in df.columns
                else pl.lit(None, pl.String).alias("descricao")
            ),
            (
                expr_gerar_codigo_fonte(
                    pl.lit(cnpj), pl.col("prod_cprod"), pl.col("prod_xprod")
                ).alias("codigo_fonte")
            ),
            pl.lit(None, pl.String).alias("descr_compl"),
            pl.lit(None, pl.String).alias("tipo_item"),
            (
                _normalizar_texto("prod_ncm").alias("ncm")
                if "prod_ncm" in df.columns
                else pl.lit(None, pl.String).alias("ncm")
            ),
            (
                _normalizar_texto("prod_cest").alias("cest")
                if "prod_cest" in df.columns
                else pl.lit(None, pl.String).alias("cest")
            ),
            gtin_expr.alias("gtin"),
            (
                _normalizar_texto("prod_ucom").alias("unid")
                if "prod_ucom" in df.columns
                else pl.lit(None, pl.String).alias("unid")
            ),
            pl.lit(0.0).alias("compras"),
            pl.lit(0.0).alias("qtd_compras"),
            pl.when(
                (pl.col("__co_emitente_str__") == cnpj)
                & (pl.col("__tipo_digit") == "1")
                & (
                    pl.col("__cfop_mercantil__")
                    if "__cfop_mercantil__" in df.columns
                    else pl.lit(True)
                )
            )
            .then(
                _num_expr("prod_vprod")
                + _num_expr("prod_vfrete")
                + _num_expr("prod_vseg")
                + _num_expr("prod_voutro")
                - _num_expr("prod_vdesc")
            )
            .otherwise(0.0)
            .alias("vendas"),
            pl.when(
                (pl.col("__co_emitente_str__") == cnpj)
                & (pl.col("__tipo_digit") == "1")
                & (
                    pl.col("__cfop_mercantil__")
                    if "__cfop_mercantil__" in df.columns
                    else pl.lit(True)
                )
            )
            .then(_num_expr("prod_qcom"))
            .otherwise(0.0)
            .alias("qtd_vendas"),
            pl.lit(nome_fonte).alias("fonte"),
        ]
    ).select(
        [
            "codigo",
            "descricao",
            "descr_compl",
            "tipo_item",
            "ncm",
            "cest",
            "gtin",
            "unid",
            "codigo_fonte",
            "compras",
            "qtd_compras",
            "vendas",
            "qtd_vendas",
            "fonte",
        ]
    )


def item_unidades(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    """Generates the ``item_unidades`` base table for the given CNPJ.

    Reads C170, Bloco H, NFe, and NFCe Parquet sources, normalizes item
    metadata, aggregates by ``(codigo, descricao, ncm, cest, gtin, unid)``,
    infers ``co_sefin_item``, and writes
    ``analises/produtos/item_unidades_{cnpj}.parquet``.

    Args:
        cnpj: CPF or CNPJ string (digits only or formatted; 11 or 14 digits).
        pasta_cnpj: Root directory for this CNPJ's data.  Defaults to
            ``CNPJ_ROOT / cnpj``.

    Returns:
        ``True`` on success, ``False`` if no eligible source was found or if
        the Parquet could not be saved.

    Raises:
        ValueError: If ``cnpj`` does not have 11 or 14 digits after stripping
            non-digits.
    """
    cnpj = re.sub(r"\D", "", cnpj or "")
    if len(cnpj) not in {11, 14}:
        raise ValueError("CPF/CNPJ invalido.")

    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj

    pasta_brutos = pasta_cnpj / "arquivos_parquet"
    if not pasta_brutos.exists():
        rprint(f"[red]Pasta de parquets nao encontrada: {pasta_brutos}[/red]")
        return False

    rprint(f"[bold cyan]Gerando item_unidades para CNPJ: {cnpj}[/bold cyan]")

    cfop_mercantil = _carregar_cfops_mercantis()

    fragmentos: list[pl.DataFrame] = []
    leitores = [
        _ler_c170(_resolver_arquivo_base(pasta_cnpj, "c170", cnpj), cnpj, cfop_mercantil),
        _ler_bloco_h(_resolver_arquivo_base(pasta_cnpj, "bloco_h", cnpj), cnpj),
        _ler_nfe_ou_nfce(
            _resolver_arquivo_base(pasta_cnpj, "nfe", cnpj), cnpj, "nfe", cfop_mercantil
        ),
        _ler_nfe_ou_nfce(
            _resolver_arquivo_base(pasta_cnpj, "nfce", cnpj),
            cnpj,
            "nfce",
            cfop_mercantil,
        ),
    ]

    fragmentos.extend(df for df in leitores if df is not None and not df.is_empty())

    if not fragmentos:
        rprint("[red]Nenhuma fonte elegivel foi encontrada para item_unidades.[/red]")
        return False

    df_total = pl.concat(fragmentos, how="diagonal_relaxed")
    df_total = _garantir_colunas(
        df_total,
        [
            "codigo",
            "descricao",
            "descr_compl",
            "tipo_item",
            "ncm",
            "cest",
            "gtin",
            "unid",
            "codigo_fonte",
        ],
    )

    chaves_agrupamento = [
        "codigo",
        "descricao",
        "descr_compl",
        "tipo_item",
        "ncm",
        "cest",
        "gtin",
        "unid",
    ]

    df_grouped = (
        df_total.group_by(chaves_agrupamento)
        .agg(
            [
                pl.col("compras").fill_null(0).sum().alias("compras"),
                pl.col("qtd_compras").fill_null(0).sum().alias("qtd_compras"),
                pl.col("vendas").fill_null(0).sum().alias("vendas"),
                pl.col("qtd_vendas").fill_null(0).sum().alias("qtd_vendas"),
                _agregar_lista_str("codigo_fonte", "lista_codigo_fonte"),
                pl.col("fonte").drop_nulls().unique().sort().alias("fontes"),
            ]
        )
        .sort(["descricao", "codigo", "unid"], nulls_last=True)
        .with_row_index("seq", offset=1)
        .with_columns(pl.format("id_item_unid_{}", pl.col("seq")).alias("id_item_unid"))
        .drop("seq")
    )

    df_grouped = _inferir_co_sefin(df_grouped)
    df_grouped = df_grouped.select(
        [
            "id_item_unid",
            "codigo",
            "descricao",
            "descr_compl",
            "tipo_item",
            "ncm",
            "cest",
            "co_sefin_item",
            "gtin",
            "unid",
            "compras",
            "qtd_compras",
            "vendas",
            "qtd_vendas",
            "lista_codigo_fonte",
            "fontes",
        ]
    )

    pasta_saida = pasta_cnpj / "analises" / "produtos"
    return salvar_para_parquet(df_grouped, pasta_saida, f"item_unidades_{cnpj}.parquet")


def gerar_item_unidades(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    """Entry-point alias for :func:`item_unidades`.

    Args:
        cnpj: CPF or CNPJ string (11 or 14 digits).
        pasta_cnpj: Override for the CNPJ root data directory.

    Returns:
        ``True`` on success, ``False`` otherwise.
    """
    return item_unidades(cnpj, pasta_cnpj)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        item_unidades(sys.argv[1])
    else:
        item_unidades(input("CNPJ: "))
