from __future__ import annotations

import re
from pathlib import Path

import polars as pl

ROOT_DIR = Path(__file__).resolve().parents[3]
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"
REF_PATH = DADOS_DIR / "referencias" / "conditional_descriptions_reference.parquet"


def _cnpj_root(cnpj: str) -> Path:
    cnpj_limpo = re.sub(r"\D", "", cnpj)
    return CNPJ_ROOT / cnpj_limpo


def _base_atomizada(cnpj: str) -> Path:
    return _cnpj_root(cnpj) / "arquivos_parquet" / "atomizadas"


def carregar_referencia_condicional() -> pl.LazyFrame:
    """Carrega a referencia de descricoes condicionais usada para enriquecer campos codificados."""

    return pl.scan_parquet(str(REF_PATH))


def carregar_parquet_atomizado(cnpj: str, dominio: str) -> pl.LazyFrame:
    """Carrega uma familia atomizada a partir da pasta padrao do CNPJ."""

    return pl.scan_parquet(str(_base_atomizada(cnpj) / dominio / "*.parquet"))


def carregar_parquet_atomizado_por_padrao(cnpj: str, dominio: str, padrao: str) -> pl.LazyFrame:
    """Carrega um subconjunto atomizado por padrao de nome de arquivo."""

    return pl.scan_parquet(str(_base_atomizada(cnpj) / dominio / padrao))


def carregar_c100_bruto(cnpj: str) -> pl.LazyFrame:
    return carregar_parquet_atomizado(cnpj, "c100")


def carregar_c170_bruto(cnpj: str) -> pl.LazyFrame:
    return carregar_parquet_atomizado(cnpj, "c170")


def carregar_c176_bruto(cnpj: str) -> pl.LazyFrame:
    return carregar_parquet_atomizado(cnpj, "c176")


def carregar_h005_bruto(cnpj: str) -> pl.LazyFrame:
    return carregar_parquet_atomizado_por_padrao(cnpj, "bloco_h", "40_*.parquet")


def carregar_h010_bruto(cnpj: str) -> pl.LazyFrame:
    return carregar_parquet_atomizado_por_padrao(cnpj, "bloco_h", "41_*.parquet")


def carregar_h020_bruto(cnpj: str) -> pl.LazyFrame:
    return carregar_parquet_atomizado_por_padrao(cnpj, "bloco_h", "42_*.parquet")


def carregar_reg0200_bruto(cnpj: str) -> pl.LazyFrame:
    return carregar_parquet_atomizado_por_padrao(cnpj, "dimensions", "50_*.parquet")


def _mapa_referencia(
    referencia: pl.LazyFrame,
    campo_origem: str,
    alias_chave: str,
    alias_descricao: str,
) -> pl.LazyFrame:
    return (
        referencia.filter(
            (pl.col("source_field") == campo_origem)
            & (pl.col("branch_kind") == "WHEN")
        )
        .select(
            pl.col("match_value").alias(alias_chave),
            pl.col("description").alias(alias_descricao),
        )
    )


def _adicionar_tipagem_periodo_efd(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Padroniza os campos de periodo e entrega presentes nos atomizados."""

    expressoes: list[pl.Expr] = []
    if "dt_ini" in lf.collect_schema().names():
        expressoes.append(pl.col("dt_ini").cast(pl.Date, strict=False).alias("periodo_efd_dt"))
    if "dt_fin" in lf.collect_schema().names():
        expressoes.append(pl.col("dt_fin").cast(pl.Date, strict=False).alias("periodo_efd_fim"))
    if "data_entrega_efd_periodo" in lf.collect_schema().names():
        expressoes.append(pl.col("data_entrega_efd_periodo").cast(pl.Date, strict=False))
    return lf.with_columns(expressoes) if expressoes else lf


def _gerar_codigo_fonte(expr_cnpj: pl.Expr, expr_item: pl.Expr) -> pl.Expr:
    return pl.concat_str([expr_cnpj.cast(pl.Utf8, strict=False), pl.lit("|"), expr_item.cast(pl.Utf8, strict=False)]).alias("codigo_fonte")


def construir_reg0200_tipado(cnpj: str) -> pl.LazyFrame:
    """Tipa a dimensao de produtos 0200 e cria a chave tecnica por CNPJ."""

    return (
        _adicionar_tipagem_periodo_efd(carregar_reg0200_bruto(cnpj))
        .with_columns(
            pl.col("cod_item").cast(pl.Utf8, strict=False),
            pl.col("descr_item").cast(pl.Utf8, strict=False),
            pl.col("tipo_item").cast(pl.Utf8, strict=False),
            pl.col("cod_ncm").cast(pl.Utf8, strict=False),
            pl.col("cest").cast(pl.Utf8, strict=False),
            pl.col("cod_barra").cast(pl.Utf8, strict=False),
            _gerar_codigo_fonte(pl.col("cnpj"), pl.col("cod_item")),
        )
    )


def construir_c100_tipado(cnpj: str) -> pl.LazyFrame:
    """
    Recompõe o C100 bruto com tipagem lazy em Polars.

    A ideia segue a abordagem da referencia atomizada: manter a extracao SQL o mais
    simples possivel e deslocar tipagem/enriquecimento para fora do banco.
    """

    c100 = carregar_c100_bruto(cnpj)
    referencia = carregar_referencia_condicional()

    cod_sit_ref = _mapa_referencia(referencia, "c100.cod_sit", "cod_sit", "cod_sit_desc")
    ind_emit_ref = _mapa_referencia(referencia, "c100.ind_emit", "ind_emit", "ind_emit_desc")
    ind_oper_ref = _mapa_referencia(referencia, "c100.ind_oper", "ind_oper", "ind_oper_desc")

    return (
        _adicionar_tipagem_periodo_efd(c100)
        .with_columns(
            pl.col("dt_doc_raw").str.strptime(pl.Date, "%d%m%Y", strict=False).alias("dt_doc"),
            pl.col("dt_e_s_raw").str.strptime(pl.Date, "%d%m%Y", strict=False).alias("dt_e_s"),
            (
                pl.col("dt_doc_raw").is_not_null()
                & pl.col("dt_doc_raw").str.strptime(pl.Date, "%d%m%Y", strict=False).is_null()
            ).alias("flag_dt_doc_invalida"),
        )
        .join(cod_sit_ref, on="cod_sit", how="left")
        .join(ind_emit_ref, on="ind_emit", how="left")
        .join(ind_oper_ref, on="ind_oper", how="left")
    )


def construir_c170_tipado(cnpj: str) -> pl.LazyFrame:
    """
    Tipa o C170 bruto e enriquece com a dimensao 0200.

    Mantem a SQL bruta minimalista e desloca para Polars os casts numericos,
    a geracao de chaves e o enriquecimento por cadastro de item.
    """

    c170 = _adicionar_tipagem_periodo_efd(carregar_c170_bruto(cnpj))
    itens_0200 = construir_reg0200_tipado(cnpj).select(
        "reg_0000_id",
        "cod_item",
        "descr_item",
        "tipo_item",
        "cod_ncm",
        "cest",
        "cod_barra",
        "codigo_fonte",
    )

    return (
        c170
        .with_columns(
            pl.col("num_item").cast(pl.Int64, strict=False),
            pl.col("cod_item").cast(pl.Utf8, strict=False),
            pl.col("descr_compl").cast(pl.Utf8, strict=False),
            pl.col("cfop").cast(pl.Utf8, strict=False),
            pl.col("cst_icms").cast(pl.Utf8, strict=False),
            pl.col("unid").cast(pl.Utf8, strict=False),
            pl.col("qtd").cast(pl.Float64, strict=False),
            pl.col("vl_item").cast(pl.Float64, strict=False),
            pl.col("vl_desc").cast(pl.Float64, strict=False),
            pl.col("vl_icms").cast(pl.Float64, strict=False),
            pl.col("vl_bc_icms").cast(pl.Float64, strict=False),
            pl.col("aliq_icms").cast(pl.Float64, strict=False),
            pl.col("vl_bc_icms_st").cast(pl.Float64, strict=False),
            pl.col("vl_icms_st").cast(pl.Float64, strict=False),
            pl.col("aliq_st").cast(pl.Float64, strict=False),
        )
        .join(itens_0200, on=["reg_0000_id", "cod_item"], how="left")
        .with_columns(
            pl.coalesce([pl.col("codigo_fonte"), _gerar_codigo_fonte(pl.col("cnpj"), pl.col("cod_item"))])
            .alias("codigo_fonte")
        )
    )


def construir_c176_tipado(cnpj: str) -> pl.LazyFrame:
    """Tipa o C176 bruto e o reconcilia com C100/C170 para facilitar auditoria."""

    c176 = _adicionar_tipagem_periodo_efd(carregar_c176_bruto(cnpj)).with_columns(
        pl.col("cod_mot_res").cast(pl.Utf8, strict=False),
        pl.col("chave_nfe_ult").cast(pl.Utf8, strict=False),
        pl.col("num_item_ult_e").cast(pl.Int64, strict=False),
        pl.col("dt_ult_e_raw").str.strptime(pl.Date, "%d%m%Y", strict=False).alias("dt_ult_e"),
        pl.col("vl_unit_ult_e").cast(pl.Float64, strict=False),
        pl.col("vl_unit_icms_ult_e").cast(pl.Float64, strict=False),
        pl.col("vl_unit_res").cast(pl.Float64, strict=False),
    )

    c170 = construir_c170_tipado(cnpj).select(
        pl.col("reg_c170_id"),
        pl.col("reg_c100_id").alias("reg_c100_id_c170"),
        "num_item",
        "cod_item",
        "descr_item",
        "descr_compl",
        "cfop",
        "unid",
        "qtd",
        "vl_item",
        "codigo_fonte",
    )
    c100 = construir_c100_tipado(cnpj).select("reg_c100_id", "chv_nfe", "num_doc", "ser", "dt_doc", "dt_e_s")

    return (
        c176
        .join(c170, left_on="reg_c170_id", right_on="reg_c170_id", how="left")
        .join(c100, left_on="reg_c100_id", right_on="reg_c100_id", how="left")
    )


def construir_h005_tipado(cnpj: str) -> pl.LazyFrame:
    """Tipa o cabecalho do inventario H005."""

    return (
        _adicionar_tipagem_periodo_efd(carregar_h005_bruto(cnpj))
        .with_columns(
            pl.col("dt_inv_raw").str.strptime(pl.Date, "%d%m%Y", strict=False).alias("dt_inv"),
            pl.col("vl_inv_raw").cast(pl.Float64, strict=False).alias("vl_inv"),
            pl.col("mot_inv").cast(pl.Utf8, strict=False),
        )
    )


def construir_h010_tipado(cnpj: str) -> pl.LazyFrame:
    """Tipa os itens do inventario H010 e enriquece com a dimensao 0200."""

    itens_0200 = construir_reg0200_tipado(cnpj).select(
        "reg_0000_id",
        "cod_item",
        "descr_item",
        "tipo_item",
        "cod_ncm",
        "cest",
        "cod_barra",
        "codigo_fonte",
    )

    return (
        _adicionar_tipagem_periodo_efd(carregar_h010_bruto(cnpj))
        .with_columns(
            pl.col("cod_item").cast(pl.Utf8, strict=False),
            pl.col("unid").cast(pl.Utf8, strict=False),
            pl.col("qtd").cast(pl.Float64, strict=False),
            pl.col("vl_unit").cast(pl.Float64, strict=False),
            pl.col("vl_item").cast(pl.Float64, strict=False),
            pl.col("ind_prop").cast(pl.Utf8, strict=False),
            pl.col("cod_part").cast(pl.Utf8, strict=False),
            pl.col("txt_compl").cast(pl.Utf8, strict=False),
        )
        .join(itens_0200, on=["reg_0000_id", "cod_item"], how="left")
        .with_columns(
            pl.coalesce([pl.col("codigo_fonte"), _gerar_codigo_fonte(pl.col("cnpj"), pl.col("cod_item"))])
            .alias("codigo_fonte")
        )
    )


def construir_h020_tipado(cnpj: str) -> pl.LazyFrame:
    """Tipa a tributacao do inventario H020."""

    return (
        _adicionar_tipagem_periodo_efd(carregar_h020_bruto(cnpj))
        .with_columns(
            pl.col("cst_icms").cast(pl.Utf8, strict=False),
            pl.col("bc_icms").cast(pl.Float64, strict=False),
            pl.col("vl_icms").cast(pl.Float64, strict=False),
        )
    )


def construir_bloco_h_tipado(cnpj: str) -> pl.LazyFrame:
    """Consolida H005/H010/H020 em uma visao lazy unica do inventario."""

    h005 = construir_h005_tipado(cnpj).select(
        "reg_h005_id",
        "reg_0000_id",
        "periodo_efd_dt",
        "periodo_efd_fim",
        "data_entrega_efd_periodo",
        "dt_inv",
        "vl_inv",
        "mot_inv",
    )
    h010 = construir_h010_tipado(cnpj)
    h020 = construir_h020_tipado(cnpj).select("reg_h010_id", "cst_icms", "bc_icms", "vl_icms")

    return (
        h010
        .join(h005, on=["reg_h005_id", "reg_0000_id"], how="left")
        .join(h020, on="reg_h010_id", how="left")
    )


def _salvar_lazyframe_atomizado(cnpj: str, nome_base: str, lf: pl.LazyFrame) -> Path:
    cnpj_limpo = re.sub(r"\D", "", cnpj)
    pasta_saida = _cnpj_root(cnpj) / "analises" / "atomizadas"
    pasta_saida.mkdir(parents=True, exist_ok=True)
    caminho_saida = pasta_saida / f"{nome_base}_{cnpj_limpo}.parquet"
    lf.collect().write_parquet(caminho_saida, compression="snappy")
    return caminho_saida


def salvar_c100_tipado(cnpj: str) -> Path:
    """Materializa o C100 tipado em `analises/atomizadas`, preservando a camada bruta separada."""

    return _salvar_lazyframe_atomizado(cnpj, "c100_tipado", construir_c100_tipado(cnpj))


def salvar_reg0200_tipado(cnpj: str) -> Path:
    return _salvar_lazyframe_atomizado(cnpj, "reg0200_tipado", construir_reg0200_tipado(cnpj))


def salvar_c170_tipado(cnpj: str) -> Path:
    return _salvar_lazyframe_atomizado(cnpj, "c170_tipado", construir_c170_tipado(cnpj))


def salvar_c176_tipado(cnpj: str) -> Path:
    return _salvar_lazyframe_atomizado(cnpj, "c176_tipado", construir_c176_tipado(cnpj))


def salvar_h005_tipado(cnpj: str) -> Path:
    return _salvar_lazyframe_atomizado(cnpj, "h005_tipado", construir_h005_tipado(cnpj))


def salvar_h010_tipado(cnpj: str) -> Path:
    return _salvar_lazyframe_atomizado(cnpj, "h010_tipado", construir_h010_tipado(cnpj))


def salvar_h020_tipado(cnpj: str) -> Path:
    return _salvar_lazyframe_atomizado(cnpj, "h020_tipado", construir_h020_tipado(cnpj))


def salvar_bloco_h_tipado(cnpj: str) -> Path:
    return _salvar_lazyframe_atomizado(cnpj, "bloco_h_tipado", construir_bloco_h_tipado(cnpj))


def materializar_camadas_atomizadas(cnpj: str) -> list[Path]:
    """
    Materializa o conjunto principal da camada atomizada.

    A ordem preserva dependencias logicas: primeiro dimensao e cabecalho,
    depois itens e visoes que dependem de joins.
    """

    return [
        salvar_reg0200_tipado(cnpj),
        salvar_c100_tipado(cnpj),
        salvar_c170_tipado(cnpj),
        salvar_c176_tipado(cnpj),
        salvar_h005_tipado(cnpj),
        salvar_h010_tipado(cnpj),
        salvar_h020_tipado(cnpj),
        salvar_bloco_h_tipado(cnpj),
    ]
