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


def carregar_c100_bruto(cnpj: str) -> pl.LazyFrame:
    return carregar_parquet_atomizado(cnpj, "c100")


def carregar_c170_bruto(cnpj: str) -> pl.LazyFrame:
    return carregar_parquet_atomizado(cnpj, "c170")


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
        c100
        .with_columns(
            pl.col("dt_doc_raw").str.strptime(pl.Date, "%d%m%Y", strict=False).alias("dt_doc"),
            pl.col("dt_e_s_raw").str.strptime(pl.Date, "%d%m%Y", strict=False).alias("dt_e_s"),
            pl.col("periodo_efd_dt").cast(pl.Date, strict=False),
            (
                pl.col("dt_doc_raw").is_not_null()
                & pl.col("dt_doc_raw").str.strptime(pl.Date, "%d%m%Y", strict=False).is_null()
            ).alias("flag_dt_doc_invalida"),
        )
        .join(cod_sit_ref, on="cod_sit", how="left")
        .join(ind_emit_ref, on="ind_emit", how="left")
        .join(ind_oper_ref, on="ind_oper", how="left")
    )


def salvar_c100_tipado(cnpj: str) -> Path:
    """Materializa o C100 tipado em `analises/atomizadas`, preservando a camada bruta separada."""

    cnpj_limpo = re.sub(r"\D", "", cnpj)
    pasta_saida = _cnpj_root(cnpj) / "analises" / "atomizadas"
    pasta_saida.mkdir(parents=True, exist_ok=True)
    caminho_saida = pasta_saida / f"c100_tipado_{cnpj_limpo}.parquet"
    construir_c100_tipado(cnpj).collect().write_parquet(caminho_saida, compression="snappy")
    return caminho_saida
