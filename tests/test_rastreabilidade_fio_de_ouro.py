from pathlib import Path
import sys

import polars as pl

sys.path.insert(0, str(Path("src").resolve()))

from transformacao.rastreabilidade_produtos.fontes_produtos import (
    _preservar_colunas_rastreabilidade,
    _anexar_id_agrupado_por_codigo_ou_descricao,
)


def _df_mapa_base() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "codigo_fonte": ["111|001", "111|002"],
            "id_agrupado": ["AGR_1", "AGR_2"],
            "descricao_normalizada": ["PRODUTO A", "PRODUTO B"],
        }
    )


def _df_attrs_base() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id_agrupado": ["AGR_1", "AGR_2"],
            "descr_padrao": ["Produto A", "Produto B"],
            "ncm_padrao": ["22083000", "22083000"],
            "cest_padrao": ["0300700", "0300700"],
            "co_sefin_padrao": ["123", "123"],
            "versao_agrupamento": [1, 1],
        }
    )


def test_id_linha_origem_preservado_quando_presente_na_fonte():
    df_src = pl.DataFrame(
        {
            "prod_xprod": ["Produto A"],
            "codigo_fonte": ["111|001"],
            "id_linha_origem": ["NFe_001|1"],
        }
    )

    exprs = _preservar_colunas_rastreabilidade(df_src)
    df_result = df_src.with_columns(exprs)

    assert "id_linha_origem" in df_result.columns
    assert df_result["id_linha_origem"].to_list() == ["NFe_001|1"]


def test_pipeline_nao_falha_quando_id_linha_origem_ausente():
    df_src = pl.DataFrame(
        {
            "prod_xprod": ["Produto A"],
            "codigo_fonte": ["111|001"],
        }
    )

    exprs = _preservar_colunas_rastreabilidade(df_src)
    df_result = df_src.with_columns(exprs)

    assert "id_linha_origem" not in df_result.columns


def test_codigo_fonte_colisao_logada(tmp_path: Path):
    cnpj = "77777777000100"
    pasta_analises = tmp_path / "analises"
    pasta_analises.mkdir(parents=True, exist_ok=True)

    df_mapa_colisao = pl.DataFrame(
        {
            "codigo_fonte": ["111|001", "111|001"],
            "id_agrupado": ["AGR_A", "AGR_B"],
            "descricao_normalizada": ["PRODUTO A", "PRODUTO A"],
        }
    )

    df_src = pl.DataFrame(
        {
            "prod_xprod": ["Produto A"],
            "codigo_fonte": ["111|001"],
        }
    )

    _anexar_id_agrupado_por_codigo_ou_descricao(
        df_src=df_src,
        df_mapa=df_mapa_colisao,
        df_attrs=_df_attrs_base(),
        col_desc="prod_xprod",
        pasta_analises=pasta_analises,
        cnpj=cnpj,
    )

    log_path = pasta_analises / f"audit_codigo_fonte_colisao_{cnpj}.parquet"
    assert log_path.exists()
    df_log = pl.read_parquet(log_path)
    assert df_log["codigo_fonte"].to_list() == ["111|001"]


def test_vinculo_por_codigo_fonte_tem_prioridade_sobre_descricao():
    df_src = pl.DataFrame(
        {
            "prod_xprod": ["Produto A"],
            "codigo_fonte": ["111|001"],
        }
    )

    df_result = _anexar_id_agrupado_por_codigo_ou_descricao(
        df_src=df_src,
        df_mapa=_df_mapa_base(),
        df_attrs=_df_attrs_base(),
        col_desc="prod_xprod",
    )

    assert df_result["id_agrupado"].to_list() == ["AGR_1"]
    assert df_result["origem_vinculo_agrupamento"].to_list() == ["codigo_fonte"]
