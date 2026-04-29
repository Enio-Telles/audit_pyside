import polars as pl

from interface_grafica.services.similaridade_multiatributo_service import (
    ordenar_blocos_similaridade_multiatributo,
)


def test_mesmo_codigo_fiscal_reforca_score_com_descricoes_diferentes():
    df = pl.DataFrame(
        {
            "id_agrupado": ["1", "2", "3"],
            "descr_padrao": [
                "REFRIG COCA COLA 2L",
                "COCA-COLA PET 2 LITROS",
                "ARROZ TIPO 1 5KG",
            ],
            "Cod_item": ["12345", "12345", "99999"],
            "ncm_padrao": ["22021000", "22021090", "10063021"],
            "cest_padrao": ["0300700", "0300700", ""],
            "gtin_padrao": ["", "", ""],
        }
    )

    out = ordenar_blocos_similaridade_multiatributo(df)
    coca = out.filter(pl.col("id_agrupado").is_in(["1", "2"]))

    assert "sim_score_codigo_fiscal" in out.columns
    assert "sim_score_total" in out.columns
    assert coca["sim_score_codigo_fiscal"].drop_nulls().max() == 100
    assert coca["sim_motivos"].str.contains("CODIGO_FISCAL").any()
    assert coca["sim_score_total"].min() >= coca["sim_score"].min()


def test_codigo_fiscal_fraco_nao_reforca_score():
    df = pl.DataFrame(
        {
            "id_agrupado": ["1", "2"],
            "descr_padrao": ["PRODUTO ALFA", "PRODUTO BETA"],
            "prod_cprod": ["001", "001"],
            "ncm_padrao": ["", ""],
            "cest_padrao": ["", ""],
            "gtin_padrao": ["", ""],
        }
    )

    out = ordenar_blocos_similaridade_multiatributo(df)

    assert out["sim_score_codigo_fiscal"].drop_nulls().is_empty()
    assert not out["sim_motivos"].str.contains("CODIGO_FISCAL_IGUAL").any()


def test_prod_cprod_e_cod_item_sao_tratados_como_codigo_fiscal():
    df = pl.DataFrame(
        {
            "id_agrupado": ["1", "2"],
            "descr_padrao": ["SABONETE PERFUMADO", "SABONETE 90G"],
            "prod_cprod": ["ABC-123", "ABC-123"],
            "ncm_padrao": ["34011190", "34011190"],
            "cest_padrao": ["2005400", "2005400"],
            "gtin_padrao": ["", ""],
        }
    )

    out = ordenar_blocos_similaridade_multiatributo(df)

    assert out["sim_codigo_fiscal_norm"].to_list() == ["ABC-123", "ABC-123"]
    assert out["sim_score_codigo_fiscal"].drop_nulls().to_list() == [100, 100]
