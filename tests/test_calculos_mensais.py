from datetime import date
from pathlib import Path
import sys

import polars as pl
import pytest

sys.path.insert(0, str(Path("src").resolve()))

from transformacao.calculos_mensais import calcular_aba_mensal_dataframe


def test_calcular_aba_mensal_com_devolucao_excluida_das_medias():
    df = pl.DataFrame(
        {
            "id_agrupado": ["id_1", "id_1", "id_1", "id_1"],
            "co_sefin_agr": ["1001", "1001", "1001", "1001"],
            "descr_padrao": ["Produto X"] * 4,
            "Tipo_operacao": ["1 - ENTRADA", "1 - ENTRADA", "2 - SAIDAS", "2 - SAIDAS"],
            "Dt_doc": [date(2024, 1, 2), date(2024, 1, 5), date(2024, 1, 10), date(2024, 1, 20)],
            "Dt_e_s": [date(2024, 1, 2), date(2024, 1, 5), date(2024, 1, 10), date(2024, 1, 20)],
            "ordem_operacoes": [1, 2, 3, 4],
            "Unid": ["CX", "CX", "CX", "CX"],
            "unid_ref": ["UN", "UN", "UN", "UN"],
            "q_conv": [10.0, 2.0, 4.0, 0.0],
            "preco_item": [100.0, 30.0, 80.0, 0.0],
            "entr_desac_anual": [0.0, 0.0, 0.0, 3.0],
            "saldo_estoque_anual": [10.0, 12.0, 8.0, 8.0],
            "custo_medio_anual": [10.0, 10.8333, 10.8333, 10.8333],
            "Aliq_icms": [18.0, 18.0, 18.0, 18.0],
            "it_pc_interna": [18.0, 18.0, 18.0, 18.0],
            "it_pc_mva": [40.0, 40.0, 40.0, 40.0],
            "it_in_st": ["S", "S", "S", "S"],
            "it_in_mva_ajustado": ["N", "N", "N", "N"],
            "finnfe": ["1", "4", "1", "1"],
            "dev_simples": [False, True, False, False],
            "dev_venda": [False, False, False, False],
            "dev_compra": [False, False, False, False],
            "dev_ent_simples": [False, False, False, False],
            "excluir_estoque": [False, False, False, False],
        }
    )

    df_aux_st = pl.DataFrame(
        {
            "it_co_sefin": ["1001"],
            "it_da_inicio": ["20240101"],
            "it_da_final": ["20241231"],
            "it_in_st": ["S"],
        }
    )

    result = calcular_aba_mensal_dataframe(df, df_aux_st=df_aux_st)

    assert result.height == 1
    row = result.row(0, named=True)
    assert row["ano"] == 2024
    assert row["mes"] == 1
    assert row["id_agregado"] == "id_1"
    assert row["unids_mes"] == ["CX"]
    assert row["unids_ref_mes"] == ["UN"]
    assert row["ST"] == "['S' de 01/01/2024 ate 31/01/2024]"
    assert row["valor_entradas"] == 130.0
    assert row["qtd_entradas"] == 12.0
    assert row["pme_mes"] == 10.0
    assert row["valor_saidas"] == 80.0
    assert row["qtd_saidas"] == 4.0
    assert row["pms_mes"] == 20.0
    assert row["MVA"] == 40.0
    assert row["MVA_ajustado"] is None
    assert row["entradas_desacob"] == 3.0
    assert row["saldo_mes"] == 8.0
    assert row["custo_medio_mes"] == pytest.approx(10.8333, rel=1e-4)
    assert row["valor_estoque"] == pytest.approx(86.67, rel=1e-3)
    assert row["ICMS_entr_desacob"] == pytest.approx(10.8, rel=1e-4)


def test_calcular_aba_mensal_icms_st_usa_fallback_pme_mva_quando_nao_ha_pms():
    df = pl.DataFrame(
        {
            "id_agrupado": ["id_2", "id_2", "id_2"],
            "co_sefin_agr": ["2002", "2002", "2002"],
            "descr_padrao": ["Produto Y"] * 3,
            "Tipo_operacao": ["1 - ENTRADA", "1 - ENTRADA", "1 - ENTRADA"],
            "Dt_doc": [date(2024, 2, 1), date(2024, 2, 2), date(2024, 2, 28)],
            "Dt_e_s": [date(2024, 2, 1), date(2024, 2, 2), date(2024, 2, 28)],
            "ordem_operacoes": [1, 2, 3],
            "Unid": ["UN", "UN", "UN"],
            "unid_ref": ["UN", "UN", "UN"],
            "q_conv": [10.0, 2.0, 0.0],
            "preco_item": [50.0, 40.0, 0.0],
            "entr_desac_anual": [0.0, 0.0, 2.0],
            "saldo_estoque_anual": [10.0, 10.0, 10.0],
            "custo_medio_anual": [5.0, 5.0, 5.0],
            "Aliq_icms": [12.0, 12.0, 12.0],
            "it_pc_interna": [18.0, 18.0, 18.0],
            "it_pc_mva": [40.0, 40.0, 40.0],
            "it_in_st": ["S", "S", "S"],
            "it_in_mva_ajustado": ["S", "S", "S"],
            "finnfe": ["1", "4", "1"],
            "dev_simples": [False, True, False],
            "dev_venda": [False, False, False],
            "dev_compra": [False, False, False],
            "dev_ent_simples": [False, False, False],
            "excluir_estoque": [False, False, True],
        }
    )

    df_aux_st = pl.DataFrame(
        {
            "it_co_sefin": ["2002"],
            "it_da_inicio": ["20240201"],
            "it_da_final": ["20241231"],
            "it_in_st": ["S"],
        }
    )

    result = calcular_aba_mensal_dataframe(df, df_aux_st=df_aux_st)

    assert result.height == 1
    row = result.row(0, named=True)
    assert row["valor_entradas"] == 90.0
    assert row["qtd_entradas"] == 12.0
    assert row["pme_mes"] == 5.0
    assert row["valor_saidas"] == 0.0
    assert row["qtd_saidas"] == 0.0
    assert row["pms_mes"] == 0.0
    assert row["MVA"] == 40.0
    assert row["MVA_ajustado"] == pytest.approx((((1 + 0.40) * (1 - 0.12)) / (1 - 0.18)) - 1, rel=1e-6)
    assert row["entradas_desacob"] == 2.0
    assert row["ICMS_entr_desacob"] == pytest.approx(0.9, rel=1e-4)


def test_calcular_aba_mensal_so_calcula_icms_quando_st_estiver_vigente_no_mes():
    df = pl.DataFrame(
        {
            "id_agrupado": ["id_3", "id_3"],
            "co_sefin_agr": ["3003", "3003"],
            "descr_padrao": ["Produto Z"] * 2,
            "Tipo_operacao": ["1 - ENTRADA", "1 - ENTRADA"],
            "Dt_doc": [date(2024, 3, 5), date(2024, 3, 31)],
            "Dt_e_s": [date(2024, 3, 5), date(2024, 3, 31)],
            "ordem_operacoes": [1, 2],
            "Unid": ["UN", "UN"],
            "unid_ref": ["UN", "UN"],
            "q_conv": [10.0, 0.0],
            "preco_item": [100.0, 0.0],
            "entr_desac_anual": [0.0, 2.0],
            "saldo_estoque_anual": [10.0, 10.0],
            "custo_medio_anual": [10.0, 10.0],
            "Aliq_icms": [12.0, 12.0],
            "it_pc_interna": [18.0, 18.0],
            "it_pc_mva": [40.0, 40.0],
            "it_in_st": ["S", "S"],
            "it_in_mva_ajustado": ["N", "N"],
            "finnfe": ["1", "1"],
            "dev_simples": [False, False],
            "dev_venda": [False, False],
            "dev_compra": [False, False],
            "dev_ent_simples": [False, False],
            "excluir_estoque": [False, False],
        }
    )

    df_aux_st = pl.DataFrame(
        {
            "it_co_sefin": ["3003"],
            "it_da_inicio": ["20240401"],
            "it_da_final": ["20241231"],
            "it_in_st": ["S"],
        }
    )

    result = calcular_aba_mensal_dataframe(df, df_aux_st=df_aux_st)

    assert result.height == 1
    row = result.row(0, named=True)
    assert row["ST"] == ""
    assert row["MVA"] is None
    assert row["MVA_ajustado"] is None
    assert row["ICMS_entr_desacob"] == 0.0


def test_calcular_aba_mensal_ignora_preco_de_linha_com_q_conv_zero_nas_medias():
    df = pl.DataFrame(
        {
            "id_agrupado": ["id_4", "id_4", "id_4"],
            "co_sefin_agr": ["4004", "4004", "4004"],
            "descr_padrao": ["Produto com linha neutralizada"] * 3,
            "Tipo_operacao": ["1 - ENTRADA", "2 - SAIDAS", "2 - SAIDAS"],
            "Dt_doc": [date(2024, 4, 1), date(2024, 4, 10), date(2024, 4, 11)],
            "Dt_e_s": [date(2024, 4, 1), date(2024, 4, 10), date(2024, 4, 11)],
            "ordem_operacoes": [1, 2, 3],
            "Unid": ["UN", "UN", "UN"],
            "unid_ref": ["UN", "UN", "UN"],
            "q_conv": [10.0, 4.0, 0.0],
            "preco_item": [100.0, 80.0, 900.0],
            "entr_desac_anual": [0.0, 0.0, 2.0],
            "saldo_estoque_anual": [10.0, 6.0, 6.0],
            "custo_medio_anual": [10.0, 10.0, 10.0],
            "Aliq_icms": [18.0, 18.0, 18.0],
            "it_pc_interna": [18.0, 18.0, 18.0],
            "it_pc_mva": [40.0, 40.0, 40.0],
            "it_in_st": ["S", "S", "S"],
            "it_in_mva_ajustado": ["N", "N", "N"],
            "finnfe": ["1", "1", "1"],
            "dev_simples": [False, False, False],
            "dev_venda": [False, False, False],
            "dev_compra": [False, False, False],
            "dev_ent_simples": [False, False, False],
            "excluir_estoque": [False, False, False],
        }
    )
    df_aux_st = pl.DataFrame(
        {
            "it_co_sefin": ["4004"],
            "it_da_inicio": ["20240401"],
            "it_da_final": ["20241231"],
            "it_in_st": ["S"],
        }
    )

    result = calcular_aba_mensal_dataframe(df, df_aux_st=df_aux_st)
    row = result.row(0, named=True)

    assert row["pme_mes"] == 10.0
    assert row["pms_mes"] == 20.0


def test_calcular_aba_mensal_usa_pms_arredondado_no_icms_entr_desacob():
    df = pl.DataFrame(
        {
            "id_agrupado": ["id_4b", "id_4b"],
            "co_sefin_agr": ["4005", "4005"],
            "descr_padrao": ["Produto com PMS fracionado"] * 2,
            "Tipo_operacao": ["2 - SAIDAS", "2 - SAIDAS"],
            "Dt_doc": [date(2024, 4, 10), date(2024, 4, 11)],
            "Dt_e_s": [date(2024, 4, 10), date(2024, 4, 11)],
            "ordem_operacoes": [1, 2],
            "Unid": ["UN", "UN"],
            "unid_ref": ["UN", "UN"],
            "q_conv": [1000.0, 0.0],
            "preco_item": [10234.0, 0.0],
            "entr_desac_anual": [0.0, 1000.0],
            "saldo_estoque_anual": [0.0, 0.0],
            "custo_medio_anual": [0.0, 0.0],
            "Aliq_icms": [4.0, 4.0],
            "it_pc_interna": [37.0, 37.0],
            "it_pc_mva": [53.63, 53.63],
            "it_in_st": ["S", "S"],
            "it_in_mva_ajustado": ["S", "S"],
            "finnfe": ["1", "1"],
            "dev_simples": [False, False],
            "dev_venda": [False, False],
            "dev_compra": [False, False],
            "dev_ent_simples": [False, False],
            "excluir_estoque": [False, False],
        }
    )
    df_aux_st = pl.DataFrame(
        {
            "it_co_sefin": ["4005"],
            "it_da_inicio": ["20240401"],
            "it_da_final": ["20241231"],
            "it_in_st": ["S"],
        }
    )

    result = calcular_aba_mensal_dataframe(df, df_aux_st=df_aux_st)
    row = result.row(0, named=True)

    assert row["pms_mes"] == pytest.approx(10.234, rel=1e-6)
    assert row["entradas_desacob"] == 1000.0
    assert row["ICMS_entr_desacob"] == pytest.approx(3785.10, rel=1e-4)


def test_calcular_aba_mensal_usa_pme_arredondado_no_fallback_do_icms_entr_desacob():
    df = pl.DataFrame(
        {
            "id_agrupado": ["id_4c", "id_4c", "id_4c"],
            "co_sefin_agr": ["4006", "4006", "4006"],
            "descr_padrao": ["Produto com PME fracionado"] * 3,
            "Tipo_operacao": ["1 - ENTRADA", "1 - ENTRADA", "1 - ENTRADA"],
            "Dt_doc": [date(2024, 4, 1), date(2024, 4, 2), date(2024, 4, 30)],
            "Dt_e_s": [date(2024, 4, 1), date(2024, 4, 2), date(2024, 4, 30)],
            "ordem_operacoes": [1, 2, 3],
            "Unid": ["UN", "UN", "UN"],
            "unid_ref": ["UN", "UN", "UN"],
            "q_conv": [3.0, 0.0, 0.0],
            "preco_item": [10.0, 0.0, 0.0],
            "entr_desac_anual": [0.0, 0.0, 2.0],
            "saldo_estoque_anual": [3.0, 3.0, 3.0],
            "custo_medio_anual": [3.3333, 3.3333, 3.3333],
            "Aliq_icms": [12.0, 12.0, 12.0],
            "it_pc_interna": [18.0, 18.0, 18.0],
            "it_pc_mva": [40.0, 40.0, 40.0],
            "it_in_st": ["S", "S", "S"],
            "it_in_mva_ajustado": ["N", "N", "N"],
            "finnfe": ["1", "1", "1"],
            "dev_simples": [False, False, False],
            "dev_venda": [False, False, False],
            "dev_compra": [False, False, False],
            "dev_ent_simples": [False, False, False],
            "excluir_estoque": [False, False, True],
        }
    )
    df_aux_st = pl.DataFrame(
        {
            "it_co_sefin": ["4006"],
            "it_da_inicio": ["20240401"],
            "it_da_final": ["20241231"],
            "it_in_st": ["S"],
        }
    )

    result = calcular_aba_mensal_dataframe(df, df_aux_st=df_aux_st)
    row = result.row(0, named=True)

    assert row["pme_mes"] == pytest.approx(3.3333, rel=1e-6)
    assert row["pms_mes"] == 0.0
    assert row["entradas_desacob"] == 2.0
    assert row["ICMS_entr_desacob"] == pytest.approx(0.48, rel=1e-4)


def test_calcular_aba_mensal_nao_cria_entrada_desacob_por_estoque_final():
    df = pl.DataFrame(
        {
            "id_agrupado": ["id_5", "id_5"],
            "co_sefin_agr": ["5005", "5005"],
            "descr_padrao": ["Produto inventario final"] * 2,
            "Tipo_operacao": ["1 - ENTRADA", "3 - ESTOQUE FINAL"],
            "Dt_doc": [date(2024, 12, 1), date(2024, 12, 31)],
            "Dt_e_s": [date(2024, 12, 1), date(2024, 12, 31)],
            "ordem_operacoes": [1, 2],
            "Unid": ["UN", "UN"],
            "unid_ref": ["UN", "UN"],
            "q_conv": [5.0, 0.0],
            "preco_item": [50.0, 0.0],
            "entr_desac_anual": [0.0, 0.0],
            "saldo_estoque_anual": [5.0, 5.0],
            "custo_medio_anual": [10.0, 10.0],
            "Aliq_icms": [18.0, 18.0],
            "it_pc_interna": [18.0, 18.0],
            "it_pc_mva": [40.0, 40.0],
            "it_in_st": ["N", "N"],
            "it_in_mva_ajustado": ["N", "N"],
            "finnfe": ["1", "1"],
            "dev_simples": [False, False],
            "dev_venda": [False, False],
            "dev_compra": [False, False],
            "dev_ent_simples": [False, False],
            "excluir_estoque": [False, False],
        }
    )

    df_aux_st = pl.DataFrame(
        {
            "it_co_sefin": ["5005"],
            "it_da_inicio": ["20240101"],
            "it_da_final": ["20241231"],
            "it_in_st": ["N"],
        }
    )

    result = calcular_aba_mensal_dataframe(df, df_aux_st=df_aux_st)
    row = result.row(0, named=True)

    assert row["entradas_desacob"] == 0.0
    assert row["saldo_mes"] == 5.0
