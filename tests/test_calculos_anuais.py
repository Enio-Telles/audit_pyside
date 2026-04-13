from datetime import date
from pathlib import Path
import sys

import polars as pl
import pytest

sys.path.insert(0, str(Path("src").resolve()))

from transformacao.calculos_anuais import calcular_aba_anual_dataframe


def test_calcular_aba_anual_zera_apenas_icms_saidas_quando_st_ativo_no_ano():
    df = pl.DataFrame(
        {
            "id_agrupado": ["id_1", "id_1", "id_1", "id_1"],
            "co_sefin_agr": ["1001"] * 4,
            "descr_padrao": ["Produto ST"] * 4,
            "unid_ref": ["UN"] * 4,
            "Tipo_operacao": ["0 - ESTOQUE INICIAL", "1 - ENTRADA", "2 - SAIDAS", "3 - ESTOQUE FINAL"],
            "Dt_doc": [date(2024, 1, 1), date(2024, 3, 1), date(2024, 6, 1), date(2024, 12, 31)],
            "Dt_e_s": [date(2024, 1, 1), date(2024, 3, 1), date(2024, 6, 1), date(2024, 12, 31)],
            "ordem_operacoes": [1, 2, 3, 4],
            "q_conv": [10.0, 5.0, 4.0, 0.0],
            "__qtd_decl_final_audit__": [0.0, 0.0, 0.0, 8.0],
            "preco_item": [0.0, 50.0, 48.0, 0.0],
            "Vl_item": [0.0, 50.0, 48.0, 0.0],
            "entr_desac_anual": [0.0, 0.0, 0.0, 0.0],
            "saldo_estoque_anual": [10.0, 15.0, 11.0, 11.0],
            "it_pc_interna": [17.0, 17.0, 17.0, 17.0],
            "dev_simples": [False, False, False, False],
            "excluir_estoque": [False, False, False, False],
        }
    )
    df_aux_st = pl.DataFrame(
        {
            "co_sefin_agr": ["1001"],
            "ano": [2024],
            "ST": ["['S' de 01/01/2024 até 31/12/2024]"],
            "__tem_st_ano__": [True],
            "__aliq_ref__": [17.0],
        }
    )

    result = calcular_aba_anual_dataframe(df, df_aux_st)
    row = result.row(0, named=True)

    assert row["ST"] == "['S' de 01/01/2024 até 31/12/2024]"
    assert row["unid_ref"] == "UN"
    assert row["estoque_final"] == 8.0
    assert row["pme"] == 10.0
    assert row["pms"] == 12.0
    assert row["saidas_desacob"] == 0.0
    assert row["estoque_final_desacob"] == 3.0
    assert row["ICMS_saidas_desac"] == 0.0
    assert row["ICMS_estoque_desac"] == pytest.approx(6.12, rel=1e-4)


def test_calcular_aba_anual_usa_fallback_pme_mva_quando_nao_ha_pms():
    df = pl.DataFrame(
        {
            "id_agrupado": ["id_2", "id_2", "id_2"],
            "co_sefin_agr": ["2002"] * 3,
            "descr_padrao": ["Produto sem PMS"] * 3,
            "unid_ref": ["CX"] * 3,
            "Tipo_operacao": ["0 - ESTOQUE INICIAL", "1 - ENTRADA", "3 - ESTOQUE FINAL"],
            "Dt_doc": [date(2024, 1, 1), date(2024, 2, 1), date(2024, 12, 31)],
            "Dt_e_s": [date(2024, 1, 1), date(2024, 2, 1), date(2024, 12, 31)],
            "ordem_operacoes": [1, 2, 3],
            "q_conv": [0.0, 10.0, 0.0],
            "__qtd_decl_final_audit__": [0.0, 0.0, 7.0],
            "preco_item": [0.0, 100.0, 0.0],
            "Vl_item": [0.0, 100.0, 0.0],
            "entr_desac_anual": [0.0, 0.0, 0.0],
            "saldo_estoque_anual": [0.0, 10.0, 10.0],
            "it_pc_interna": [18.0, 18.0, 18.0],
            "dev_simples": [False, False, False],
            "excluir_estoque": [False, False, False],
        }
    )
    df_aux_st = pl.DataFrame(
        {
            "co_sefin_agr": ["2002"],
            "ano": [2024],
            "ST": ["['N' de 01/01/2024 até 31/12/2024]"],
            "__tem_st_ano__": [False],
            "__aliq_ref__": [18.0],
        }
    )

    result = calcular_aba_anual_dataframe(df, df_aux_st)
    row = result.row(0, named=True)

    assert row["unid_ref"] == "CX"
    assert row["estoque_final"] == 7.0
    assert row["pme"] == 10.0
    assert row["pms"] == 0.0
    assert row["saidas_desacob"] == 0.0
    assert row["estoque_final_desacob"] == 3.0
    assert row["ICMS_saidas_desac"] == 0.0
    assert row["ICMS_estoque_desac"] == pytest.approx(7.02, rel=1e-4)


def test_calcular_aba_anual_inclui_entradas_desacob_em_saidas_calculadas():
    df = pl.DataFrame(
        {
            "id_agrupado": ["id_3", "id_3", "id_3"],
            "co_sefin_agr": ["3003"] * 3,
            "descr_padrao": ["Produto com entrada desacob"] * 3,
            "unid_ref": ["UN"] * 3,
            "Tipo_operacao": ["0 - ESTOQUE INICIAL", "1 - ENTRADA", "3 - ESTOQUE FINAL"],
            "Dt_doc": [date(2024, 1, 1), date(2024, 5, 1), date(2024, 12, 31)],
            "Dt_e_s": [date(2024, 1, 1), date(2024, 5, 1), date(2024, 12, 31)],
            "ordem_operacoes": [1, 2, 3],
            "q_conv": [10.0, 5.0, 0.0],
            "__qtd_decl_final_audit__": [0.0, 0.0, 8.0],
            "preco_item": [0.0, 50.0, 0.0],
            "Vl_item": [0.0, 50.0, 0.0],
            "entr_desac_anual": [0.0, 2.0, 0.0],
            "saldo_estoque_anual": [10.0, 15.0, 15.0],
            "it_pc_interna": [17.0, 17.0, 17.0],
            "dev_simples": [False, False, False],
            "excluir_estoque": [False, False, False],
        }
    )
    df_aux_st = pl.DataFrame(
        {
            "co_sefin_agr": ["3003"],
            "ano": [2024],
            "ST": [""],
            "__tem_st_ano__": [False],
            "__aliq_ref__": [17.0],
        }
    )

    result = calcular_aba_anual_dataframe(df, df_aux_st)
    row = result.row(0, named=True)

    assert row["entradas_desacob"] == 2.0
    assert row["estoque_final"] == 8.0
    assert row["saidas_calculadas"] == 9.0


def test_calcular_aba_anual_ignora_preco_de_saida_com_q_conv_zero_no_pms():
    df = pl.DataFrame(
        {
            "id_agrupado": ["id_4", "id_4", "id_4"],
            "co_sefin_agr": ["4004"] * 3,
            "descr_padrao": ["Produto com saida neutralizada"] * 3,
            "unid_ref": ["UN"] * 3,
            "Tipo_operacao": ["1 - ENTRADA", "2 - SAIDAS", "2 - SAIDAS"],
            "Dt_doc": [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)],
            "Dt_e_s": [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)],
            "ordem_operacoes": [1, 2, 3],
            "q_conv": [10.0, 4.0, 0.0],
            "preco_item": [100.0, 80.0, 900.0],
            "Vl_item": [100.0, 80.0, 900.0],
            "entr_desac_anual": [0.0, 0.0, 0.0],
            "saldo_estoque_anual": [10.0, 6.0, 6.0],
            "it_pc_interna": [17.0, 17.0, 17.0],
            "dev_simples": [False, False, False],
            "excluir_estoque": [False, False, False],
        }
    )
    df_aux_st = pl.DataFrame(
        {
            "co_sefin_agr": ["4004"],
            "ano": [2024],
            "ST": [""],
            "__tem_st_ano__": [False],
            "__aliq_ref__": [17.0],
        }
    )

    result = calcular_aba_anual_dataframe(df, df_aux_st)
    row = result.row(0, named=True)

    assert row["pme"] == 10.0
    assert row["pms"] == 20.0


def test_calcular_aba_anual_usa_pms_arredondado_no_icms_estoque_desac():
    df = pl.DataFrame(
        {
            "id_agrupado": ["id_4b", "id_4b", "id_4b"],
            "co_sefin_agr": ["4005"] * 3,
            "descr_padrao": ["Produto com PMS fracionado estoque"] * 3,
            "unid_ref": ["UN"] * 3,
            "Tipo_operacao": ["1 - ENTRADA", "2 - SAIDAS", "3 - ESTOQUE FINAL"],
            "Dt_doc": [date(2024, 1, 1), date(2024, 2, 1), date(2024, 12, 31)],
            "Dt_e_s": [date(2024, 1, 1), date(2024, 2, 1), date(2024, 12, 31)],
            "ordem_operacoes": [1, 2, 3],
            "q_conv": [2000.0, 1000.0, 0.0],
            "__qtd_decl_final_audit__": [0.0, 0.0, 0.0],
            "preco_item": [20000.0, 10234.0, 0.0],
            "Vl_item": [20000.0, 10234.0, 0.0],
            "entr_desac_anual": [0.0, 0.0, 0.0],
            "saldo_estoque_anual": [2000.0, 1000.0, 1000.0],
            "it_pc_interna": [17.0, 17.0, 17.0],
            "dev_simples": [False, False, False],
            "excluir_estoque": [False, False, False],
        }
    )

    result = calcular_aba_anual_dataframe(df, pl.DataFrame())
    row = result.row(0, named=True)

    assert row["pms"] == 10.23
    assert row["estoque_final_desacob"] == 1000.0
    assert row["ICMS_estoque_desac"] == pytest.approx(1739.10, rel=1e-4)


def test_calcular_aba_anual_usa_pms_arredondado_no_icms_saidas_desac():
    df = pl.DataFrame(
        {
            "id_agrupado": ["id_4c", "id_4c", "id_4c"],
            "co_sefin_agr": ["4006"] * 3,
            "descr_padrao": ["Produto com PMS fracionado saida"] * 3,
            "unid_ref": ["UN"] * 3,
            "Tipo_operacao": ["0 - ESTOQUE INICIAL", "2 - SAIDAS", "3 - ESTOQUE FINAL"],
            "Dt_doc": [date(2024, 1, 1), date(2024, 2, 1), date(2024, 12, 31)],
            "Dt_e_s": [date(2024, 1, 1), date(2024, 2, 1), date(2024, 12, 31)],
            "ordem_operacoes": [1, 2, 3],
            "q_conv": [3000.0, 1000.0, 0.0],
            "__qtd_decl_final_audit__": [0.0, 0.0, 3000.0],
            "preco_item": [0.0, 10234.0, 0.0],
            "Vl_item": [0.0, 10234.0, 0.0],
            "entr_desac_anual": [0.0, 0.0, 0.0],
            "saldo_estoque_anual": [3000.0, 2000.0, 2000.0],
            "it_pc_interna": [17.0, 17.0, 17.0],
            "dev_simples": [False, False, False],
            "excluir_estoque": [False, False, False],
        }
    )

    result = calcular_aba_anual_dataframe(df, pl.DataFrame())
    row = result.row(0, named=True)

    assert row["pms"] == 10.23
    assert row["saidas_desacob"] == 1000.0
    assert row["ICMS_saidas_desac"] == pytest.approx(1739.10, rel=1e-4)


def test_calcular_aba_anual_zera_estoque_final_desacob_quando_saldo_igual_estoque():
    df = pl.DataFrame(
        {
            "id_agrupado": ["id_5", "id_5", "id_5"],
            "co_sefin_agr": ["5005"] * 3,
            "descr_padrao": ["Produto saldo igual"] * 3,
            "unid_ref": ["UN"] * 3,
            "Tipo_operacao": ["0 - ESTOQUE INICIAL", "1 - ENTRADA", "3 - ESTOQUE FINAL"],
            "Dt_doc": [date(2024, 1, 1), date(2024, 6, 1), date(2024, 12, 31)],
            "Dt_e_s": [date(2024, 1, 1), date(2024, 6, 1), date(2024, 12, 31)],
            "ordem_operacoes": [1, 2, 3],
            "q_conv": [2.0, 3.0, 0.0],
            "__qtd_decl_final_audit__": [0.0, 0.0, 5.0],
            "preco_item": [0.0, 30.0, 0.0],
            "Vl_item": [0.0, 30.0, 0.0],
            "entr_desac_anual": [0.0, 0.0, 0.0],
            "saldo_estoque_anual": [2.0, 5.0, 5.0],
            "it_pc_interna": [17.0, 17.0, 17.0],
            "dev_simples": [False, False, False],
            "excluir_estoque": [False, False, False],
        }
    )

    result = calcular_aba_anual_dataframe(df, pl.DataFrame())
    row = result.row(0, named=True)

    assert row["estoque_final"] == 5.0
    assert row["saidas_desacob"] == 0.0
    assert row["estoque_final_desacob"] == 0.0
    assert row["ICMS_saidas_desac"] == 0.0
    assert row["ICMS_estoque_desac"] == 0.0


def test_calcular_aba_anual_zera_estoque_final_desacob_quando_saldo_menor_que_estoque():
    df = pl.DataFrame(
        {
            "id_agrupado": ["id_6", "id_6", "id_6"],
            "co_sefin_agr": ["6006"] * 3,
            "descr_padrao": ["Produto saldo menor"] * 3,
            "unid_ref": ["UN"] * 3,
            "Tipo_operacao": ["0 - ESTOQUE INICIAL", "1 - ENTRADA", "3 - ESTOQUE FINAL"],
            "Dt_doc": [date(2024, 1, 1), date(2024, 6, 1), date(2024, 12, 31)],
            "Dt_e_s": [date(2024, 1, 1), date(2024, 6, 1), date(2024, 12, 31)],
            "ordem_operacoes": [1, 2, 3],
            "q_conv": [2.0, 3.0, 0.0],
            "__qtd_decl_final_audit__": [0.0, 0.0, 8.0],
            "preco_item": [0.0, 30.0, 0.0],
            "Vl_item": [0.0, 30.0, 0.0],
            "entr_desac_anual": [0.0, 0.0, 0.0],
            "saldo_estoque_anual": [2.0, 5.0, 5.0],
            "it_pc_interna": [17.0, 17.0, 17.0],
            "dev_simples": [False, False, False],
            "excluir_estoque": [False, False, False],
        }
    )

    result = calcular_aba_anual_dataframe(df, pl.DataFrame())
    row = result.row(0, named=True)

    assert row["estoque_final"] == 8.0
    assert row["saidas_desacob"] == 3.0
    assert row["estoque_final_desacob"] == 0.0
    assert row["ICMS_saidas_desac"] == pytest.approx(6.63, rel=1e-4)
    assert row["ICMS_estoque_desac"] == 0.0


def test_calcular_aba_anual_mantem_estoque_final_zero_para_inventario_gerado():
    df = pl.DataFrame(
        {
            "id_agrupado": ["id_7", "id_7", "id_7"],
            "co_sefin_agr": ["7007"] * 3,
            "descr_padrao": ["Produto inventario gerado"] * 3,
            "unid_ref": ["UN"] * 3,
            "Tipo_operacao": ["0 - ESTOQUE INICIAL", "1 - ENTRADA", "3 - ESTOQUE FINAL gerado"],
            "Dt_doc": [date(2024, 1, 1), date(2024, 6, 1), date(2024, 12, 31)],
            "Dt_e_s": [date(2024, 1, 1), date(2024, 6, 1), date(2024, 12, 31)],
            "ordem_operacoes": [1, 2, 3],
            "q_conv": [2.0, 3.0, 0.0],
            "__qtd_decl_final_audit__": [0.0, 0.0, 0.0],
            "preco_item": [0.0, 30.0, 0.0],
            "Vl_item": [0.0, 30.0, 0.0],
            "entr_desac_anual": [0.0, 0.0, 0.0],
            "saldo_estoque_anual": [2.0, 5.0, 5.0],
            "it_pc_interna": [17.0, 17.0, 17.0],
            "dev_simples": [False, False, False],
            "excluir_estoque": [False, False, False],
        }
    )

    result = calcular_aba_anual_dataframe(df, pl.DataFrame())
    row = result.row(0, named=True)

    assert row["estoque_final"] == 0.0
    assert row["saidas_calculadas"] == 5.0
    assert row["saidas_desacob"] == 0.0
    assert row["estoque_final_desacob"] == 5.0


def test_calcular_aba_anual_nao_cria_entrada_desacob_por_estoque_final():
    df = pl.DataFrame(
        {
            "id_agrupado": ["id_8", "id_8"],
            "co_sefin_agr": ["8008", "8008"],
            "descr_padrao": ["Produto inventario final"] * 2,
            "unid_ref": ["UN", "UN"],
            "Tipo_operacao": ["1 - ENTRADA", "3 - ESTOQUE FINAL"],
            "Dt_doc": [date(2024, 12, 1), date(2024, 12, 31)],
            "Dt_e_s": [date(2024, 12, 1), date(2024, 12, 31)],
            "ordem_operacoes": [1, 2],
            "q_conv": [5.0, 0.0],
            "__qtd_decl_final_audit__": [0.0, 8.0],
            "preco_item": [50.0, 0.0],
            "Vl_item": [50.0, 0.0],
            "entr_desac_anual": [0.0, 0.0],
            "saldo_estoque_anual": [5.0, 5.0],
            "it_pc_interna": [17.0, 17.0],
            "dev_simples": [False, False],
            "excluir_estoque": [False, False],
        }
    )

    result = calcular_aba_anual_dataframe(df, pl.DataFrame())
    row = result.row(0, named=True)

    assert row["entradas_desacob"] == 0.0
    assert row["estoque_final"] == 8.0
    assert row["saldo_final"] == 5.0
    assert row["saidas_desacob"] == 3.0
    assert row["estoque_final_desacob"] == 0.0


def test_calcular_aba_anual_mantem_exclusividade_entre_saidas_e_estoque_desacob():
    df = pl.DataFrame(
        {
            "id_agrupado": ["id_9", "id_9", "id_10", "id_10"],
            "co_sefin_agr": ["9009", "9009", "9010", "9010"],
            "descr_padrao": ["Produto A", "Produto A", "Produto B", "Produto B"],
            "unid_ref": ["UN", "UN", "UN", "UN"],
            "Tipo_operacao": ["1 - ENTRADA", "3 - ESTOQUE FINAL", "1 - ENTRADA", "3 - ESTOQUE FINAL"],
            "Dt_doc": [date(2024, 1, 2), date(2024, 12, 31), date(2024, 1, 2), date(2024, 12, 31)],
            "Dt_e_s": [date(2024, 1, 2), date(2024, 12, 31), date(2024, 1, 2), date(2024, 12, 31)],
            "ordem_operacoes": [1, 2, 3, 4],
            "q_conv": [5.0, 0.0, 12.0, 0.0],
            "__qtd_decl_final_audit__": [0.0, 8.0, 0.0, 7.0],
            "preco_item": [50.0, 0.0, 120.0, 0.0],
            "Vl_item": [50.0, 0.0, 120.0, 0.0],
            "entr_desac_anual": [0.0, 0.0, 0.0, 0.0],
            "saldo_estoque_anual": [5.0, 5.0, 12.0, 12.0],
            "it_pc_interna": [17.0, 17.0, 17.0, 17.0],
            "dev_simples": [False, False, False, False],
            "excluir_estoque": [False, False, False, False],
        }
    )

    result = calcular_aba_anual_dataframe(df, pl.DataFrame())

    assert result.filter((pl.col("saidas_desacob") > 0) & (pl.col("estoque_final_desacob") > 0)).height == 0
