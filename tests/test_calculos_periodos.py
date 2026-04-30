from __future__ import annotations

from datetime import date

import polars as pl

from transformacao.calculos_periodo_pkg.calculos_periodo import (
    calcular_aba_periodos_dataframe,
)


def test_calcular_aba_periodos_data_estoque_inicial_e_final_distintas() -> None:
    df = pl.DataFrame(
        {
            "id_agrupado": ["A", "A", "A", "A"],
            "periodo_inventario": [1, 1, 1, 1],
            "descr_padrao": ["Prod", "Prod", "Prod", "Prod"],
            "unid_ref": ["UN", "UN", "UN", "UN"],
            "co_sefin_agr": ["x", "x", "x", "x"],
            "Dt_doc": [date(2025, 1, 1), date(2025, 6, 15), date(2025, 9, 30), date(2025, 10, 1)],
            "Dt_e_s": [date(2025, 1, 1), date(2025, 6, 15), date(2025, 9, 30), date(2025, 10, 1)],
            "Tipo_operacao": [
                "0 - ESTOQUE INICIAL",
                "1 - ENTRADA",
                "3 - ESTOQUE FINAL",
                "1 - ENTRADA",
            ],
            "q_conv": [5.0, 10.0, 0.0, 3.0],
            "__qtd_decl_final_audit__": [0.0, 0.0, 8.0, 0.0],
            "preco_item": [0.0, 50.0, 0.0, 50.0],
            "saldo_estoque_periodo": [5.0, 15.0, 7.0, 10.0],
            "entr_desac_periodo": [0.0, 0.0, 0.0, 0.0],
            "ordem_operacoes": [1, 2, 3, 4],
            "it_pc_interna": [18.0, 18.0, 18.0, 18.0],
        }
    )

    result = calcular_aba_periodos_dataframe(df, df_aux_st=pl.DataFrame())
    row = result.row(0, named=True)

    assert row["data_estoque_inicial"] == "01/01/2025"
    assert row["data_estoque_final"] == "30/09/2025"
    assert row["periodo_label"] == "01/01/2025 até 30/09/2025"


def test_calcular_aba_periodos_periodo_em_aberto_mesma_data() -> None:
    """EI e EF na mesma data mas EF=0: periodo em aberto, data_estoque_final deve ser null."""
    df = pl.DataFrame(
        {
            "id_agrupado": ["A", "A", "A"],
            "periodo_inventario": [1, 1, 1],
            "descr_padrao": ["Prod", "Prod", "Prod"],
            "unid_ref": ["UN", "UN", "UN"],
            "co_sefin_agr": ["x", "x", "x"],
            "Dt_doc": [date(2022, 1, 1), date(2022, 6, 15), date(2022, 1, 1)],
            "Dt_e_s": [date(2022, 1, 1), date(2022, 6, 15), date(2022, 1, 1)],
            "Tipo_operacao": ["0 - ESTOQUE INICIAL", "1 - ENTRADA", "3 - ESTOQUE FINAL"],
            "q_conv": [50.0, 10.0, 0.0],
            "__qtd_decl_final_audit__": [0.0, 0.0, 0.0],
            "preco_item": [0.0, 50.0, 0.0],
            "saldo_estoque_periodo": [50.0, 60.0, 60.0],
            "entr_desac_periodo": [0.0, 0.0, 0.0],
            "ordem_operacoes": [1, 2, 3],
            "it_pc_interna": [18.0, 18.0, 18.0],
        }
    )

    result = calcular_aba_periodos_dataframe(df, df_aux_st=pl.DataFrame())
    row = result.row(0, named=True)

    assert row["data_estoque_inicial"] == "01/01/2022"
    assert row["data_estoque_final"] is None
    assert row["periodo_label"] == "01/01/2022 até ?"


def test_calcular_aba_periodos_sem_estoque_inicial_data_nula() -> None:
    """Periodo sem operacao de estoque inicial: data_estoque_inicial deve ser null."""
    df = pl.DataFrame(
        {
            "id_agrupado": ["A", "A"],
            "periodo_inventario": [0, 0],
            "descr_padrao": ["Prod", "Prod"],
            "unid_ref": ["UN", "UN"],
            "co_sefin_agr": ["x", "x"],
            "Dt_doc": [date(2020, 12, 31), date(2020, 12, 31)],
            "Dt_e_s": [date(2020, 12, 31), date(2020, 12, 31)],
            "Tipo_operacao": ["1 - ENTRADA", "3 - ESTOQUE FINAL"],
            "q_conv": [10.0, 0.0],
            "__qtd_decl_final_audit__": [0.0, 5.0],
            "preco_item": [50.0, 0.0],
            "saldo_estoque_periodo": [10.0, 5.0],
            "entr_desac_periodo": [0.0, 0.0],
            "ordem_operacoes": [1, 2],
            "it_pc_interna": [18.0, 18.0],
        }
    )

    result = calcular_aba_periodos_dataframe(df, df_aux_st=pl.DataFrame())
    row = result.row(0, named=True)

    assert row["data_estoque_inicial"] is None
    assert row["data_estoque_final"] == "31/12/2020"
    assert row["periodo_label"] == "? até 31/12/2020"


def test_calcular_aba_periodos_expoe_aliases_icms_periodo() -> None:
    df = pl.DataFrame(
        {
            "id_agrupado": ["A", "A"],
            "periodo_inventario": [202501, 202501],
            "descr_padrao": ["Produto A", "Produto A"],
            "unid_ref": ["UN", "UN"],
            "co_sefin_agr": ["1000", "1000"],
            "Dt_doc": [date(2025, 1, 1), date(2025, 1, 31)],
            "Dt_e_s": [date(2025, 1, 1), date(2025, 1, 31)],
            "Tipo_operacao": ["1 - ENTRADA", "3 - ESTOQUE FINAL"],
            "q_conv": [10.0, 0.0],
            "__qtd_decl_final_audit__": [0.0, 12.0],
            "preco_item": [100.0, 0.0],
            "saldo_estoque_periodo": [10.0, 10.0],
            "entr_desac_periodo": [0.0, 0.0],
            "ordem_operacoes": [1, 2],
            "it_pc_interna": [18.0, 18.0],
        }
    )

    result = calcular_aba_periodos_dataframe(df, df_aux_st=pl.DataFrame())
    row = result.row(0, named=True)

    assert "ICMS_saidas_desac_periodo" in result.columns
    assert "ICMS_estoque_desac_periodo" in result.columns
    assert row["ICMS_saidas_desac_periodo"] == row["ICMS_saidas_desac"]
    assert row["ICMS_estoque_desac_periodo"] == row["ICMS_estoque_desac"]
