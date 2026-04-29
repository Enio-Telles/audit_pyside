from __future__ import annotations

from datetime import date

import polars as pl

from transformacao.calculos_periodo_pkg.calculos_periodo import (
    calcular_aba_periodos_dataframe,
)


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
