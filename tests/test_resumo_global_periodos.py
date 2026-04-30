from __future__ import annotations

import polars as pl

from transformacao.resumo_global import gerar_resumo_global_dataframe


def test_resumo_global_periodos_cod_per_sequencial_usa_data_estoque_inicial() -> None:
    """cod_per sequencial (1,2,3) nao e YYYYMM; o mapeamento deve usar data_estoque_inicial."""
    mensal = pl.DataFrame(
        {"ano": [2025], "mes": [1], "ICMS_entr_desacob": [10.0], "ICMS_entr_desacob_periodo": [2.0]}
    )
    anual = pl.DataFrame(
        {"ano": [2025], "ICMS_saidas_desac": [5.0], "ICMS_estoque_desac": [3.0]}
    )
    periodos = pl.DataFrame(
        {
            "cod_per": [1, 2],
            "data_estoque_inicial": ["01/01/2025", "01/04/2025"],
            "ICMS_saidas_desac_periodo": [4.0, 7.0],
            "ICMS_estoque_desac_periodo": [6.0, 9.0],
        }
    )

    resumo = gerar_resumo_global_dataframe(mensal, anual, periodos, anos_base=[2025])
    jan = resumo.filter(pl.col("Ano/Mes") == "2025-01").row(0, named=True)
    abr = resumo.filter(pl.col("Ano/Mes") == "2025-04").row(0, named=True)

    assert jan["ICMS_saidas_desac_periodo"] == 4.0
    assert jan["ICMS_estoque_desac_periodo"] == 6.0
    assert jan["Total_periodo"] == 2.0 + 4.0 + 6.0
    assert abr["ICMS_saidas_desac_periodo"] == 7.0
    assert abr["ICMS_estoque_desac_periodo"] == 9.0


def test_resumo_global_dataframe_inclui_totais_icms_periodo() -> None:
    mensal = pl.DataFrame(
        {
            "ano": [2025, 2025],
            "mes": [1, 1],
            "ICMS_entr_desacob": [10.0, 5.0],
            "ICMS_entr_desacob_periodo": [1.25, 2.75],
        }
    )
    anual = pl.DataFrame(
        {
            "ano": [2025],
            "ICMS_saidas_desac": [7.0],
            "ICMS_estoque_desac": [3.0],
        }
    )
    periodos = pl.DataFrame(
        {
            "cod_per": [202501, 202501],
            "ICMS_saidas_desac_periodo": [4.0, 6.0],
            "ICMS_estoque_desac_periodo": [8.0, 2.0],
        }
    )

    resumo = gerar_resumo_global_dataframe(mensal, anual, periodos)
    row_janeiro = resumo.filter(pl.col("Ano/Mes") == "2025-01").row(0, named=True)

    assert row_janeiro["ICMS_entr_desacob"] == 15.0
    assert row_janeiro["ICMS_entr_desacob_periodo"] == 4.0
    assert row_janeiro["ICMS_saidas_desac_periodo"] == 10.0
    assert row_janeiro["ICMS_estoque_desac_periodo"] == 10.0
    assert row_janeiro["Total_periodo"] == 24.0
