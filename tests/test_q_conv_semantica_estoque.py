from datetime import date
from pathlib import Path
import sys

import polars as pl

sys.path.insert(0, str(Path("src").resolve()))

from transformacao.calculos_anuais_pkg.calculos_anuais import calcular_aba_anual_dataframe
from transformacao.calculos_mensais_pkg.calculos_mensais import calcular_aba_mensal_dataframe


def test_anual_usa_q_conv_fisica_e_nao_q_conv_para_movimento():
    df = pl.DataFrame(
        {
            "id_agrupado": ["AGR_1"] * 4,
            "Tipo_operacao": [
                "0 - ESTOQUE INICIAL",
                "1 - ENTRADA",
                "2 - SAIDAS",
                "3 - ESTOQUE FINAL",
            ],
            "Dt_doc": [date(2021, 1, 1), date(2021, 1, 5), date(2021, 1, 10), date(2021, 12, 31)],
            "Dt_e_s": [date(2021, 1, 1), date(2021, 1, 5), date(2021, 1, 10), date(2021, 12, 31)],
            "q_conv": [100.0, 50.0, 20.0, 130.0],
            "q_conv_fisica": [100.0, 50.0, 20.0, 0.0],
            "__qtd_decl_final_audit__": [0.0, 0.0, 0.0, 130.0],
            "entr_desac_anual": [0.0, 0.0, 0.0, 0.0],
            "saldo_estoque_anual": [100.0, 150.0, 130.0, 130.0],
            "ordem_operacoes": [1, 2, 3, 4],
            "descr_padrao": ["Produto A"] * 4,
            "unid_ref": ["UN"] * 4,
            "co_sefin_agr": ["COD1"] * 4,
            "it_pc_interna": [18.0] * 4,
            "preco_item": [100.0, 50.0, 20.0, 0.0],
            "Vl_item": [100.0, 50.0, 20.0, 0.0],
            "dev_simples": [False] * 4,
            "excluir_estoque": [False] * 4,
        }
    )

    df_anual = calcular_aba_anual_dataframe(df)
    row = df_anual.row(0, named=True)

    assert row["estoque_inicial"] == 100.0
    assert row["entradas"] == 50.0
    assert row["saidas"] == 20.0
    assert row["estoque_final"] == 130.0
    assert row["saldo_final"] == 130.0


def test_mensal_ignora_q_conv_de_estoque_final_nas_metricas_fisicas():
    df = pl.DataFrame(
        {
            "id_agrupado": ["AGR_1", "AGR_1", "AGR_1"],
            "Tipo_operacao": ["1 - ENTRADA", "2 - SAIDAS", "3 - ESTOQUE FINAL"],
            "Dt_doc": [date(2021, 6, 1), date(2021, 6, 10), date(2021, 6, 30)],
            "Dt_e_s": [date(2021, 6, 1), date(2021, 6, 10), date(2021, 6, 30)],
            "q_conv": [10.0, 4.0, 999.0],
            "q_conv_fisica": [10.0, 4.0, 0.0],
            "finnfe": ["1", "1", "1"],
            "preco_item": [100.0, 40.0, 0.0],
            "Vl_item": [100.0, 40.0, 0.0],
            "entr_desac_anual": [0.0, 0.0, 0.0],
            "saldo_estoque_anual": [10.0, 6.0, 6.0],
            "custo_medio_anual": [10.0, 10.0, 10.0],
            "entr_desac_periodo": [0.0, 0.0, 0.0],
            "saldo_estoque_periodo": [10.0, 6.0, 6.0],
            "custo_medio_periodo": [10.0, 10.0, 10.0],
            "Aliq_icms": [12.0, 12.0, 12.0],
            "it_pc_interna": [18.0, 18.0, 18.0],
            "it_pc_mva": [0.0, 0.0, 0.0],
            "it_in_st": ["N", "N", "N"],
            "it_in_mva_ajustado": ["N", "N", "N"],
            "excluir_estoque": [False, False, False],
            "dev_simples": [False, False, False],
            "dev_venda": [False, False, False],
            "dev_compra": [False, False, False],
            "dev_ent_simples": [False, False, False],
            "Unid": ["UN", "UN", "UN"],
            "unid_ref": ["UN", "UN", "UN"],
            "ordem_operacoes": [1, 2, 3],
            "co_sefin_agr": ["COD1", "COD1", "COD1"],
            "descr_padrao": ["Produto A", "Produto A", "Produto A"],
        }
    )

    df_mensal = calcular_aba_mensal_dataframe(df)
    row = df_mensal.row(0, named=True)

    assert row["qtd_entradas"] == 10.0
    assert row["qtd_saidas"] == 4.0


def test_q_conv_fallback_and_semantics():
    df = pl.DataFrame(
        {
            "id_agrupado": ["A", "A", "A"],
            "Tipo_operacao": ["0 - ESTOQUE INICIAL", "1 - ENTRADA", "3 - ESTOQUE FINAL"],
            "Qtd": [10, 5, 2],
            "fator": [1.0, 1.0, 1.0],
            "Dt_e_s": ["2023-01-01", "2023-02-01", "2023-12-31"],
            "Dt_doc": ["2023-01-01", "2023-02-01", "2023-12-31"],
            "finnfe": ["1", "1", "1"],
            "q_conv": [10.0, 5.0, 2.0],
            "descr_padrao": ["Produto A", "Produto A", "Produto A"],
            "preco_item": [0.0, 100.0, 0.0],
        }
    )

    df = df.with_columns([pl.col("Dt_e_s").str.strptime(pl.Date, "%Y-%m-%d").alias("Dt_e_s")])

    # Simula parquet antigo sem q_conv_fisica
    res = calcular_aba_mensal_dataframe(df)

    # No mês de 2023-02 (entrada) esperamos qtd_entradas == 5.0
    res_feb = res.filter((pl.col("ano") == 2023) & (pl.col("mes") == 2) & (pl.col("id_agregado") == "A"))
    assert res_feb.height == 1
    assert float(res_feb["qtd_entradas"][0]) == 5.0
