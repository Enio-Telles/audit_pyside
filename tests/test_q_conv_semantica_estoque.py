from datetime import date
from pathlib import Path
import sys
from datetime import date
from pathlib import Path
import sys

import polars as pl

sys.path.insert(0, str(Path("src").resolve()))

from transformacao.calculos_anuais_pkg.calculos_anuais import calcular_aba_anual_dataframe
from transformacao.calculos_mensais_pkg.calculos_mensais import calcular_aba_mensal_dataframe
from transformacao.rastreabilidade_produtos.fatores_conversao import calcular_fatores_conversao


def test_calculos_anuais_separam_fluxo_fisico_de_auditoria_final():
    df = pl.DataFrame(
        {
            "id_agrupado": ["prod_1"] * 4,
            "Tipo_operacao": [
                "0 - ESTOQUE INICIAL",
                "1 - ENTRADA",
                "2 - SAIDAS",
                "3 - ESTOQUE FINAL",
            ],
            "Dt_doc": [date(2024, 1, 1), date(2024, 1, 10), date(2024, 1, 20), date(2024, 12, 31)],
            "Dt_e_s": [date(2024, 1, 1), date(2024, 1, 10), date(2024, 1, 20), date(2024, 12, 31)],
            "q_conv": [100.0, 20.0, 10.0, 90.0],
            "q_conv_fisica": [100.0, 20.0, 10.0, 0.0],
            "__qtd_decl_final_audit__": [0.0, 0.0, 0.0, 90.0],
            "entr_desac_anual": [0.0, 0.0, 0.0, 0.0],
            "saldo_estoque_anual": [100.0, 120.0, 110.0, 110.0],
            "ordem_operacoes": [1, 2, 3, 4],
            "descr_padrao": ["Produto 1"] * 4,
            "unid_ref": ["UN"] * 4,
            "co_sefin_agr": ["COD"] * 4,
            "it_pc_interna": [18.0] * 4,
            "preco_item": [1000.0, 200.0, 100.0, 0.0],
            "Vl_item": [1000.0, 200.0, 100.0, 0.0],
            "dev_simples": [False] * 4,
            "excluir_estoque": [False] * 4,
        }
    )

    df_anual = calcular_aba_anual_dataframe(df)
    row = df_anual.row(0, named=True)

    assert row["estoque_inicial"] == 100.0
    assert row["entradas"] == 20.0
    assert row["saidas"] == 10.0
    assert row["estoque_final"] == 90.0
    assert row["saldo_final"] == 110.0


def test_calculos_mensais_ignoram_qconv_observacional_em_estoque_final():
    df = pl.DataFrame(
        {
            "id_agrupado": ["prod_1"] * 3,
            "Tipo_operacao": ["1 - ENTRADA", "2 - SAIDAS", "3 - ESTOQUE FINAL"],
            "Dt_doc": [date(2024, 6, 1), date(2024, 6, 10), date(2024, 6, 30)],
            "Dt_e_s": [date(2024, 6, 1), date(2024, 6, 10), date(2024, 6, 30)],
            "finnfe": ["1"] * 3,
            "q_conv": [20.0, 5.0, 90.0],
            "q_conv_fisica": [20.0, 5.0, 0.0],
            "__qtd_decl_final_audit__": [0.0, 0.0, 90.0],
            "entr_desac_anual": [0.0, 0.0, 0.0],
            "saldo_estoque_anual": [20.0, 15.0, 15.0],
            "custo_medio_anual": [10.0, 10.0, 10.0],
            "entr_desac_periodo": [0.0, 0.0, 0.0],
            "saldo_estoque_periodo": [20.0, 15.0, 15.0],
            "custo_medio_periodo": [10.0, 10.0, 10.0],
            "ordem_operacoes": [1, 2, 3],
            "descr_padrao": ["Produto 1"] * 3,
            "Unid": ["UN"] * 3,
            "unid_ref": ["UN"] * 3,
            "co_sefin_agr": ["COD"] * 3,
            "preco_item": [200.0, 50.0, 0.0],
            "Vl_item": [200.0, 50.0, 0.0],
            "Aliq_icms": [12.0] * 3,
            "it_pc_interna": [18.0] * 3,
            "it_pc_mva": [0.0] * 3,
            "it_in_st": ["N"] * 3,
            "it_in_mva_ajustado": ["N"] * 3,
            "dev_simples": [False] * 3,
            "dev_venda": [False] * 3,
            "dev_compra": [False] * 3,
            "dev_ent_simples": [False] * 3,
            "excluir_estoque": [False] * 3,
        }
    )

    df_mensal = calcular_aba_mensal_dataframe(df)
    row = df_mensal.row(0, named=True)

    assert row["qtd_entradas"] == 20.0
    assert row["qtd_saidas"] == 5.0


def test_q_conv_fallback_and_semantics():
    df = pl.DataFrame(
        {
            "id_agrupado": ["A", "A", "A"],
            "Tipo_operacao": [
                "0 - ESTOQUE INICIAL",
                "1 - ENTRADA",
                "3 - ESTOQUE FINAL",
            ],
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

    df = df.with_columns(
        [pl.col("Dt_e_s").str.strptime(pl.Date, "%Y-%m-%d").alias("Dt_e_s")]
    )

    # Simula parquet antigo sem q_conv_fisica
    res = calcular_aba_mensal_dataframe(df)

    # No mês de 2023-02 (entrada) esperamos qtd_entradas == 5.0
    res_feb = res.filter(
        (pl.col("ano") == 2023) & (pl.col("mes") == 2) & (pl.col("id_agregado") == "A")
    )
    assert res_feb.height == 1
    assert float(res_feb["qtd_entradas"][0]) == 5.0


def test_fatores_conversao_prioriza_map_produto_agrupado_no_vinculo(tmp_path):
    cnpj = "12345678901234"
    pasta = tmp_path

    # produtos_final com descricoes ambiguas (mesma descricao_normalizada para ids diferentes)
    df_prod_final = pl.DataFrame(
        {
            "id_agrupado": ["P2", "P3"],
            "descricao_normalizada": ["prod_x", "prod_x"],
            "descr_padrao": ["Prod X v2", "Prod X v3"],
            "unid_ref_sugerida": ["UN", "UN"],
        }
    )

    # map_produto_agrupado aponta para P1 para 'prod_x' (deve ser priorizado)
    df_map = pl.DataFrame({"descricao_normalizada": ["prod_x"], "id_agrupado": ["P1"]})

    # item_unidades com a descricao que sera vinculada
    df_unid = pl.DataFrame(
        {
            "descricao": ["prod_x"],
            "unid": ["UN"],
            "compras": [0.0],
            "vendas": [0.0],
            "qtd_compras": [0.0],
            "qtd_vendas": [0.0],
        }
    )

    arq_prod_final = pasta / f"produtos_final_{cnpj}.parquet"
    arq_map = pasta / f"map_produto_agrupado_{cnpj}.parquet"
    arq_unid = pasta / f"item_unidades_{cnpj}.parquet"

    df_prod_final.write_parquet(arq_prod_final)
    df_map.write_parquet(arq_map)
    df_unid.write_parquet(arq_unid)

    ok = calcular_fatores_conversao(cnpj, pasta)
    assert ok is True

    arq_fatores = pasta / f"fatores_conversao_{cnpj}.parquet"
    assert arq_fatores.exists()
    df_fatores = pl.read_parquet(arq_fatores)

    # Verifica se id_agrupado P1 (vinculo pelo map) esta presente
    assert df_fatores.filter(pl.col("id_agrupado") == "P1").height == 1
    # Simula parquet antigo sem q_conv_fisica
    res = calcular_aba_mensal_dataframe(df)

    # No mês de 2023-02 (entrada) esperamos qtd_entradas == 5.0
    res_feb = res.filter(
        (pl.col("ano") == 2023) & (pl.col("mes") == 2) & (pl.col("id_agregado") == "A")
    )
    assert res_feb.height == 1
    assert float(res_feb["qtd_entradas"][0]) == 5.0


def test_fatores_conversao_prioriza_map_produto_agrupado_no_vinculo(tmp_path):
    cnpj = "12345678901234"
    pasta = tmp_path

    # produtos_final com descricoes ambiguas (mesma descricao_normalizada para ids diferentes)
    df_prod_final = pl.DataFrame(
        {
            "id_agrupado": ["P2", "P3"],
            "descricao_normalizada": ["prod_x", "prod_x"],
            "descr_padrao": ["Prod X v2", "Prod X v3"],
            "unid_ref_sugerida": ["UN", "UN"],
        }
    )

    # map_produto_agrupado aponta para P1 para 'prod_x' (deve ser priorizado)
    df_map = pl.DataFrame({"descricao_normalizada": ["prod_x"], "id_agrupado": ["P1"]})

    # item_unidades com a descricao que sera vinculada
    df_unid = pl.DataFrame(
        {
            "descricao": ["prod_x"],
            "unid": ["UN"],
            "compras": [0.0],
            "vendas": [0.0],
            "qtd_compras": [0.0],
            "qtd_vendas": [0.0],
        }
    )

    arq_prod_final = pasta / f"produtos_final_{cnpj}.parquet"
    arq_map = pasta / f"map_produto_agrupado_{cnpj}.parquet"
    arq_unid = pasta / f"item_unidades_{cnpj}.parquet"

    df_prod_final.write_parquet(arq_prod_final)
    df_map.write_parquet(arq_map)
    df_unid.write_parquet(arq_unid)

    ok = calcular_fatores_conversao(cnpj, pasta)
    assert ok is True

    arq_fatores = pasta / f"fatores_conversao_{cnpj}.parquet"
    assert arq_fatores.exists()
    df_fatores = pl.read_parquet(arq_fatores)

    # Verifica se id_agrupado P1 (vinculo pelo map) esta presente
    assert df_fatores.filter(pl.col("id_agrupado") == "P1").height == 1
