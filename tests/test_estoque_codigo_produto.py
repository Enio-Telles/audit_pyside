from __future__ import annotations

from datetime import date
from pathlib import Path
import sys

import polars as pl

sys.path.insert(0, str(Path("src").resolve()))

from transformacao.estoque_codigo_produto import calcular_estoque_codigo_produto_dataframe


def test_estoque_codigo_produto_consolida_por_cod_item_com_qconv_convertido() -> None:
    mov = pl.DataFrame(
        {
            "id_agrupado": ["AGR_DESCR_1", "AGR_DESCR_2", "AGR_DESCR_1"],
            "Cod_item": ["P001", "P001", "P001"],
            "fonte": ["c170", "nfe", "bloco_h"],
            "Tipo_operacao": ["1 - ENTRADA", "2 - SAIDAS", "3 - ESTOQUE FINAL"],
            "Dt_doc": [date(2024, 1, 5), date(2024, 1, 20), date(2024, 12, 31)],
            "Dt_e_s": [date(2024, 1, 5), date(2024, 1, 20), date(2024, 12, 31)],
            "ordem_operacoes": [1, 2, 3],
            "Unid": ["CX", "UN", "UN"],
            "unid_ref": ["UN", "UN", "UN"],
            "fator": [12.0, 1.0, 1.0],
            "q_conv": [12.0, 2.0, 9.0],
            "q_conv_fisica": [12.0, 2.0, 0.0],
            "__qtd_decl_final_audit__": [0.0, 0.0, 9.0],
            "preco_item": [120.0, 30.0, 0.0],
            "Vl_item": [120.0, 30.0, 0.0],
            "entr_desac_anual": [0.0, 0.0, 0.0],
            "saldo_estoque_anual": [12.0, 10.0, 10.0],
            "custo_medio_anual": [10.0, 10.0, 10.0],
            "entr_desac_periodo": [0.0, 0.0, 0.0],
            "saldo_estoque_periodo": [12.0, 10.0, 10.0],
            "custo_medio_periodo": [10.0, 10.0, 10.0],
            "descr_padrao": ["Produto A", "Produto A XML", "Produto A inv"],
            "co_sefin_agr": ["1001", "1001", "1001"],
            "it_pc_interna": [18.0, 18.0, 18.0],
            "Aliq_icms": [18.0, 18.0, 18.0],
            "it_pc_mva": [0.0, 0.0, 0.0],
            "it_in_st": ["N", "N", "N"],
            "it_in_mva_ajustado": ["N", "N", "N"],
            "finnfe": ["1", "1", "1"],
            "dev_simples": [False, False, False],
            "dev_venda": [False, False, False],
            "dev_compra": [False, False, False],
            "dev_ent_simples": [False, False, False],
            "excluir_estoque": [False, False, False],
        }
    )

    result = calcular_estoque_codigo_produto_dataframe(mov)

    mov_codigo = result["mov_estoque"]
    assert mov_codigo["id_agrupado"].unique().to_list() == ["P001"]
    assert mov_codigo["id_agrupado_original"].to_list() == [
        "AGR_DESCR_1",
        "AGR_DESCR_2",
        "AGR_DESCR_1",
    ]

    anual = result["aba_anual"]
    assert anual.height == 1
    row = anual.row(0, named=True)
    assert row["id_agregado"] == "P001"
    assert row["entradas"] == 12.0
    assert row["saidas"] == 2.0
    assert row["estoque_final"] == 9.0
