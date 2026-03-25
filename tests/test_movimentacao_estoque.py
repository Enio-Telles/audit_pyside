from pathlib import Path
import sys

import polars as pl
import pytest

sys.path.insert(0, str(Path("src/transformacao").resolve()))

from movimentacao_estoque import _calcular_saldo_estoque_anual


def test_calcular_saldo_estoque_anual_com_custo_medio_por_movimento():
    df = pl.DataFrame(
        {
            "__q_conv_sinal__": [10.0, 10.0, -5.0, -20.0],
            "q_conv": [10.0, 10.0, 5.0, 20.0],
            "preco_item": [100.0, 140.0, 999.0, 999.0],
            "Tipo_operacao": [
                "0 - ESTOQUE INICIAL",
                "1 - ENTRADA",
                "2 - SAIDAS",
                "2 - SAIDAS",
            ],
            "__qtd_decl_final_audit__": [0.0, 0.0, 0.0, 0.0],
            "finnfe": ["", "", "", ""],
            "dev_simples": [False, False, False, False],
            "dev_venda": [False, False, False, False],
            "dev_compra": [False, False, False, False],
            "dev_ent_simples": [False, False, False, False],
        }
    )

    result = _calcular_saldo_estoque_anual(df)

    assert result["saldo_estoque_anual"].to_list() == [10.0, 20.0, 15.0, 0.0]
    assert result["entr_desac_anual"].to_list() == [0.0, 0.0, 0.0, 5.0]
    assert result["custo_medio_anual"].to_list() == [10.0, 12.0, 12.0, 0.0]


def test_calcular_saldo_estoque_anual_audita_estoque_final_sem_alterar_saldo():
    df = pl.DataFrame(
        {
            "__q_conv_sinal__": [5.0, 0.0],
            "q_conv": [5.0, 0.0],
            "preco_item": [50.0, 80.0],
            "Tipo_operacao": ["1 - ENTRADA", "3 - ESTOQUE FINAL"],
            "__qtd_decl_final_audit__": [0.0, 8.0],
            "finnfe": ["", ""],
            "dev_simples": [False, False],
            "dev_venda": [False, False],
            "dev_compra": [False, False],
            "dev_ent_simples": [False, False],
        }
    )

    result = _calcular_saldo_estoque_anual(df)

    assert result["saldo_estoque_anual"].to_list() == [5.0, 5.0]
    assert result["entr_desac_anual"].to_list() == [0.0, 3.0]
    assert result["custo_medio_anual"].to_list() == [10.0, 10.0]


def test_calculos_reiniciam_por_ano():
    df = pl.DataFrame(
        {
            "id_agrupado": ["A", "A"],
            "__ano_saldo__": [2024, 2025],
            "__q_conv_sinal__": [5.0, -1.0],
            "q_conv": [5.0, 1.0],
            "preco_item": [50.0, 999.0],
            "Tipo_operacao": ["1 - ENTRADA", "2 - SAIDAS"],
            "__qtd_decl_final_audit__": [0.0, 0.0],
            "finnfe": ["", ""],
            "dev_simples": [False, False],
            "dev_venda": [False, False],
            "dev_compra": [False, False],
            "dev_ent_simples": [False, False],
        }
    )

    result = (
        df.group_by("id_agrupado", "__ano_saldo__", maintain_order=True)
        .map_groups(_calcular_saldo_estoque_anual)
        .sort("__ano_saldo__")
    )

    assert result["saldo_estoque_anual"].to_list() == [5.0, 0.0]
    assert result["entr_desac_anual"].to_list() == [0.0, 1.0]
    assert result["custo_medio_anual"].to_list() == [10.0, 0.0]


def test_devolucao_venda_recalcula_custo_medio_pelo_preco_de_retorno():
    df = pl.DataFrame(
        {
            "__q_conv_sinal__": [10.0, 2.0],
            "q_conv": [10.0, 2.0],
            "preco_item": [100.0, 40.0],
            "Tipo_operacao": ["1 - ENTRADA", "1 - ENTRADA"],
            "__qtd_decl_final_audit__": [0.0, 0.0],
            "finnfe": ["1", "4"],
            "dev_simples": [False, True],
            "dev_venda": [False, True],
            "dev_compra": [False, False],
            "dev_ent_simples": [False, False],
        }
    )

    result = _calcular_saldo_estoque_anual(df)

    assert result["saldo_estoque_anual"].to_list() == [10.0, 12.0]
    assert result["custo_medio_anual"][0] == 10.0
    assert result["custo_medio_anual"][1] == pytest.approx(140.0 / 12.0)


def test_devolucao_compra_sai_pelo_custo_medio_vigente():
    df = pl.DataFrame(
        {
            "__q_conv_sinal__": [10.0, -2.0],
            "q_conv": [10.0, 2.0],
            "preco_item": [100.0, 999.0],
            "Tipo_operacao": ["1 - ENTRADA", "2 - SAIDAS"],
            "__qtd_decl_final_audit__": [0.0, 0.0],
            "finnfe": ["1", "4"],
            "dev_simples": [False, True],
            "dev_venda": [False, False],
            "dev_compra": [False, True],
            "dev_ent_simples": [False, False],
        }
    )

    result = _calcular_saldo_estoque_anual(df)

    assert result["saldo_estoque_anual"].to_list() == [10.0, 8.0]
    assert result["entr_desac_anual"].to_list() == [0.0, 0.0]
    assert result["custo_medio_anual"].to_list() == [10.0, 10.0]


def test_inventarios_intermediarios_sao_ignorados_no_saldo_omissao_e_custo():
    df = pl.DataFrame(
        {
            "__q_conv_sinal__": [10.0, 0.0, 0.0, -2.0],
            "q_conv": [10.0, 0.0, 0.0, 2.0],
            "preco_item": [100.0, 999.0, 999.0, 999.0],
            "__preco_item_calc__": [100.0, 0.0, 0.0, 999.0],
            "Tipo_operacao": [
                "0 - ESTOQUE INICIAL",
                "0 - ESTOQUE INICIAL",
                "3 - ESTOQUE FINAL",
                "2 - SAIDAS",
            ],
            "__qtd_decl_final_audit__": [0.0, 0.0, 0.0, 0.0],
            "finnfe": ["", "", "", ""],
            "dev_simples": [False, False, False, False],
            "dev_venda": [False, False, False, False],
            "dev_compra": [False, False, False, False],
            "dev_ent_simples": [False, False, False, False],
        }
    )

    result = _calcular_saldo_estoque_anual(df)

    assert result["saldo_estoque_anual"].to_list() == [10.0, 10.0, 10.0, 8.0]
    assert result["entr_desac_anual"].to_list() == [0.0, 0.0, 0.0, 0.0]
    assert result["custo_medio_anual"].to_list() == [10.0, 10.0, 10.0, 10.0]


def test_flags_de_devolucao_em_texto_nao_quebram_o_calculo():
    df = pl.DataFrame(
        {
            "__q_conv_sinal__": [10.0, 2.0],
            "q_conv": [10.0, 2.0],
            "preco_item": [100.0, 24.0],
            "Tipo_operacao": ["1 - ENTRADA", "1 - ENTRADA"],
            "__qtd_decl_final_audit__": [0.0, 0.0],
            "finnfe": ["1", "4"],
            "dev_simples": ["false", "true"],
            "dev_venda": ["N", "S"],
            "dev_compra": ["0", "0"],
            "dev_ent_simples": ["", ""],
        }
    )

    result = _calcular_saldo_estoque_anual(df)

    assert result["saldo_estoque_anual"].to_list() == [10.0, 12.0]
    assert result["custo_medio_anual"][1] == pytest.approx(124.0 / 12.0)


def test_linha_neutralizada_nao_altera_saldo_omissao_ou_custo():
    df = pl.DataFrame(
        {
            "__q_conv_sinal__": [10.0, 0.0, -2.0],
            "q_conv": [10.0, 0.0, 2.0],
            "preco_item": [100.0, 999.0, 999.0],
            "Tipo_operacao": ["1 - ENTRADA", "1 - ENTRADA", "2 - SAIDAS"],
            "__qtd_decl_final_audit__": [0.0, 0.0, 0.0],
            "finnfe": ["1", "1", "1"],
            "dev_simples": [False, False, False],
            "dev_venda": [False, False, False],
            "dev_compra": [False, False, False],
            "dev_ent_simples": [False, False, False],
        }
    )

    result = _calcular_saldo_estoque_anual(df)

    assert result["saldo_estoque_anual"].to_list() == [10.0, 10.0, 8.0]
    assert result["entr_desac_anual"].to_list() == [0.0, 0.0, 0.0]
    assert result["custo_medio_anual"].to_list() == [10.0, 10.0, 10.0]
