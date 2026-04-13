from pathlib import Path
import sys

import polars as pl

sys.path.insert(0, str(Path("src").resolve()))

from transformacao.movimentacao_estoque import _calcular_saldo_estoque_anual
from transformacao.movimentacao_estoque_pkg.calculo_saldos import gerar_eventos_estoque
from transformacao.movimentacao_estoque_pkg.movimentacao_estoque import (
    filtrar_movimentacoes_por_fonte,
    marcar_mov_rep_por_chave_item,
)


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
    assert result["entr_desac_anual"].to_list() == [0.0, 0.0]
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


def test_devolucao_venda_nao_altera_custo_medio_anual():
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
    assert result["custo_medio_anual"].to_list() == [10.0, 10.0]


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
    assert result["custo_medio_anual"].to_list() == [10.0, 10.0]


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


def test_mov_rep_e_marcado_por_chave_e_item_repetidos():
    df = pl.DataFrame(
        {
            "Chv_nfe": ["abc", "abc", "abc", "def", None],
            "Num_item": ["1", "1", "2", "1", "1"],
            "mov_rep": [None, None, None, None, None],
        }
    )

    result = marcar_mov_rep_por_chave_item(df)

    assert result["mov_rep"].to_list() == [True, True, False, False, False]


def test_filtrar_movimentacoes_por_fonte_restringe_direcao_por_origem():
    df = pl.DataFrame(
        {
            "fonte": ["c170", "c170", "nfe", "nfe", "nfce", "nfce", "bloco_h"],
            "Tipo_operacao": [
                "1 - ENTRADA",
                "2 - SAIDAS",
                "1 - ENTRADA",
                "2 - SAIDAS",
                "1 - ENTRADA",
                "2 - SAIDAS",
                "3 - ESTOQUE FINAL",
            ],
            "id_linha": [1, 2, 3, 4, 5, 6, 7],
        }
    )

    result = filtrar_movimentacoes_por_fonte(df)

    assert result["id_linha"].to_list() == [1, 4, 6, 7]
    assert result["fonte"].to_list() == ["c170", "nfe", "nfce", "bloco_h"]
    assert result["Tipo_operacao"].to_list() == [
        "1 - ENTRADA",
        "2 - SAIDAS",
        "2 - SAIDAS",
        "3 - ESTOQUE FINAL",
    ]


def test_gerar_eventos_estoque_preserva_fonte_fisica_e_rotula_inicial_derivada():
    df = pl.DataFrame(
        {
            "id_agrupado": ["AGR_1", "AGR_1"],
            "Tipo_operacao": ["1 - ENTRADA", "inventario"],
            "Dt_doc": [None, "2024-12-31"],
            "Dt_e_s": ["2024-05-10", "2024-12-31"],
            "fonte": ["nfe", "bloco_h"],
            "ncm_padrao": ["22083000", "22083000"],
            "cest_padrao": ["0300700", "0300700"],
            "descr_padrao": ["Produto A", "Produto A"],
            "Cod_item": ["0001", "0001"],
            "Cod_barra": ["", ""],
            "Ncm": ["22083000", "22083000"],
            "Cest": ["0300700", "0300700"],
            "Tipo_item": ["00", "00"],
            "Descr_item": ["Produto A", "Produto A"],
            "Cfop": ["1102", None],
            "co_sefin_agr": ["123", "123"],
            "unid_ref": ["UN", "UN"],
            "fator": [1.0, 1.0],
            "Qtd": [10.0, 12.0],
            "Vl_item": [100.0, 120.0],
            "Unid": ["UN", "UN"],
            "Ser": ["1", "1"],
        }
    ).with_columns(
        [
            pl.col("Dt_doc").str.strptime(pl.Datetime, "%Y-%m-%d", strict=False),
            pl.col("Dt_e_s").str.strptime(pl.Datetime, "%Y-%m-%d", strict=False),
        ]
    )

    result = gerar_eventos_estoque(df).sort(["Dt_doc", "Tipo_operacao"], nulls_last=True)

    assert result.filter(pl.col("Tipo_operacao") == "1 - ENTRADA")["fonte"].to_list() == ["nfe"]
    assert result.filter(pl.col("Tipo_operacao") == "3 - ESTOQUE FINAL")["fonte"].to_list() == ["bloco_h"]
    assert result.filter(pl.col("Tipo_operacao") == "0 - ESTOQUE INICIAL")["fonte"].to_list() == ["gerado"]


def test_gerar_eventos_estoque_rotula_eventos_sinteticos_como_gerado():
    df = pl.DataFrame(
        {
            "id_agrupado": ["AGR_1"],
            "Tipo_operacao": ["1 - ENTRADA"],
            "Dt_doc": ["2024-05-10"],
            "Dt_e_s": ["2024-05-10"],
            "fonte": ["nfe"],
            "ncm_padrao": ["22083000"],
            "cest_padrao": ["0300700"],
            "descr_padrao": ["Produto A"],
            "Cod_item": ["0001"],
            "Cod_barra": [""],
            "Ncm": ["22083000"],
            "Cest": ["0300700"],
            "Tipo_item": ["00"],
            "Descr_item": ["Produto A"],
            "Cfop": ["1102"],
            "co_sefin_agr": ["123"],
            "unid_ref": ["UN"],
            "fator": [1.0],
            "Qtd": [10.0],
            "Vl_item": [100.0],
            "Unid": ["UN"],
            "Ser": ["1"],
        }
    ).with_columns(
        [
            pl.col("Dt_doc").str.strptime(pl.Datetime, "%Y-%m-%d", strict=False),
            pl.col("Dt_e_s").str.strptime(pl.Datetime, "%Y-%m-%d", strict=False),
        ]
    )

    result = gerar_eventos_estoque(df)

    assert result.filter(pl.col("Tipo_operacao") == "3 - ESTOQUE FINAL gerado")["fonte"].to_list() == ["gerado"]
    fontes_iniciais = result.filter(pl.col("Tipo_operacao") == "0 - ESTOQUE INICIAL gerado")["fonte"].to_list()
    assert fontes_iniciais
    assert set(fontes_iniciais) == {"gerado"}
