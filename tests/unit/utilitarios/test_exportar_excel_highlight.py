from __future__ import annotations

from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from utilitarios.exportar_excel_adaptado import (
    _detectar_preset,
    _to_pandas,
    exportar_excel,
)


def _out(tmp_path: Path, name: str) -> Path:
    return tmp_path / f"{name}.xlsx"


def test_exportar_tabela_descricoes_boolean_true_rule(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {
            "descricao": ["Produto A", "Produto B"],
            "co_sefin_divergentes": [True, False],
            "codigo": ["001", "002"],
        }
    )
    result = exportar_excel(df, "tabela_descricoes", tmp_path)
    assert result is not None and result.exists()


def test_exportar_tabela_codigos_greater_than_rule(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {
            "cod_normalizado": ["ABC", "DEF"],
            "qtd_descr": [3, 1],
            "lista_descricao": [["d1", "d2", "d3"], ["d1"]],
        }
    )
    result = exportar_excel(df, "tabela_codigos", tmp_path)
    assert result is not None and result.exists()


def test_exportar_bloco_h_inventario_not_blank_rule(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {
            "codigo_produto": ["P001", "P002"],
            "descricao_produto": ["Item A", "Item B"],
            "dt_inv": ["2025-12-31", "2025-12-31"],
            "participante_terceiro": ["CNPJ001", None],
            "ano": ["2025", "2025"],
        }
    )
    result = exportar_excel(df, "bloco_h_inventario", tmp_path)
    assert result is not None and result.exists()


def test_exportar_c176_mensal_resumo_not_equal_zero_rule(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {
            "cnpj": ["12345678000190"],
            "periodo_efd": ["012025"],
            "diferenca_credito_proprio": [500.0],
            "diferenca_st_retido": [0.0],
            "qtd_itens_analisados_c176": [10],
        }
    )
    result = exportar_excel(df, "c176_mensal_resumo", tmp_path)
    assert result is not None and result.exists()


def test_exportar_fronteira_completo_compare_columns_and_equals_rules(
    tmp_path: Path,
) -> None:
    df = pl.DataFrame(
        {
            "chave": ["ABC123"],
            "nota": ["001"],
            "cnpj_emit": ["12345678000190"],
            "prod_nitem": ["1"],
            "valor_devido": [100.0],
            "valor_pago": [80.0],
            "situação": ["VERIFICAR"],
        }
    )
    result = exportar_excel(df, "fronteira_completo", tmp_path)
    assert result is not None and result.exists()


def test_exportar_com_coluna_ano_aciona_formato_ano(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {
            "ano": ["2025", "2024"],
            "codigo": ["A001", "A002"],
            "valor": [100.0, 200.0],
        }
    )
    result = exportar_excel(df, "generico", tmp_path)
    assert result is not None and result.exists()


def test_exportar_com_coluna_datetime_aciona_formato_datetime(tmp_path: Path) -> None:
    df_pd = pd.DataFrame(
        {
            "dh_emissao": pd.to_datetime(["2025-01-15 10:30:00", "2025-02-20 14:00:00"]),
            "valor": [1.0, 2.0],
        }
    )
    result = exportar_excel(df_pd, "generico", tmp_path)
    assert result is not None and result.exists()


def test_exportar_dados_cadastrais_url_cols(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {
            "cnpj": ["12345678000190"],
            "ie": ["123456789"],
            "nome": ["Empresa X"],
            "situação da ie": ["ATIVA"],
            "redesim": ["https://redesim.gov.br/12345678000190"],
        }
    )
    result = exportar_excel(df, "dados_cadastrais", tmp_path)
    assert result is not None and result.exists()


# ---------------------------------------------------------------------------
# _detectar_preset — detecção por colunas (18 branches)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cols,expected",
    [
        (["descricao", "lista_chave_item_individualizado", "lista_cod_normalizado"], "tabela_descricoes"),
        (["cod_normalizado", "lista_descricao", "qtd_descr"], "tabela_codigos"),
        (["chave_item_individualizado", "codigo", "descricao", "lista_unidades", "fonte"], "tabela_itens_caracteristicas"),
        (["periodo_efd", "chv_nfe", "num_item", "cod_item", "descr_item"], "c170_sped"),
        (["tipo_operacao", "chv_nfe", "num_item", "cod_item", "id_agrupado", "score_vinculo_xml"], "c170_xml"),
        (["dt_inv", "codigo_produto", "descricao_produto", "valor_total_inventario_h005"], "bloco_h_inventario"),
        (["tipo_operacao", "chave_acesso", "prod_nitem", "prod_cprod", "prod_xprod", "co_indpres"], "nfce_bi_detalhe"),
        (["tipo_operacao", "chave_acesso", "prod_nitem", "prod_cprod", "prod_xprod"], "nfe_bi_detalhe"),
        (["chave_acesso", "prod_nitem", "prod_cprod", "icms_vbcst", "icms_vicmsst"], "nfe_dados_st_xml"),
        (["chave_acesso", "nsu_evento", "evento_dhevento", "evento_tpevento"], "nfe_evento"),
        (["periodo_efd", "cod_item", "descr_item", "cod_ncm", "tipo_item"], "reg_0200_sped"),
        (["periodo_efd", "chave_saida", "cod_mot_res", "vl_ressarc_st_retido"], "c176_ressarcimento"),
        (["periodo_efd", "qtd_itens_analisados_c176", "diferenca_credito_proprio", "diferenca_st_retido"], "c176_mensal_resumo"),
        (["periodo_efd", "chv_nfe", "descr_item", "descricao_cst_icms", "vl_icms_st"], "c176_v2_analitico"),
        (["cnpj", "ie", "nome", "situação da ie"], "dados_cadastrais"),
        (["periodo_efd", "codigo_ajuste", "valor_ajuste", "descricao_codigo_ajuste"], "e111_ajustes"),
        (["tipo_operacao", "chave_acesso", "num_item", "cod_item", "valor_icms_fronteira"], "fronteira_resumida"),
        (["chave", "nota", "cnpj_emit", "prod_nitem", "valor_devido", "valor_pago", "situação"], "fronteira_completo"),
    ],
)
def test_detectar_preset_por_colunas(cols: list[str], expected: str) -> None:
    df = pd.DataFrame({c: [] for c in cols})
    assert _detectar_preset("dataset_xyz", df) == expected


# ---------------------------------------------------------------------------
# _to_pandas — lista de dicts (line 55)
# ---------------------------------------------------------------------------


def test_to_pandas_com_lista() -> None:
    df = _to_pandas([{"a": 1, "b": 2}, {"a": 3, "b": 4}])
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["a", "b"]


# ---------------------------------------------------------------------------
# _escolher_formato — boolean_cols (c170_xml/match_xml, line 1748)
# ---------------------------------------------------------------------------


def test_exportar_c170_xml_boolean_match_xml(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {
            "tipo_operacao": ["E"],
            "chv_nfe": ["abc123"],
            "num_item": ["1"],
            "cod_item": ["X001"],
            "id_agrupado": ["A001"],
            "score_vinculo_xml": [0.95],
            "match_xml": [True],
        }
    )
    result = exportar_excel(df, "c170_xml", tmp_path)
    assert result is not None and result.exists()


# ---------------------------------------------------------------------------
# _escolher_formato — int dtype (1755-56), padrao (1757), date auto-width (1965)
# ---------------------------------------------------------------------------


def test_exportar_mov_estoque_varios_tipos(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {
            "dt_doc": ["2025-01-01"],
            "id_agrupado": ["A001"],
            "chv_nfe": ["abc"],
            "valor": [10.0],
            "qtd": [2.0],
            "qtd_pecas": [3],          # int dtype → linha 1755-56
            "status_lote": ["OK"],     # str → linha 1757
        }
    )
    result = exportar_excel(df, "mov_estoque", tmp_path)
    assert result is not None and result.exists()


# ---------------------------------------------------------------------------
# _aplicar_links_url — continue quando coluna URL ausente (line 1879)
# ---------------------------------------------------------------------------


def test_exportar_dados_cadastrais_sem_redesim(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {
            "cnpj": ["12345678000190"],
            "ie": ["123456789"],
            "nome": ["Empresa X"],
            "situação da ie": ["ATIVA"],
            # sem coluna redesim
        }
    )
    result = exportar_excel(df, "dados_cadastrais", tmp_path)
    assert result is not None and result.exists()


# ---------------------------------------------------------------------------
# _aplicar_condicional — continue em compare_columns (1776) e col ausente (1796)
# ---------------------------------------------------------------------------


def test_exportar_com_preset_fronteira_sem_colunas_highlight(tmp_path: Path) -> None:
    df = pl.DataFrame({"id": ["A"], "valor": [1.0]})
    result = exportar_excel(df, "x", tmp_path, preset="fronteira_completo")
    assert result is not None and result.exists()
