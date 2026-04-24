from __future__ import annotations

import pandas as pd
import polars as pl

from utilitarios.exportar_excel_adaptado import (
    _base_config,
    _colunas_lower,
    _detectar_preset,
    _is_empty_df,
    _normalizar_objetos,
    _obter_preset_config,
    _sanitize_sheet_name,
    _serializar_listas,
    _serializar_valor,
    _to_pandas,
)


def test_is_empty_df_polars_empty() -> None:
    assert _is_empty_df(pl.DataFrame()) is True


def test_is_empty_df_polars_not_empty() -> None:
    assert _is_empty_df(pl.DataFrame({"a": [1]})) is False


def test_is_empty_df_pandas_empty() -> None:
    assert _is_empty_df(pd.DataFrame()) is True


def test_is_empty_df_pandas_not_empty() -> None:
    assert _is_empty_df(pd.DataFrame({"a": [1]})) is False


def test_to_pandas_from_polars() -> None:
    df_pl = pl.DataFrame({"x": [1, 2]})
    result = _to_pandas(df_pl)
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["x"]


def test_to_pandas_from_pandas() -> None:
    df_pd = pd.DataFrame({"y": [3, 4]})
    result = _to_pandas(df_pd)
    assert isinstance(result, pd.DataFrame)


def test_sanitize_sheet_name_normal() -> None:
    assert _sanitize_sheet_name("dados") == "dados"


def test_sanitize_sheet_name_special_chars() -> None:
    assert "*" not in _sanitize_sheet_name("test*sheet")
    assert "/" not in _sanitize_sheet_name("a/b")


def test_sanitize_sheet_name_too_long() -> None:
    long = "a" * 50
    assert len(_sanitize_sheet_name(long)) <= 31


def test_sanitize_sheet_name_empty() -> None:
    assert _sanitize_sheet_name("") == "Dados"


def test_serializar_valor_list() -> None:
    assert _serializar_valor([1, 2, 3]) == "1 | 2 | 3"


def test_serializar_valor_scalar() -> None:
    assert _serializar_valor("abc") == "abc"


def test_serializar_valor_ignores_none_in_list() -> None:
    assert _serializar_valor([1, None, 2]) == "1 | 2"


def test_serializar_listas_converts_list_cols() -> None:
    df = pd.DataFrame({"lista": [[1, 2], [3, 4]]})
    result = _serializar_listas(df)
    assert result["lista"][0] == "1 | 2"


def test_normalizar_objetos_returns_dataframe() -> None:
    df = pd.DataFrame({"s": ["a", "b"], "n": [1.0, 2.0]})
    result = _normalizar_objetos(df)
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["s", "n"]


def test_colunas_lower() -> None:
    df = pd.DataFrame({"A": [1], "B_Col": [2]})
    cols = _colunas_lower(df)
    assert "a" in cols
    assert "b_col" in cols


_EMPTY_DF = pd.DataFrame()


def test_detectar_preset_por_nome() -> None:
    presets = (
        "nfe_dados_st_xml", "nfe_evento", "reg_0200_sped", "c176_mensal_resumo",
        "c176_v2_analitico", "c176_ressarcimento", "c170_xml", "dados_cadastrais",
        "e111_ajustes", "fronteira_completo", "fronteira_resumida", "tabela_descricoes",
        "tabela_codigos", "tabela_itens_caracteristicas", "bloco_h_inventario",
        "nfce_bi_detalhe", "c170_sped", "mov_estoque",
    )
    assert all(_detectar_preset(preset, _EMPTY_DF) == preset for preset in presets)
    assert _detectar_preset("nfe", _EMPTY_DF) == "nfe_bi_detalhe"


def test_detectar_preset_generico() -> None:
    assert _detectar_preset("qualquer_coisa", _EMPTY_DF) == "generico"


def test_obter_preset_config_herda_base_e_especializa_colunas() -> None:
    base = _base_config()
    config = _obter_preset_config("tabela_descricoes")
    assert base["zoom"] == 90
    assert config["zoom"] == 85
    assert "descricao" in config["larguras_fixas"]
    assert "co_sefin_divergentes" in config["boolean_cols"]


def test_exportar_excel_cria_arquivo(tmp_path) -> None:
    from utilitarios.exportar_excel_adaptado import exportar_excel
    df = pl.DataFrame({"codigo": ["ABC"], "valor": [1.5], "descricao": ["Teste"]})
    result = exportar_excel(df, "generico", tmp_path)
    assert result is not None
    assert result.exists()
    assert result.suffix == ".xlsx"


def test_exportar_excel_retorna_none_se_vazio(tmp_path) -> None:
    from utilitarios.exportar_excel_adaptado import exportar_excel
    result = exportar_excel(pl.DataFrame(), "generico", tmp_path)
    assert result is None
