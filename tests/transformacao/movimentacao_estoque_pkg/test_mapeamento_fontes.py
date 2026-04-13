import polars as pl
import pytest
from src.transformacao.movimentacao_estoque_pkg.mapeamento_fontes import parse_expression

def evaluate_expr(df: pl.DataFrame, expr: pl.Expr) -> pl.Series:
    """Helper function to evaluate a polars expression on a dummy dataframe."""
    return df.select(expr).to_series()

def test_parse_expression_empty_or_vazio():
    df = pl.DataFrame({"dummy": [1]})

    # Test None
    expr = parse_expression(None, "alias1")
    res = evaluate_expr(df, expr)
    assert res.to_list() == [None]
    assert res.name == "alias1"

    # Test empty string
    expr = parse_expression("", "alias2")
    res = evaluate_expr(df, expr)
    assert res.to_list() == [None]
    assert res.name == "alias2"

    # Test "(vazio)"
    expr = parse_expression("(vazio)", "alias3")
    res = evaluate_expr(df, expr)
    assert res.to_list() == [None]
    assert res.name == "alias3"

def test_parse_expression_literal_string():
    df = pl.DataFrame({"dummy": [1]})
    expr = parse_expression('"literal_value"', "alias1")
    res = evaluate_expr(df, expr)
    assert res.to_list() == ["literal_value"]
    assert res.name == "alias1"

def test_parse_expression_ncm_cest_cleanup():
    df = pl.DataFrame({"cod_ncm": ["12.345.678", "abc90", None]})
    expr = parse_expression("cod_ncm", "ncm_clean")
    res = evaluate_expr(df, expr)
    assert res.to_list() == ["12345678", "90", None]
    assert res.name == "ncm_clean"

def test_parse_expression_complex_cst():
    df = pl.DataFrame({
        "icms_orig": ["0", "1", "2"],
        "icms_cst": ["40", None, "00"],
        "icms_csosn": [None, "102", "500"]
    })
    expr = parse_expression("icms_orig & icms_cst ou icms_csosn", "cst_final")
    res = evaluate_expr(df, expr)
    assert res.to_list() == ["040", "1102", "200"]
    assert res.name == "cst_final"

def test_parse_expression_complex_cod_barra():
    df = pl.DataFrame({
        "prod_ceantrib": ["123", None, None],
        "prod_cean": ["456", "789", None]
    })
    expr = parse_expression("prod_ceantrib ou caso for nulo -> prod_cean", "cod_barra")
    res = evaluate_expr(df, expr)
    assert res.to_list() == ["123", "789", None]
    assert res.name == "cod_barra"

def test_parse_expression_math_vl_item():
    df = pl.DataFrame({
        "vl_item": [100.0, 50.0, 10.0],
        "vl_desc": [10.0, None, 2.5]
    })
    expr = parse_expression("vl_item-vl_desc", "vl_liquido")
    res = evaluate_expr(df, expr)
    assert res.to_list() == [90.0, 50.0, 7.5]
    assert res.name == "vl_liquido"

def test_parse_expression_math_prod_vprod():
    df = pl.DataFrame({
        "prod_vprod": [100.0, 50.0],
        "prod_vfrete": [10.0, None],
        "prod_vseg": [5.0, None],
        "prod_voutro": [2.0, None],
        "prod_vdesc": [7.0, 10.0]
    })
    expr = parse_expression("prod_vprod+prod_vfrete+prod_vseg+prod_voutro-prod_vdesc", "vl_total")
    res = evaluate_expr(df, expr)
    # 100 + 10 + 5 + 2 - 7 = 110.0
    # 50 + 0 + 0 + 0 - 10 = 40.0
    assert res.to_list() == [110.0, 40.0]
    assert res.name == "vl_total"

def test_parse_expression_chave_nfe():
    df = pl.DataFrame({
        "chv_nfe": ["35210112345678901234550010000000011000000018"]
    })

    expr_mod = parse_expression("correspondência com chave NF", "mod")
    res_mod = evaluate_expr(df, expr_mod)
    assert res_mod.to_list() == ["55"]

    expr_uf = parse_expression("correspondência com chave NF", "co_uf_emit")
    res_uf = evaluate_expr(df, expr_uf)
    assert res_uf.to_list() == ["35"]

    expr_dest = parse_expression("correspondência com chave NF", "co_uf_dest")
    res_dest = evaluate_expr(df, expr_dest)
    assert res_dest.to_list() == [None]

def test_parse_expression_gerado_registro():
    df = pl.DataFrame({"dummy": [1]})
    expr = parse_expression('"gerado" ou "registro" (se está no bloco_h)', "ind_registro")
    res = evaluate_expr(df, expr)
    assert res.to_list() == ["registro"]
    assert res.name == "ind_registro"

def test_parse_expression_fallback_column():
    df = pl.DataFrame({"minha_coluna": ["a", "b"]})
    expr = parse_expression("minha_coluna", "alias_col")
    res = evaluate_expr(df, expr)
    assert res.to_list() == ["a", "b"]
    assert res.name == "alias_col"
