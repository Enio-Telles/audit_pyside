from __future__ import annotations
from pathlib import Path
import sys
import pytest

# Add src to sys.path
sys.path.insert(0, str(Path("src").resolve()))

from transformacao.movimentacao_estoque_pkg.mapeamento_fontes import (
    _norm,
    detectar_coluna_descricao,
    detectar_coluna_unidade
)

# Mocking polars DataFrame for testing if polars is not available
class MockDataFrame:
    def __init__(self, columns):
        self.columns = columns

def test_norm_basic():
    assert _norm("  produto   teste  ") == "PRODUTO TESTE"
    assert _norm("Café com Açúcar") == "CAFE COM ACUCAR"
    assert _norm(None) == ""
    assert _norm("   ") == ""

def test_detectar_coluna_descricao_c170():
    # candidatos = ["descr_item", "descricao", "prod_xprod"]
    df = MockDataFrame(["descr_item", "outra_coluna"])
    assert detectar_coluna_descricao(df, "c170") == "descr_item"

    df2 = MockDataFrame(["descricao", "prod_xprod"])
    assert detectar_coluna_descricao(df2, "c170") == "descricao"

def test_detectar_coluna_descricao_bloco_h():
    # candidatos = ["descricao_produto", "descr_item", "descricao", "prod_xprod"]
    df = MockDataFrame(["descricao_produto", "descr_item"])
    assert detectar_coluna_descricao(df, "bloco_h") == "descricao_produto"

    df2 = MockDataFrame(["descr_item", "descricao"])
    assert detectar_coluna_descricao(df2, "bloco_h") == "descr_item"

def test_detectar_coluna_descricao_nfe_nfce():
    # candidatos = ["prod_xprod", "descricao", "descr_item"]
    df = MockDataFrame(["prod_xprod", "descricao"])
    assert detectar_coluna_descricao(df, "nfe") == "prod_xprod"
    assert detectar_coluna_descricao(df, "nfce") == "prod_xprod"

    df2 = MockDataFrame(["descricao", "descr_item"])
    assert detectar_coluna_descricao(df2, "nfe") == "descricao"

def test_detectar_coluna_descricao_not_found():
    df = MockDataFrame(["coluna_aleatoria"])
    assert detectar_coluna_descricao(df, "c170") is None
    assert detectar_coluna_descricao(df, "desconhecido") is None

def test_detectar_coluna_unidade_c170():
    # candidatos = ["unid"]
    df = MockDataFrame(["unid"])
    assert detectar_coluna_unidade(df, "c170") == "unid"

def test_detectar_coluna_unidade_bloco_h():
    # candidatos = ["unidade_medida", "unidade_media", "unid", "unidade"]
    df = MockDataFrame(["unidade_medida"])
    assert detectar_coluna_unidade(df, "bloco_h") == "unidade_medida"

    df2 = MockDataFrame(["unid"])
    assert detectar_coluna_unidade(df2, "bloco_h") == "unid"

def test_detectar_coluna_unidade_nfe_nfce():
    # candidatos = ["prod_ucom"]
    df = MockDataFrame(["prod_ucom"])
    assert detectar_coluna_unidade(df, "nfe") == "prod_ucom"
    assert detectar_coluna_unidade(df, "nfce") == "prod_ucom"

def test_detectar_coluna_unidade_not_found():
    df = MockDataFrame(["coluna_aleatoria"])
    assert detectar_coluna_unidade(df, "nfe") is None

try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

@pytest.mark.skipif(not HAS_POLARS, reason="Polars not installed")
def test_detectar_coluna_descricao_real_df():
    df = pl.DataFrame({"descr_item": ["A"], "vazio": [None]})
    assert detectar_coluna_descricao(df, "c170") == "descr_item"

@pytest.mark.skipif(not HAS_POLARS, reason="Polars not installed")
def test_norm_via_expr():
    from transformacao.movimentacao_estoque_pkg.mapeamento_fontes import normalizar_descricao_expr
    df = pl.DataFrame({"desc": [" café  com LEITE "]})
    res = df.select(normalizar_descricao_expr("desc"))
    assert res.columns == ["__descricao_normalizada__"]
    assert res["__descricao_normalizada__"][0] == "CAFE COM LEITE"
