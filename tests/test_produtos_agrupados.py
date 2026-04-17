import polars as pl
import sys
from pathlib import Path

# Add src to python path to import the module
sys.path.insert(0, str(Path("src").resolve()))

from transformacao.rastreabilidade_produtos.produtos_agrupados import (
    _primeira_descricao_valida,
)
from utilitarios.text import normalize_desc as _normalizar_descricao_para_match


def test_primeira_descricao_valida_missing_column():
    """Test when the DataFrame does not have a 'descricao' column."""
    df = pl.DataFrame({"other_col": [1, 2, 3]})
    result = _primeira_descricao_valida(df)
    assert result is None


def test_primeira_descricao_valida_all_null_or_empty():
    """Test when the 'descricao' column contains only nulls, empty strings, or spaces."""
    df = pl.DataFrame({"descricao": [None, "", "   ", None]})
    result = _primeira_descricao_valida(df)
    assert result is None


def test_primeira_descricao_valida_returns_first_valid():
    """Test when there is a valid description after empty strings, it returns the first valid one."""
    df = pl.DataFrame(
        {"descricao": [None, "", "  ", "  Valid Description  ", "Another Valid"]}
    )
    result = _primeira_descricao_valida(df)
    # The function uses str.strip_chars(), which strips leading/trailing whitespace
    assert result == "Valid Description"


def test_primeira_descricao_valida_non_string_types():
    """Test when the column has non-string types, it casts to string and strips."""
    df = pl.DataFrame(
        {"descricao": pl.Series("descricao", [None, 123, "Test"], strict=False)}
    )
    result = _primeira_descricao_valida(df)
    assert result == "123"


def test_normalizar_descricao_none():
    """Testa se retorna string vazia quando o input é None."""
    assert _normalizar_descricao_para_match(None) == ""


def test_normalizar_descricao_empty():
    """Testa se retorna string vazia quando o input é string vazia."""
    assert _normalizar_descricao_para_match("") == ""
    assert _normalizar_descricao_para_match("   ") == ""


def test_normalizar_descricao_uppercase():
    """Testa se a conversão para maiúsculas funciona corretamente."""
    assert _normalizar_descricao_para_match("produto de teste") == "PRODUTO DE TESTE"


def test_normalizar_descricao_accents():
    """Testa a remoção de acentos."""
    assert _normalizar_descricao_para_match("Coração Maçã Água") == "CORACAO MACA AGUA"
    assert _normalizar_descricao_para_match("áéíóúç") == "AEIOUC"


def test_normalizar_descricao_whitespace():
    """Testa a remoção de espaços extras e nas pontas."""
    assert (
        _normalizar_descricao_para_match("  Produto   Com   Espaços  ")
        == "PRODUTO COM ESPACOS"
    )


def test_normalizar_descricao_realistic():
    """Testa um caso realista de descrição de produto."""
    input_str = " Refrigerante   Coca-cola  2L  PET   com Açúcar "
    expected_str = "REFRIGERANTE COCA COLA 2L PET COM ACUCAR"
    assert _normalizar_descricao_para_match(input_str) == expected_str
