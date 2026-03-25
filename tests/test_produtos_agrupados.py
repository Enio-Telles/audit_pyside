import pytest
import polars as pl
import sys
from pathlib import Path

# Add src to python path to import the module
sys.path.insert(0, str(Path("src").resolve()))

from transformacao.rastreabilidade_produtos.produtos_agrupados import _primeira_descricao_valida

def test_primeira_descricao_valida_missing_column():
    """Test when the DataFrame does not have a 'descricao' column."""
    df = pl.DataFrame({
        "other_col": [1, 2, 3]
    })
    result = _primeira_descricao_valida(df)
    assert result is None

def test_primeira_descricao_valida_all_null_or_empty():
    """Test when the 'descricao' column contains only nulls, empty strings, or spaces."""
    df = pl.DataFrame({
        "descricao": [None, "", "   ", None]
    })
    result = _primeira_descricao_valida(df)
    assert result is None

def test_primeira_descricao_valida_returns_first_valid():
    """Test when there is a valid description after empty strings, it returns the first valid one."""
    df = pl.DataFrame({
        "descricao": [None, "", "  ", "  Valid Description  ", "Another Valid"]
    })
    result = _primeira_descricao_valida(df)
    # The function uses str.strip_chars(), which strips leading/trailing whitespace
    assert result == "Valid Description"

def test_primeira_descricao_valida_non_string_types():
    """Test when the column has non-string types, it casts to string and strips."""
    df = pl.DataFrame({
        "descricao": pl.Series("descricao", [None, 123, "Test"], strict=False)
    })
    result = _primeira_descricao_valida(df)
    assert result == "123"
