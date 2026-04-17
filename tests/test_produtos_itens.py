import pytest
import polars as pl
import sys
from pathlib import Path

# Add src to python path to import the module
sys.path.insert(0, str(Path("src").resolve()))

from transformacao.produtos_itens import _normalizar_descricao_expr


def test_normalizar_descricao_expr_various_cases():
    """
    Test _normalizar_descricao_expr with various input types and string conditions:
    - Happy path (normal text)
    - Diacritics (accents)
    - Multiple spaces (leading, trailing, internal)
    - Null values
    - Numeric values
    - Empty strings
    """
    df = pl.DataFrame(
        {
            "descricao": pl.Series(
                "descricao",
                [
                    "Normal Text",  # 0: Happy path
                    "Açúcar, Café & Maçã",  # 1: Diacritics
                    "  Extra   Spaces  ",  # 2: Multiple spaces
                    None,  # 3: Null value
                    12345,  # 4: Numeric value
                    "",  # 5: Empty string
                ],
                strict=False,
            )
        }
    )

    # Apply the expression
    result_df = df.with_columns(_normalizar_descricao_expr("descricao"))

    # Assert output column exists
    assert "__descricao_normalizada__" in result_df.columns

    # Fetch the results as a list
    result_list = result_df["__descricao_normalizada__"].to_list()

    # 0: Happy path -> Uppercase
    assert result_list[0] == "NORMAL TEXT"

    # 1: Diacritics -> Removed, uppercase
    assert result_list[1] == "ACUCAR, CAFE & MACA"

    # 2: Multiple spaces -> Single spaces, stripped, uppercase
    assert result_list[2] == "EXTRA SPACES"

    # 3: Null value -> Empty string
    assert result_list[3] == ""

    # 4: Numeric value -> Cast to string
    assert result_list[4] == "12345"

    # 5: Empty string -> Remains empty string
    assert result_list[5] == ""


def test_normalizar_descricao_expr_special_chars():
    """Test with special characters."""
    df = pl.DataFrame({"desc": ["C@misa! (Polo) #1"]})

    result_df = df.with_columns(_normalizar_descricao_expr("desc"))

    assert result_df["__descricao_normalizada__"].to_list()[0] == "C@MISA! (POLO) #1"
