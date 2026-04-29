from pathlib import Path
import sys

import polars as pl

PROJECT_ROOT = Path(__file__).parent.parent
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from utilitarios.codigo_fonte import expr_normalizar_codigo_fonte, normalizar_codigo_fonte


def test_expr_normalizar_codigo_fonte_matches_python_logic():
    test_cases = [
        "12345678000155|CODIGO123",
        "  12345678000155  |  CODIGO 123  ",
        "CODIGO123",
        "  CODIGO 123  ",
        "12.345.678/0001-55|CODIGO123",
        "|CODIGO123",
        "12345678000155|",
        "",
        None,
        "ABC|COD123",
        "123|",
    ]

    df = pl.DataFrame({"codigo_raw": test_cases})
    df_result = df.with_columns(
        expr_normalizar_codigo_fonte("codigo_raw", alias="codigo_normalized")
    )

    normalized_expr = df_result["codigo_normalized"].to_list()
    normalized_python = [normalizar_codigo_fonte(v) for v in test_cases]

    for i, (expr_value, python_value) in enumerate(
        zip(normalized_expr, normalized_python, strict=True)
    ):
        assert expr_value == python_value, (
            f"Mismatch at index {i}: expr={expr_value}, python={python_value} "
            f"for input='{test_cases[i]}'"
        )


if __name__ == "__main__":
    test_expr_normalizar_codigo_fonte_matches_python_logic()
    print("Test passed!")
