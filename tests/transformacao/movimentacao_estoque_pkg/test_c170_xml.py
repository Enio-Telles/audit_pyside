import pytest
import polars as pl
from polars.testing import assert_frame_equal
from src.transformacao.movimentacao_estoque_pkg.c170_xml import _get_desc_similarity_expr

def test_vectorized_desc_similarity():
    df = pl.DataFrame({
        "Descr_item_c170_norm": [
            "PRODUTO TESTE",
            "MACA",
            "MACA VERDE",
            "MACA VERDE DOCE",
            "",
            "MACA",
            None,
            "MACA MACA VERDE", # dup
            "  produto   teste  ", # spaces are usually handled by norm, but let's see if extra spaces exist in list
            "BOLA AZUL",
        ],
        "Descr_item_xml_norm": [
            "PRODUTO TESTE",
            "BANANA",
            "MACA VERMELHA",
            "MACA VERMELHA",
            "",
            "",
            "MACA",
            "MACA VERMELHA VERMELHA", # dup
            "PRODUTO TESTE", # extra spaces
            "BOLA  AZUL", # extra spaces in split
        ],
    })

    res = df.with_columns(_get_desc_similarity_expr())

    expected = [
        1.0,  # Identical
        0.0,  # Completely different
        0.5,  # MACA VERDE vs MACA VERMELHA -> inter 1, max(2,2) = 2 -> 0.5
        1.0/3.0, # MACA VERDE DOCE vs MACA VERMELHA -> inter 1, max(3,2) = 3 -> 0.333
        0.0,  # Both empty
        0.0,  # One empty
        0.0,  # One null
        0.5,  # MACA MACA VERDE (MACA VERDE = len 2) vs MACA VERMELHA VERMELHA (MACA VERMELHA = len 2) -> inter 1, max(2,2) = 0.5
        0.0,  # not identical directly, and words: {produto, teste} vs {PRODUTO, TESTE} = 0. (norm handles capitalization earlier, here it's 0)
        1.0,  # BOLA AZUL vs BOLA  AZUL -> {BOLA, AZUL} vs {BOLA, AZUL} -> inter 2, max 2 -> 1.0. Even though they don't exactly match (one has double space), words match!
    ]

    for i, exp in enumerate(expected):
        assert res["desc_similarity"][i] == pytest.approx(exp, abs=0.01)
