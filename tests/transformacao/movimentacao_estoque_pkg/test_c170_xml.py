import pytest
from src.transformacao.movimentacao_estoque_pkg.c170_xml import _desc_similarity


def test_desc_similarity_identical():
    payload = {"a": "PRODUTO TESTE", "b": "PRODUTO TESTE"}
    assert _desc_similarity(payload) == 1.0


def test_desc_similarity_identical_different_casing_spacing():
    # _norm_text handles casing and extra spacing
    payload = {"a": "  produto   teste  ", "b": "PRODUTO TESTE"}
    assert _desc_similarity(payload) == 1.0


def test_desc_similarity_identical_with_accents():
    # _norm_text handles accents
    payload = {"a": "pródutõ téstè", "b": "PRODUTO TESTE"}
    assert _desc_similarity(payload) == 1.0


def test_desc_similarity_completely_different():
    payload = {"a": "MACA", "b": "BANANA"}
    assert _desc_similarity(payload) == 0.0


def test_desc_similarity_partially_matching():
    # ta = {"MACA", "VERDE"} -> len 2
    # tb = {"MACA", "VERMELHA"} -> len 2
    # inter = {"MACA"} -> len 1
    # denom = max(2, 2, 1) = 2
    # expected = 1 / 2 = 0.5
    payload = {"a": "MACA VERDE", "b": "MACA VERMELHA"}
    assert _desc_similarity(payload) == 0.5


def test_desc_similarity_partially_matching_different_lengths():
    # ta = {"MACA", "VERDE", "DOCE"} -> len 3
    # tb = {"MACA", "VERMELHA"} -> len 2
    # inter = {"MACA"} -> len 1
    # denom = max(3, 2, 1) = 3
    # expected = 1 / 3
    payload = {"a": "MACA VERDE DOCE", "b": "MACA VERMELHA"}
    assert _desc_similarity(payload) == pytest.approx(1 / 3)


def test_desc_similarity_empty_payload():
    payload = {}
    assert _desc_similarity(payload) == 0.0


def test_desc_similarity_missing_keys():
    payload = {"a": "MACA"}
    assert _desc_similarity(payload) == 0.0


def test_desc_similarity_empty_strings():
    payload = {"a": "", "b": ""}
    assert _desc_similarity(payload) == 0.0


def test_desc_similarity_only_spaces():
    payload = {"a": "   ", "b": " "}
    assert _desc_similarity(payload) == 0.0


def test_desc_similarity_one_empty():
    payload = {"a": "MACA", "b": ""}
    assert _desc_similarity(payload) == 0.0
