import polars as pl

from src.metodologia_mds.service import MovimentacaoService


def test_apply_conversion_uses_fator_when_no_override():
    df = pl.DataFrame(
        {
            "id_agrupado": ["A"],
            "Qtd": [10.0],
            "fator": [0.5],
            "unid_ref": ["L"],
        }
    )
    res = MovimentacaoService.apply_conversion_factors(df)
    r = res.to_dicts()[0]
    assert r["fator_conversao"] == 0.5
    assert r["fator_original"] == 0.5
    assert r["unidade_referencia"] == "L"


def test_apply_conversion_prefers_override_and_marks_origin():
    df = pl.DataFrame(
        {
            "id_agrupado": ["B"],
            "Qtd": [2.0],
            "fator": [0.5],
            "fator_conversao_override": [2.0],
            "unid_ref": ["kg"],
        }
    )
    res = MovimentacaoService.apply_conversion_factors(df)
    r = res.to_dicts()[0]
    assert r["fator_conversao"] == 2.0
    assert r["fator_original"] == 0.5
    assert r["fator_conversao_origem"] == "manual"
    assert r["fator"] == 2.0
