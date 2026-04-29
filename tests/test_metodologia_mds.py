import polars as pl

from src.metodologia_mds.service import MovimentacaoService


def test_derive_quantities_basic():
    df = pl.DataFrame(
        {
            "id_linha_origem": [1, 2, 3],
            "tipo_operacao": ["0 - ESTOQUE INICIAL", "1 - ENTRADA", "2 - SAIDA"],
            "quantidade_convertida": [10.0, 5.0, 3.0],
        }
    )
    res = MovimentacaoService.derive_quantities(df)
    rows = res.to_dicts()
    assert rows[0]["quantidade_fisica"] == 10.0
    assert rows[1]["quantidade_fisica"] == 5.0
    assert rows[2]["quantidade_fisica_sinalizada"] == -3.0
