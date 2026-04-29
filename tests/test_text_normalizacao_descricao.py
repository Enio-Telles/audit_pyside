import polars as pl

from utilitarios.text import expr_normalizar_descricao, normalize_desc


def test_normalize_desc_preserva_pontuacao_canonica():
    texto = "  Água   mineral 500ml - %$#@!.,}{][\\/;  "
    assert normalize_desc(texto) == "AGUA MINERAL 500ML - %$#@!.,}{][\\/;"


def test_normalize_desc_remove_acentos_e_reduz_espacos():
    assert normalize_desc("PÃO   DE   AÇÚCAR") == "PAO DE ACUCAR"


def test_normalize_desc_nao_remove_stopwords():
    assert normalize_desc("OLEO DE SOJA") == "OLEO DE SOJA"


def test_expr_normalizar_descricao_equivale_normalize_desc():
    texto = "Água   mineral 500ml - %$#@!.,}{][\\/;"
    df = pl.DataFrame({"descricao": [texto]})

    out = df.with_columns(expr_normalizar_descricao("descricao").alias("norm"))

    assert out["norm"][0] == normalize_desc(texto)
