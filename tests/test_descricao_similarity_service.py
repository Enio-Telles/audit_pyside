import polars as pl

from interface_grafica.services.descricao_similarity_service import (
    ordenar_blocos_similaridade_descricao,
)


def _df_base() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id_agrupado": ["1", "2", "3", "4"],
            "descr_padrao": [
                "CERVEJA HEINEKEN LATA 350ML",
                "ARROZ TIPO 1 5KG",
                "CERVEJA HEINEKEN 350 ML LATA",
                "CERVEJA HEINEKEN LONG NECK 330ML",
            ],
            "ncm_padrao": ["2203", "1006", "2203", "2203"],
            "cest_padrao": ["0302100", "", "0302100", "0302100"],
            "gtin_padrao": ["789000000001", "", "789000000001", "789000000099"],
        }
    )


def test_ordenacao_adiciona_indicadores_e_preserva_linhas():
    df = _df_base()
    out = ordenar_blocos_similaridade_descricao(df)

    assert out.height == df.height
    assert set(out["id_agrupado"].to_list()) == set(df["id_agrupado"].to_list())
    for col in [
        "sim_bloco",
        "sim_score",
        "sim_score_desc",
        "sim_score_ncm",
        "sim_score_cest",
        "sim_score_gtin",
        "sim_nivel",
        "sim_desc_norm",
        "sim_chave_ordem",
        "sim_desc_referencia",
    ]:
        assert col in out.columns


def test_descricoes_parecidas_com_mesmo_ncm_cest_gtin_recebem_score_alto():
    out = ordenar_blocos_similaridade_descricao(_df_base())
    cervejas_iguais = out.filter(pl.col("id_agrupado").is_in(["1", "3"]))

    assert cervejas_iguais["sim_score"].min() >= 82
    assert cervejas_iguais["sim_score_ncm"].drop_nulls().max() == 100
    assert cervejas_iguais["sim_score_cest"].drop_nulls().max() == 100
    assert cervejas_iguais["sim_score_gtin"].drop_nulls().max() == 100


def test_pode_ignorar_priorizacao_ncm_cest():
    out = ordenar_blocos_similaridade_descricao(_df_base(), usar_ncm_cest=False)

    assert out.height == 4
    assert "sim_bloco" in out.columns
