import polars as pl

from interface_grafica.services.descricao_similarity_service import (
    ordenar_blocos_similaridade_descricao,
)
from utilitarios.unidades_descricao import normalizar_unidades_em_texto


def test_litros_para_ml():
    assert normalizar_unidades_em_texto("0,5L") == "500ML"
    assert normalizar_unidades_em_texto("1,5 L") == "1500ML"
    assert normalizar_unidades_em_texto("2 LITROS") == "2000ML"
    assert normalizar_unidades_em_texto("1 LT") == "1000ML"


def test_quilogramas_para_g():
    assert normalizar_unidades_em_texto("1KG") == "1000G"
    assert normalizar_unidades_em_texto("0,5 KG") == "500G"
    assert normalizar_unidades_em_texto("2 QUILOS") == "2000G"
    assert normalizar_unidades_em_texto("1.5 KG") == "1500G"


def test_ml_e_g_sao_normalizados_no_formato():
    assert normalizar_unidades_em_texto("350 ML") == "350ML"
    assert normalizar_unidades_em_texto("100 G") == "100G"
    assert normalizar_unidades_em_texto("100 GRAMAS") == "100G"
    assert normalizar_unidades_em_texto("100GR") == "100G"


def test_caso_misto_em_uma_descricao():
    entrada = "OLEO DE SOJA 0,9L CAIXA COM 12 UN"
    saida = normalizar_unidades_em_texto(entrada)
    assert "900ML" in saida


def test_texto_vazio_e_nulo():
    assert normalizar_unidades_em_texto("") == ""
    assert normalizar_unidades_em_texto(None) is None


def test_descricoes_equivalentes_em_unidades_diferentes_geram_score_alto():
    """350ML e 0,35L devem dar match no sim_score_numeros."""
    df = pl.DataFrame(
        {
            "id_agrupado": ["1", "2"],
            "descr_padrao": [
                "REFRIGERANTE COCA-COLA 350ML",
                "REFRIGERANTE COCA-COLA 0,35L",
            ],
            "ncm_padrao": ["22021000", "22021000"],
            "cest_padrao": ["", ""],
            "gtin_padrao": ["", ""],
        }
    )
    out = ordenar_blocos_similaridade_descricao(df)
    assert out["sim_score_numeros"].drop_nulls().to_list() == [100, 100]
    assert out["sim_motivos"].str.contains("NUMEROS_IGUAIS").any()


def test_quilo_e_grama_equivalem():
    df = pl.DataFrame(
        {
            "id_agrupado": ["1", "2"],
            "descr_padrao": [
                "ARROZ TIPO 1 1KG",
                "ARROZ TIPO 1 1000G",
            ],
            "ncm_padrao": ["10063021", "10063021"],
            "cest_padrao": ["", ""],
            "gtin_padrao": ["", ""],
        }
    )
    out = ordenar_blocos_similaridade_descricao(df)
    assert out["sim_score_numeros"].drop_nulls().to_list() == [100, 100]
