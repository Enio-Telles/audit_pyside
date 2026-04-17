from pathlib import Path
import sys

import polars as pl

sys.path.insert(0, str(Path("src").resolve()))

import transformacao.rastreabilidade_produtos.produtos_final_v2 as produtos_mod
import transformacao.rastreabilidade_produtos.fontes_produtos as fontes_mod


def test_construir_tabela_ponte_explode_lista_codigo_fonte():
    df = pl.DataFrame(
        {
            "id_descricao": ["id_descricao_1", "id_descricao_2"],
            "id_agrupado": ["AGR_1", "AGR_1"],
            "descricao_normalizada": ["CAFE TORRADO", "CAFE TORRADO"],
            "lista_codigo_fonte": [["111|A", "111|B"], ["111|C"]],
        }
    )

    result = produtos_mod._construir_tabela_ponte(df).sort(
        ["codigo_fonte", "chave_produto"]
    )

    assert result["codigo_fonte"].to_list() == ["111|A", "111|B", "111|C"]
    assert set(result["id_agrupado"].to_list()) == {"AGR_1"}


def test_anexar_id_agrupado_prioriza_codigo_fonte_sobre_descricao():
    df_src = pl.DataFrame(
        {
            "codigo_fonte": ["111|A", "999|Z"],
            "prod_xprod": ["CAFE TORRADO", "CAFE TORRADO"],
        }
    )
    df_mapa = pl.DataFrame(
        {
            "id_agrupado": ["AGR_1", "AGR_2"],
            "codigo_fonte": ["111|A", None],
            "descricao_normalizada": ["CAFE TORRADO", "CAFE TORRADO"],
        }
    )
    df_attrs = pl.DataFrame(
        {
            "id_agrupado": ["AGR_1", "AGR_2"],
            "descr_padrao": ["CAFÉ TORRADO", "CAFÉ ESPECIAL"],
            "ncm_padrao": ["09012100", "09012100"],
            "cest_padrao": [None, None],
            "co_sefin_agr": ["123", "456"],
            "unid_ref_sugerida": ["UN", "UN"],
        }
    )

    result = fontes_mod._anexar_id_agrupado_por_codigo_ou_descricao(
        df_src=df_src,
        df_mapa=df_mapa,
        df_attrs=df_attrs,
        col_desc="prod_xprod",
    )

    assert result["id_agrupado"].to_list()[0] == "AGR_1"
    assert result["descr_padrao"].to_list()[0] == "CAFÉ TORRADO"


def test_mapa_descricao_univoca_ignora_descricoes_ambiguas():
    df_mapa = pl.DataFrame(
        {
            "id_agrupado": ["AGR_1", "AGR_2"],
            "codigo_fonte": [None, None],
            "descricao_normalizada": ["CAFE TORRADO", "CAFE TORRADO"],
        }
    )

    result = fontes_mod._construir_mapa_descricao_univoca(df_mapa)

    assert result.is_empty()
