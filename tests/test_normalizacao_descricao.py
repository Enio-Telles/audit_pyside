from pathlib import Path
import sys

import polars as pl

sys.path.insert(0, str(Path("src").resolve()))

from utilitarios.text import normalize_desc, expr_normalizar_descricao  # noqa: E402
from transformacao.rastreabilidade_produtos.produtos_final_v2 import gerar_produtos_final  # noqa: E402


def test_normalize_desc_aplica_apenas_regras_textuais_solicitadas():
    assert normalize_desc("  Café   Premium  ") == "CAFE PREMIUM"
    assert normalize_desc("ÁGUA   MINERAL") == "AGUA MINERAL"
    assert normalize_desc("PROD. A") == "PROD. A"
    assert normalize_desc("PROD A") == "PROD A"


def test_expr_normalizar_descricao_preserva_pontuacao():
    df = pl.DataFrame({"descricao": ["  Café   Premium  ", "PROD. A", "PROD A"]}).with_columns(
        expr_normalizar_descricao("descricao").alias("descricao_normalizada")
    )

    assert df["descricao_normalizada"].to_list() == [
        "CAFE PREMIUM",
        "PROD. A",
        "PROD A",
    ]


def test_produtos_com_descricoes_iguais_apos_normalizacao_sao_agregados(tmp_path: Path):
    cnpj = "12345678000155"
    pasta_cnpj = tmp_path / cnpj
    pasta_analises = pasta_cnpj / "analises" / "produtos"
    pasta_analises.mkdir(parents=True, exist_ok=True)

    pl.DataFrame(
        {
            "id_descricao": ["id_desc_1", "id_desc_2"],
            "descricao_normalizada": ["CAFE PREMIUM", "CAFE PREMIUM"],
            "descricao": ["  Café   Premium  ", "CAFE PREMIUM"],
            "lista_desc_compl": [["Lote 1"], ["Lote 2"]],
            "lista_codigos": [["001"], ["002"]],
            "lista_ncm": [["22083000"], ["99999999"]],
            "lista_cest": [["0300700"], ["0300700"]],
            "lista_co_sefin": [["123"], ["123"]],
            "lista_gtin": [["789"], ["790"]],
            "lista_unid": [["UN"], ["UN"]],
            "lista_codigo_fonte": [["111|001"], ["111|002"]],
            "fontes": [["nfe"], ["nfe"]],
            "lista_id_item_unid": [["IU1"], ["IU2"]],
            "lista_id_item": [["I1"], ["I2"]],
        }
    ).write_parquet(pasta_analises / f"descricao_produtos_{cnpj}.parquet")

    pl.DataFrame(
        {
            "descricao": ["  Café   Premium  ", "CAFE PREMIUM"],
            "ncm": ["22083000", "99999999"],
            "cest": ["0300700", "0300700"],
            "gtin": ["789", "790"],
            "co_sefin_item": ["123", "123"],
        }
    ).write_parquet(pasta_analises / f"item_unidades_{cnpj}.parquet")

    assert gerar_produtos_final(cnpj, pasta_cnpj=pasta_cnpj)

    df_mestra = pl.read_parquet(pasta_analises / f"produtos_agrupados_{cnpj}.parquet")
    assert df_mestra.height == 1
    assert df_mestra["qtd_descricoes_grupo"].to_list() == [2]

    df_final = pl.read_parquet(pasta_analises / f"produtos_final_{cnpj}.parquet")
    assert len(set(df_final["id_agrupado"].to_list())) == 1


def test_produtos_com_pontuacao_diferente_nao_sao_agregados_por_engano(tmp_path: Path):
    cnpj = "12345678000156"
    pasta_cnpj = tmp_path / cnpj
    pasta_analises = pasta_cnpj / "analises" / "produtos"
    pasta_analises.mkdir(parents=True, exist_ok=True)

    pl.DataFrame(
        {
            "id_descricao": ["id_desc_1", "id_desc_2"],
            "descricao_normalizada": ["PROD. A", "PROD A"],
            "descricao": ["PROD. A", "PROD A"],
            "lista_desc_compl": [["Lote 1"], ["Lote 2"]],
            "lista_codigos": [["001"], ["002"]],
            "lista_ncm": [["22083000"], ["22083000"]],
            "lista_cest": [["0300700"], ["0300700"]],
            "lista_co_sefin": [["123"], ["123"]],
            "lista_gtin": [["789"], ["790"]],
            "lista_unid": [["UN"], ["UN"]],
            "lista_codigo_fonte": [["111|001"], ["111|002"]],
            "fontes": [["nfe"], ["nfe"]],
            "lista_id_item_unid": [["IU1"], ["IU2"]],
            "lista_id_item": [["I1"], ["I2"]],
        }
    ).write_parquet(pasta_analises / f"descricao_produtos_{cnpj}.parquet")

    pl.DataFrame(
        {
            "descricao": ["PROD. A", "PROD A"],
            "ncm": ["22083000", "22083000"],
            "cest": ["0300700", "0300700"],
            "gtin": ["789", "790"],
            "co_sefin_item": ["123", "123"],
        }
    ).write_parquet(pasta_analises / f"item_unidades_{cnpj}.parquet")

    assert gerar_produtos_final(cnpj, pasta_cnpj=pasta_cnpj)

    df_mestra = pl.read_parquet(pasta_analises / f"produtos_agrupados_{cnpj}.parquet")
    assert df_mestra.height == 2
