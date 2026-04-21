from pathlib import Path
import sys

import polars as pl

sys.path.insert(0, str(Path("src").resolve()))

from transformacao.rastreabilidade_produtos.produtos_final_v2 import gerar_produtos_final  # noqa: E402


def _criar_bases(tmp_path: Path, cnpj: str) -> Path:
    pasta_cnpj = tmp_path / cnpj
    pasta_analises = pasta_cnpj / "analises" / "produtos"
    pasta_analises.mkdir(parents=True, exist_ok=True)

    pl.DataFrame(
        {
            "id_descricao": ["id_desc_1", "id_desc_2"],
            "descricao_normalizada": ["PRODUTO A", "PRODUTO B"],
            "descricao": ["Produto A", "Produto B"],
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
            "descricao": ["Produto A", "Produto B"],
            "ncm": ["22083000", "22083000"],
            "cest": ["0300700", "0300700"],
            "gtin": ["789", "790"],
            "co_sefin_item": ["123", "123"],
        }
    ).write_parquet(pasta_analises / f"item_unidades_{cnpj}.parquet")

    return pasta_cnpj


def test_agrupamento_automatico_gera_id_deterministico(tmp_path: Path):
    cnpj = "12345678000100"
    pasta_cnpj = _criar_bases(tmp_path, cnpj)

    assert gerar_produtos_final(cnpj, pasta_cnpj=pasta_cnpj)

    pasta_analises = pasta_cnpj / "analises" / "produtos"
    df_final = pl.read_parquet(pasta_analises / f"produtos_final_{cnpj}.parquet")

    ids = df_final["id_agrupado"].to_list()
    assert len(ids) == 2
    assert all(v.startswith("id_agrupado_auto_") for v in ids)
    assert df_final["criterio_agrupamento"].to_list() == [
        "automatico_descricao_normalizada",
        "automatico_descricao_normalizada",
    ]


def test_mapa_manual_por_descricao_normalizada_tem_precedencia(tmp_path: Path):
    cnpj = "12345678000101"
    pasta_cnpj = _criar_bases(tmp_path, cnpj)
    pasta_analises = pasta_cnpj / "analises" / "produtos"

    pl.DataFrame(
        {
            "descricao_normalizada": ["PRODUTO A", "PRODUTO B"],
            "id_agrupado": ["AGR_MANUAL_001", "AGR_MANUAL_001"],
        }
    ).write_parquet(pasta_analises / f"mapa_agrupamento_manual_{cnpj}.parquet")

    assert gerar_produtos_final(cnpj, pasta_cnpj=pasta_cnpj)

    df_mestra = pl.read_parquet(pasta_analises / f"produtos_agrupados_{cnpj}.parquet")
    assert df_mestra.height == 1
    assert df_mestra["id_agrupado"].to_list() == ["AGR_MANUAL_001"]
    assert df_mestra["criterio_agrupamento"].to_list() == ["manual"]
    assert df_mestra["qtd_descricoes_grupo"].to_list() == [2]
    assert len(df_mestra["ids_origem_agrupamento"].to_list()[0]) == 2
