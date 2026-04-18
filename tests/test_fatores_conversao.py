from pathlib import Path
import sys

import polars as pl

sys.path.insert(0, str(Path("src").resolve()))

from transformacao.rastreabilidade_produtos.fatores_conversao import (  # noqa: E402
    calcular_fatores_conversao,
    _carregar_vinculo_por_id_item_unid,
)


def _preparar_contexto(tmp_path: Path, cnpj: str) -> Path:
    pasta_cnpj = tmp_path / cnpj
    pasta_analises = pasta_cnpj / "analises" / "produtos"
    pasta_analises.mkdir(parents=True, exist_ok=True)

    pl.DataFrame(
        {
            "descricao": ["Produto A", "Produto A"],
            "unid": ["UN", "CX"],
            "compras": [100.0, 200.0],
            "vendas": [0.0, 0.0],
            "qtd_compras": [10.0, 5.0],
            "qtd_vendas": [0.0, 0.0],
        }
    ).write_parquet(pasta_analises / f"item_unidades_{cnpj}.parquet")

    pl.DataFrame(
        {
            "id_descricao": ["id_desc_1"],
            "descricao_normalizada": ["PRODUTO A"],
        }
    ).write_parquet(pasta_analises / f"descricao_produtos_{cnpj}.parquet")

    pl.DataFrame(
        {
            "chave_produto": ["id_desc_1"],
            "id_agrupado": ["AGR_1"],
            "codigo_fonte": ["111|001"],
            "descricao_normalizada": ["PRODUTO A"],
        }
    ).write_parquet(pasta_analises / f"map_produto_agrupado_{cnpj}.parquet")

    pl.DataFrame(
        {
            "id_agrupado": ["AGR_1"],
            "descricao_normalizada": ["PRODUTO A"],
            "descricao_final": ["Produto A"],
            "descr_padrao": ["Produto A"],
            "unid_ref_sugerida": ["UN"],
        }
    ).write_parquet(pasta_analises / f"produtos_final_{cnpj}.parquet")
    return pasta_cnpj


def test_fatores_conversao_usa_camanda_canonica_para_vincular_id(tmp_path: Path):
    cnpj = "12345678000120"
    pasta_cnpj = _preparar_contexto(tmp_path, cnpj)

    assert calcular_fatores_conversao(cnpj, pasta_cnpj=pasta_cnpj)

    df = pl.read_parquet(pasta_cnpj / "analises" / "produtos" / f"fatores_conversao_{cnpj}.parquet")
    assert set(df["id_agrupado"].to_list()) == {"AGR_1"}
    assert set(df["unid"].to_list()) == {"UN", "CX"}


def test_fatores_conversao_preserva_override_manual_e_campos_explicitos(tmp_path: Path):
    cnpj = "12345678000121"
    pasta_cnpj = _preparar_contexto(tmp_path, cnpj)
    pasta_analises = pasta_cnpj / "analises" / "produtos"

    pl.DataFrame(
        {
            "id_agrupado": ["AGR_1", "AGR_1"],
            "id_produtos": ["AGR_1", "AGR_1"],
            "descr_padrao": ["Produto A", "Produto A"],
            "unid": ["UN", "CX"],
            "unid_ref": ["UN", "UN"],
            "fator": [1.0, 12.0],
            "fator_manual": [False, True],
            "unid_ref_manual": [True, True],
            "preco_medio": [10.0, 20.0],
            "origem_preco": ["COMPRA", "COMPRA"],
        }
    ).write_parquet(pasta_analises / f"fatores_conversao_{cnpj}.parquet")

    assert calcular_fatores_conversao(cnpj, pasta_cnpj=pasta_cnpj)

    df = pl.read_parquet(pasta_analises / f"fatores_conversao_{cnpj}.parquet").sort("unid")
    row_cx = df.filter(pl.col("unid") == "CX")
    assert row_cx["fator"].to_list() == [12.0]
    assert row_cx["fator_override"].to_list() == [12.0]
    assert row_cx["fator_origem"].to_list() == ["manual"]
    assert row_cx["unid_ref_override"].to_list() == ["UN"]


def test_fator_fio_de_ouro_descricao_ambigua(tmp_path: Path):
    """Dois produtos com mesma descricao_normalizada mas codigo_fonte distintos
    mapeados para id_agrupado diferentes devem receber fator correto via chave
    física (id_item_unid), sem descarte por ambiguidade textual."""
    cnpj = "12345678000130"
    pasta_cnpj = tmp_path / cnpj
    pasta_analises = pasta_cnpj / "analises" / "produtos"
    pasta_analises.mkdir(parents=True, exist_ok=True)

    # item_unidades com id_item_unid físico
    pl.DataFrame(
        {
            "id_item_unid": ["iu_1", "iu_2"],
            "descricao": ["Agua Mineral", "Agua Mineral"],
            "unid": ["UN", "UN"],
            "compras": [50.0, 100.0],
            "vendas": [0.0, 0.0],
            "qtd_compras": [10.0, 10.0],
            "qtd_vendas": [0.0, 0.0],
        }
    ).write_parquet(pasta_analises / f"item_unidades_{cnpj}.parquet")

    # descricao_produtos mapeando cada id_item_unid para um id_descricao separado
    pl.DataFrame(
        {
            "id_descricao": ["id_desc_500ml", "id_desc_1l"],
            "descricao_normalizada": ["AGUA MINERAL", "AGUA MINERAL"],
            "lista_id_item_unid": [["iu_1"], ["iu_2"]],
        }
    ).write_parquet(pasta_analises / f"descricao_produtos_{cnpj}.parquet")

    # tabela ponte mapeando cada id_descricao para id_agrupado diferente
    pl.DataFrame(
        {
            "chave_produto": ["id_desc_500ml", "id_desc_1l"],
            "id_agrupado": ["AGUA_500ML", "AGUA_1L"],
            "codigo_fonte": ["111|A500", "111|A1L"],
            "descricao_normalizada": ["AGUA MINERAL", "AGUA MINERAL"],
        }
    ).write_parquet(pasta_analises / f"map_produto_agrupado_{cnpj}.parquet")

    pl.DataFrame(
        {
            "id_agrupado": ["AGUA_500ML", "AGUA_1L"],
            "descricao_normalizada": ["AGUA MINERAL", "AGUA MINERAL"],
            "descricao_final": ["Agua Mineral 500ml", "Agua Mineral 1L"],
            "descr_padrao": ["Agua Mineral 500ml", "Agua Mineral 1L"],
            "unid_ref_sugerida": ["UN", "UN"],
        }
    ).write_parquet(pasta_analises / f"produtos_final_{cnpj}.parquet")

    # Verificar que o vinculo por id_item_unid funciona
    df_vinculo = _carregar_vinculo_por_id_item_unid(pasta_analises, cnpj)
    assert df_vinculo.height == 2
    row_iu1 = df_vinculo.filter(pl.col("id_item_unid") == "iu_1")
    row_iu2 = df_vinculo.filter(pl.col("id_item_unid") == "iu_2")
    assert row_iu1["id_agrupado"].to_list() == ["AGUA_500ML"]
    assert row_iu2["id_agrupado"].to_list() == ["AGUA_1L"]

    # Verificar que calcular_fatores_conversao produz fatores para ambos
    assert calcular_fatores_conversao(cnpj, pasta_cnpj=pasta_cnpj)
    df = pl.read_parquet(pasta_analises / f"fatores_conversao_{cnpj}.parquet")
    ids_com_fator = set(df["id_agrupado"].to_list())
    assert "AGUA_500ML" in ids_com_fator
    assert "AGUA_1L" in ids_com_fator
