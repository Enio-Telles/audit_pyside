from pathlib import Path
import sys

import polars as pl


sys.path.insert(0, str(Path("src").resolve()))

from transformacao.fatores_conversao import calcular_fatores_conversao


def _salvar_item_unidades(pasta_prod: Path, cnpj: str, descricoes: list[str]) -> None:
    pl.DataFrame(
        {
            "descricao": descricoes,
            "unid": ["UN"] * len(descricoes),
            "compras": [10.0] * len(descricoes),
            "vendas": [0.0] * len(descricoes),
            "qtd_compras": [1.0] * len(descricoes),
            "qtd_vendas": [0.0] * len(descricoes),
        }
    ).write_parquet(pasta_prod / f"item_unidades_{cnpj}.parquet")


def test_calcular_fatores_conversao_remapeia_override_manual_por_descricao(tmp_path: Path):
    cnpj = "12345678000190"
    pasta_cnpj = tmp_path / cnpj
    pasta_prod = pasta_cnpj / "analises" / "produtos"
    pasta_prod.mkdir(parents=True, exist_ok=True)

    pl.DataFrame(
        {
            "descricao": ["WHISKY JW BLACK 12/750ML", "WHISKY JW BLACK 12/750ML"],
            "unid": ["UN", "CX"],
            "compras": [10.0, 120.0],
            "vendas": [0.0, 0.0],
            "qtd_compras": [1.0, 1.0],
            "qtd_vendas": [0.0, 0.0],
        }
    ).write_parquet(pasta_prod / f"item_unidades_{cnpj}.parquet")

    pl.DataFrame(
        {
            "id_agrupado": ["id_agrupado_8"],
            "descricao_normalizada": ["WHISKY JW BLACK 12/750ML"],
            "descricao_final": ["WHISKY JW BLACK 12/750ML"],
            "descr_padrao": ["WHISKY JW BLACK 12/750ML"],
            "unid_ref_sugerida": ["UN"],
        }
    ).write_parquet(pasta_prod / f"produtos_final_{cnpj}.parquet")

    pl.DataFrame(
        {
            "id_agrupado": ["id_agrupado_1", "id_agrupado_8"],
            "descr_padrao": ["DENVER STEAK BOV ANGUS KG", "WHISKY JW BLACK 12/750ML"],
            "lista_descricoes": [["DENVER STEAK BOV ANGUS KG"], ["WHISKY JW BLACK 12/750ML"]],
        }
    ).write_parquet(pasta_prod / f"produtos_agrupados_{cnpj}.parquet")

    pl.DataFrame(
        {
            "id_agrupado": ["id_agrupado_1"],
            "id_produtos": ["id_agrupado_1"],
            "descr_padrao": ["WHISKY JW BLACK 12/750ML"],
            "unid": ["CX"],
            "unid_ref": ["UN"],
            "fator": [12.0],
            "fator_manual": [True],
            "unid_ref_manual": [True],
            "preco_medio": [120.0],
            "origem_preco": ["COMPRA"],
        }
    ).write_parquet(pasta_prod / f"fatores_conversao_{cnpj}.parquet")

    assert calcular_fatores_conversao(cnpj, pasta_cnpj=pasta_cnpj) is True

    df_resultado = pl.read_parquet(pasta_prod / f"fatores_conversao_{cnpj}.parquet").sort(["id_agrupado", "unid"])
    df_whisky = df_resultado.filter(pl.col("id_agrupado") == "id_agrupado_8")

    assert df_whisky.height == 2
    assert df_whisky.get_column("descr_padrao").unique().to_list() == ["WHISKY JW BLACK 12/750ML"]
    assert df_whisky.filter(pl.col("unid") == "CX").row(0, named=True)["fator_manual"] is True
    assert df_whisky.filter(pl.col("unid") == "CX").row(0, named=True)["fator"] == 12.0
    assert df_whisky.get_column("unid_ref").unique().to_list() == ["UN"]

    df_log = pl.read_parquet(pasta_prod / f"log_reconciliacao_overrides_fatores_{cnpj}.parquet")
    assert df_log.filter(pl.col("acao") == "remapeado").height == 1
    assert df_log.row(0, named=True)["id_agrupado_destino"] == "id_agrupado_8"


def test_calcular_fatores_conversao_descarta_override_manual_ambiguo(tmp_path: Path):
    cnpj = "12345678000190"
    pasta_cnpj = tmp_path / cnpj
    pasta_prod = pasta_cnpj / "analises" / "produtos"
    pasta_prod.mkdir(parents=True, exist_ok=True)

    _salvar_item_unidades(pasta_prod, cnpj, ["PRODUTO A NOVO"])

    pl.DataFrame(
        {
            "id_agrupado": ["id_agrupado_10"],
            "descricao_normalizada": ["PRODUTO A NOVO"],
            "descricao_final": ["PRODUTO A NOVO"],
            "descr_padrao": ["PRODUTO A NOVO"],
            "unid_ref_sugerida": ["UN"],
        }
    ).write_parquet(pasta_prod / f"produtos_final_{cnpj}.parquet")

    pl.DataFrame(
        {
            "id_agrupado": ["id_agrupado_10", "id_agrupado_20"],
            "descr_padrao": ["PRODUTO A NOVO", "PRODUTO B NOVO"],
            "lista_descricoes": [["PRODUTO ANTIGO"], ["PRODUTO ANTIGO"]],
        }
    ).write_parquet(pasta_prod / f"produtos_agrupados_{cnpj}.parquet")

    pl.DataFrame(
        {
            "id_agrupado": ["id_agrupado_1"],
            "id_produtos": ["id_agrupado_1"],
            "descr_padrao": ["PRODUTO ANTIGO"],
            "unid": ["CX"],
            "unid_ref": ["UN"],
            "fator": [12.0],
            "fator_manual": [True],
            "unid_ref_manual": [True],
            "preco_medio": [120.0],
            "origem_preco": ["COMPRA"],
        }
    ).write_parquet(pasta_prod / f"fatores_conversao_{cnpj}.parquet")

    assert calcular_fatores_conversao(cnpj, pasta_cnpj=pasta_cnpj) is True

    df_resultado = pl.read_parquet(pasta_prod / f"fatores_conversao_{cnpj}.parquet")
    assert df_resultado.filter(pl.col("fator_manual").fill_null(False)).is_empty()
    assert df_resultado.filter(pl.col("unid_ref_manual").fill_null(False)).is_empty()

    df_log = pl.read_parquet(pasta_prod / f"log_reconciliacao_overrides_fatores_{cnpj}.parquet")
    assert df_log.filter(pl.col("acao") == "descartado").height == 1
    assert "correspondencia unica" in df_log.row(0, named=True)["motivo"]
