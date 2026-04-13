from pathlib import Path

import polars as pl

import sys

sys.path.insert(0, str(Path("src/transformacao").resolve()))

from precos_medios_produtos_final import calcular_precos_medios_produtos_final


def test_calcular_precos_medios_produtos_final_com_log(tmp_path: Path):
    cnpj = "12345678000190"
    pasta_cnpj = tmp_path / cnpj
    pasta_prod = pasta_cnpj / "analises" / "produtos"
    pasta_prod.mkdir(parents=True, exist_ok=True)

    pl.DataFrame(
        {
            "codigo": ["1", "1"],
            "descricao": ["Produto A", "Produto A"],
            "descr_compl": [None, None],
            "tipo_item": [None, None],
            "ncm": ["1000", "1000"],
            "cest": ["2000", "2000"],
            "gtin": ["789", "789"],
            "unid": ["UN", "CX"],
            "compras": [10.0, 0.0],
            "vendas": [0.0, 30.0],
            "qtd_compras": [5.0, 0.0],
            "qtd_vendas": [0.0, 3.0],
        }
    ).write_parquet(pasta_prod / f"item_unidades_{cnpj}.parquet")

    pl.DataFrame(
        {
            "chave_produto": ["id_produto_1"],
            "descricao_normalizada": ["PRODUTO A"],
        }
    ).write_parquet(pasta_prod / f"produtos_{cnpj}.parquet")

    pl.DataFrame(
        {
            "id_agrupado": ["id_agrupado_1"],
            "descr_padrao": ["Produto A"],
            "lista_chave_produto": [["id_produto_1"]],
        }
    ).write_parquet(pasta_prod / f"produtos_agrupados_{cnpj}.parquet")

    pl.DataFrame(
        {
            "id_agrupado": ["id_agrupado_1"],
            "descricao_normalizada": ["PRODUTO A"],
            "descr_padrao": ["Produto A"],
        }
    ).write_parquet(pasta_prod / f"produtos_final_{cnpj}.parquet")

    df_precos, df_sem_compra = calcular_precos_medios_produtos_final(cnpj, pasta_cnpj=pasta_cnpj, salvar_logs=True)

    assert df_precos.height == 2
    assert set(df_precos["origem_preco"].to_list()) == {"COMPRA", "VENDA"}
    assert df_sem_compra.height == 1
    assert df_sem_compra["tem_preco_venda"][0] is True
    assert (pasta_prod / f"log_sem_preco_medio_compra_{cnpj}.parquet").exists()
    assert (pasta_prod / f"log_sem_preco_medio_compra_{cnpj}.json").exists()
