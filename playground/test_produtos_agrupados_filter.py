import sys
import os
import shutil
import importlib
import polars as pl
from pathlib import Path

# Add src to sys.path
sys.path.insert(0, os.path.abspath("src"))
from transformacao.rastreabilidade_produtos.produtos_agrupados import (
    inicializar_produtos_agrupados,
)

CNPJ = "00000000000000"
test_dir = Path("/tmp/test_agrupados")
if test_dir.exists():
    shutil.rmtree(test_dir)
test_dir.mkdir(parents=True)

pasta_analises = test_dir / "analises" / "produtos"
pasta_analises.mkdir(parents=True)

pl.DataFrame(
    {
        "chave_produto": ["P1", "P2"],
        "descricao_normalizada": ["PROD A", "PROD B"],
        "lista_gtin": [["111"], ["222"]],
        "lista_ncm": [["1111"], ["2222"]],
        "lista_cest": [["1111111"], ["2222222"]],
        "lista_co_sefin": [["S1"], ["S2"]],
        "lista_unid": [["UN"], ["KG"]],
        "descricao": ["Prod A", "Prod B"],
        "lista_desc_compl": [["A compl"], ["B compl"]],
    }
).write_parquet(pasta_analises / f"produtos_{CNPJ}.parquet")

pl.DataFrame(
    {
        "chave_item": ["I1", "I2"],
        "descricao": ["Prod A", "Prod B"],
        "ncm": ["1111", "2222"],
        "cest": ["1111111", "2222222"],
        "gtin": ["111", "222"],
        "co_sefin_item": ["S1", "S2"],
    }
).write_parquet(pasta_analises / f"produtos_unidades_{CNPJ}.parquet")

# 04_produtos_final
pl.DataFrame(
    {
        "id_descricao": ["D1", "D2"],
        "descricao_normalizada": ["PROD A", "PROD B"],
        "descricao": ["Prod A", "Prod B"],
        "lista_co_sefin": [["S1"], ["S2"]],
        "lista_unid": [["UN"], ["KG"]],
        "fontes": [["F1"], ["F2"]],
        "lista_ncm": [["1111"], ["2222"]],
        "lista_cest": [["1111111"], ["2222222"]],
        "lista_gtin": [["111"], ["222"]],
        "lista_desc_compl": [["A compl"], ["B compl"]],
        "lista_codigos": [["C1"], ["C2"]],
        "lista_tipo_item": [["T1"], ["T2"]],
    }
).write_parquet(pasta_analises / f"descricao_produtos_{CNPJ}.parquet")

pl.DataFrame(
    {
        "id_item_unid": ["IU1", "IU2"],
        "descricao": ["Prod A", "Prod B"],
        "ncm": ["1111", "2222"],
        "cest": ["1111111", "2222222"],
        "gtin": ["111", "222"],
        "co_sefin_item": ["S1", "S2"],
    }
).write_parquet(pasta_analises / f"item_unidades_{CNPJ}.parquet")

res1 = inicializar_produtos_agrupados(CNPJ, test_dir)
print(f"inicializar_produtos_agrupados: {res1}")

mod = importlib.import_module(
    "transformacao.rastreabilidade_produtos.04_produtos_final"
)
res2 = mod.produtos_agrupados(CNPJ, test_dir)
print(f"produtos_agrupados (04): {res2}")
