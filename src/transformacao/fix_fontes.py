import os
from utilitarios.project_paths import SRC_ROOT

path = str(SRC_ROOT / 'transformacao' / 'fontes_produtos.py')
with open(path, 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace(
    'mapa_lista_chaves = (\n        pl.read_parquet(arq_agrup)\n        .select(["id_agrupado", "lista_chave_produto"])\n    )',
    'if arq_pont.exists():\n        mapa_lista_chaves = (\n            pl.read_parquet(arq_pont)\n            .group_by("id_agrupado")\n            .agg(pl.col("chave_produto").alias("lista_chave_produto"))\n        )\n    else:\n        mapa_lista_chaves = (\n            pl.read_parquet(arq_agrup)\n            .select(["id_agrupado", "lista_chave_produto"])\n        )'
)

with open(path, 'w', encoding='utf-8') as f:
    f.write(text)
print("Fix aplicado com sucesso!")
