"""Comparacao rapida do composto vs particionamento.

Uso:
    PYTHONPATH=src python3 benchmarks/comparar_metodos_similaridade.py
"""
import time
import polars as pl

from interface_grafica.services.descricao_similarity_service import (
    ordenar_blocos_similaridade_descricao,
)
from interface_grafica.services.particionamento_fiscal import (
    ordenar_blocos_por_particionamento_fiscal,
)


def gerar_dataset(n: int) -> pl.DataFrame:
    import random
    random.seed(42)
    marcas = ["HEINEKEN", "BRAHMA", "SKOL", "ANTARTICA", "ITAIPAVA"]
    formatos = ["LATA 350ML", "LONG NECK 330ML", "GARRAFA 600ML"]
    descricoes, ncms, cests, gtins, unidades = [], [], [], [], []
    for i in range(n):
        m = random.choice(marcas)
        f = random.choice(formatos)
        descricoes.append(f"CERVEJA {m} {f}")
        ncms.append(random.choice(["22030000", "22030010", "22030090"]))
        cests.append(random.choice(["0302100", "0302101", ""]))
        gtins.append(f"789{i:09}")
        unidades.append(random.choice(["UN", "CX", ""]))
    return pl.DataFrame({
        "id_agrupado": [str(i) for i in range(n)],
        "descr_padrao": descricoes,
        "ncm_padrao": ncms,
        "cest_padrao": cests,
        "gtin_padrao": gtins,
        "unid_padrao": unidades,
    })


for n in [100, 1000, 5000]:
    df = gerar_dataset(n)
    t0 = time.perf_counter()
    out_a = ordenar_blocos_similaridade_descricao(df)
    dt_a = time.perf_counter() - t0
    t0 = time.perf_counter()
    out_b = ordenar_blocos_por_particionamento_fiscal(df)
    dt_b = time.perf_counter() - t0
    speedup = dt_a / dt_b if dt_b > 0 else float("inf")
    print(f"n={n:5d} | composto={dt_a:.2f}s | particionamento={dt_b:.2f}s | speedup={speedup:.1f}x")
