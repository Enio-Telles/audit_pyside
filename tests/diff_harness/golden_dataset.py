"""golden_dataset.py

Gera (ou le de Parquet) dataset deterministico para testes diferenciais.

O dataset cobre as 5 chaves invariantes:
    id_agrupado, id_agregado, __qtd_decl_final_audit__, q_conv, q_conv_fisica

alem de colunas auxiliares realistas para exercitar o pipeline de transformacao.
"""
import hashlib
import random
from pathlib import Path

import polars as pl

from tests.diff_harness.invariantes import INVARIANTES_FISCAIS

_GOLDEN_DIR = Path(__file__).parent / "golden"
_GOLDEN_PATH = _GOLDEN_DIR / "golden_dataset.parquet"

INVARIANTS = INVARIANTES_FISCAIS

_UNIDADES_PARES: list[tuple[str, str, float]] = [
    ("ITEM_KG", "kg", 1.0),
    ("ITEM_UN", "un", 1.0),
    ("ITEM_CX", "cx", 12.0),
    ("ITEM_ML", "ml", 0.001),
    ("ITEM_L", "l", 1.0),
    ("ITEM_M", "m", 1.0),
]

_DESCRICOES_TEMPLATE = [
    "CAFE SOLUVEL GRANULADO 200G *",
    "SAL REFINADO IODADO  500G",
    "OLEO DE SOJA  900ML",
    "FARINHA DE TRIGO ESPECIAL  1KG",
    "ACUCAR CRISTAL BRANCO-500G",
    "LEITE INTEGRAL UHT  1L",
    "FEIJAO CARIOCA  1KG",
    "ARROZ BRANCO POLIDO 5KG",
    "MACARRAO ESPAGUETE  500G",
    "BISCOITO RECHEADO  140G",
    "MARGARINA  500G *",
    "IOGURTE NATURAL  500G",
    "QUEIJO MUSSARELA KG",
    "PRESUNTO FATIADO 200G",
    "REFRIGERANTE COLA  2L",
    "AGUA MINERAL  1.5L",
    "CERVEJA LONG NECK 355ML",
    "DETERGENTE LIQUIDO  500ML",
    "SABAO EM PO  1KG",
    "PAPEL HIGIENICO  12UN",
]


def _sha1_prefix(text: str, length: int = 12) -> str:
    return hashlib.sha1(text.encode()).hexdigest()[:length]


def _gerar_id_agrupado(descricao: str) -> str:
    return f"id_agrupado_auto_{_sha1_prefix(descricao)}"


def load_golden(seed: int = 42, n_rows: int = 100_000) -> pl.DataFrame:
    """
    Retorna dataset deterministico com as 5 colunas invariantes.

    Usa cache Parquet em golden/. O arquivo e regenerado se ausente ou se
    seed/n_rows diferirem dos valores padrao (parametros alternativos ignoram cache).
    """
    use_cache = seed == 42 and n_rows == 100_000
    if use_cache and _GOLDEN_PATH.exists():
        return pl.read_parquet(_GOLDEN_PATH)

    df = _gerar_dataset(seed=seed, n_rows=n_rows)

    if use_cache:
        _GOLDEN_DIR.mkdir(parents=True, exist_ok=True)
        df.write_parquet(_GOLDEN_PATH)

    return df


def _gerar_dataset(seed: int, n_rows: int) -> pl.DataFrame:
    import numpy as np

    rng = np.random.default_rng(seed)
    py_rng = random.Random(seed)

    n_desc = len(_DESCRICOES_TEMPLATE)
    n_unid = len(_UNIDADES_PARES)

    desc_idx = rng.integers(0, n_desc, size=n_rows)
    unid_idx = rng.integers(0, n_unid, size=n_rows)

    sufixos_variacoes = [
        "",
        " TIPO A",
        " TIPO B",
        " EMBALAGEM ECONOMICA",
        " VERSAO 2",
        "  PREMIUM",
        "*IMPORTADO*",
        " - LINHA BASICA",
    ]

    descricoes = [
        _DESCRICOES_TEMPLATE[desc_idx[i]] + py_rng.choice(sufixos_variacoes)
        for i in range(n_rows)
    ]

    id_agrupados = [_gerar_id_agrupado(d) for d in descricoes]
    id_agregados = id_agrupados[:]

    unid_pares = [_UNIDADES_PARES[unid_idx[i]] for i in range(n_rows)]
    item_codes = [par[0] for par in unid_pares]
    unidades = [par[1] for par in unid_pares]
    fatores = [par[2] for par in unid_pares]

    qtd_bruta = rng.uniform(0.01, 500.0, size=n_rows)
    fatores_arr = np.array(fatores)
    qtd_decl = qtd_bruta * rng.uniform(0.95, 1.05, size=n_rows)
    q_conv = qtd_bruta * fatores_arr
    q_conv_fisica = q_conv * rng.uniform(0.98, 1.02, size=n_rows)

    dt_doc_mask = rng.random(n_rows) < 0.85
    dt_es_mask = rng.random(n_rows) < 0.80
    anos = rng.integers(2021, 2024, size=n_rows)
    meses = rng.integers(1, 13, size=n_rows)
    dias = rng.integers(1, 29, size=n_rows)

    dt_doc = [
        f"{anos[i]}-{meses[i]:02d}-{dias[i]:02d}" if dt_doc_mask[i] else None
        for i in range(n_rows)
    ]
    dt_es = [
        f"{anos[i]}-{meses[i]:02d}-{min(dias[i] + 1, 28):02d}" if dt_es_mask[i] else None
        for i in range(n_rows)
    ]

    tipos_op = ["1 - ENTRADA", "2 - SAIDAS", "inventario"]
    tipo_op_vals = [py_rng.choice(tipos_op) for _ in range(n_rows)]

    return pl.DataFrame(
        {
            "id_agrupado": id_agrupados,
            "id_agregado": id_agregados,
            "__qtd_decl_final_audit__": qtd_decl,
            "q_conv": q_conv,
            "q_conv_fisica": q_conv_fisica,
            "descricao": descricoes,
            "id_item_unid": [f"{item_codes[i]}_{unidades[i]}" for i in range(n_rows)],
            "unid": unidades,
            "fator_conversao": fatores,
            "Dt_doc": pl.Series(dt_doc, dtype=pl.Utf8).cast(pl.Date, strict=False),
            "Dt_e_s": pl.Series(dt_es, dtype=pl.Utf8).cast(pl.Date, strict=False),
            "Tipo_operacao": tipo_op_vals,
            "qtd_bruta": qtd_bruta,
        }
    )
