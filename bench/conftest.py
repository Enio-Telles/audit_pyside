"""
Fixtures deterministicas para benchmarks.

Seeds fixas garantem reproducibilidade entre execucoes e maquinas.
Os arquivos .parquet sao gerados on-demand em bench/data/ e nao sao commitados.
"""
from __future__ import annotations

import random
from pathlib import Path

import polars as pl
import pytest

_DATA_DIR = Path(__file__).parent / "data"
_RNG_SEED = 42


def _make_rng(seed: int = _RNG_SEED) -> random.Random:
    return random.Random(seed)


def _gerar_descricoes(n: int, seed: int = _RNG_SEED) -> list[str]:
    rng = _make_rng(seed)
    sufixos = ["KG", "UN", "CX", "ML", "L", "M", "PCT", "FD", "GL", "SC"]
    prefixos = ["PROD", "ITEM", "ART", "MED", "ALI", "BEB", "HIG", "LIM", "EMP", "IMP"]
    palavras_extras = [
        "NATURAL", "ORGANICO", "LIGHT", "DIET", "PREMIUM", "BASICO",
        "INDUSTRIAL", "COMERCIAL", "ESPECIAL", "EXTRA",
    ]
    descricoes: list[str] = []
    for _ in range(n):
        pref = rng.choice(prefixos)
        num = rng.randint(1, 9999)
        suf = rng.choice(sufixos)
        extra = rng.choice(palavras_extras) if rng.random() < 0.4 else ""
        acento = rng.choice(["CAFe", "SALao", "PReSTACAO", ""]) if rng.random() < 0.2 else ""
        hifens = "-" if rng.random() < 0.3 else ""
        asterisco = "*" if rng.random() < 0.2 else ""
        espacos = "   " if rng.random() < 0.15 else " "
        partes = [p for p in [pref, str(num), extra, acento, suf, asterisco, hifens] if p]
        descricoes.append(espacos.join(partes))
    return descricoes


def _gerar_movimentacao_sintetica(
    n_linhas: int = 50_000,
    n_itens: int = 1_000,
    n_meses: int = 12,
    seed: int = _RNG_SEED,
) -> pl.DataFrame:
    """DataFrame sintetico compativel com gerar_eventos_estoque / calcular_saldo_estoque_anual."""
    import numpy as np

    rng = np.random.default_rng(seed)
    py_rng = _make_rng(seed)

    tipos_op = ["1 - ENTRADA", "2 - SAIDAS", "inventario"]
    unids = ["kg", "un", "cx", "ml", "l", "m"]

    ids_agrupados = [f"id_agrupado_auto_{i:012x}" for i in range(n_itens)]
    cods_item = [f"COD{i:05d}" for i in range(n_itens)]

    idx_item = rng.integers(0, n_itens, size=n_linhas)
    meses = rng.integers(1, n_meses + 1, size=n_linhas)
    dias = rng.integers(1, 29, size=n_linhas)
    anos = rng.integers(2021, 2024, size=n_linhas)

    tipo_op_idx = rng.integers(0, len(tipos_op), size=n_linhas)
    tipo_op_vals = [tipos_op[i] for i in tipo_op_idx]

    unid_idx = rng.integers(0, len(unids), size=n_linhas)
    unid_vals = [unids[i] for i in unid_idx]

    dt_doc_mask = rng.random(n_linhas) < 0.85
    dt_es_mask = rng.random(n_linhas) < 0.80

    dt_doc = [
        f"{anos[i]}-{meses[i]:02d}-{dias[i]:02d}" if dt_doc_mask[i] else None
        for i in range(n_linhas)
    ]
    dt_es = [
        f"{anos[i]}-{meses[i]:02d}-{min(dias[i] + 1, 28):02d}" if dt_es_mask[i] else None
        for i in range(n_linhas)
    ]

    descricoes = _gerar_descricoes(n_itens, seed=seed)

    q_conv = rng.uniform(0.01, 100.0, size=n_linhas)
    q_sinal_sign = rng.choice([-1.0, 1.0], size=n_linhas, p=[0.35, 0.65])

    return pl.DataFrame(
        {
            "id_agrupado": [ids_agrupados[i] for i in idx_item],
            "Dt_doc": pl.Series(dt_doc, dtype=pl.Utf8).cast(pl.Date, strict=False),
            "Dt_e_s": pl.Series(dt_es, dtype=pl.Utf8).cast(pl.Date, strict=False),
            "Tipo_operacao": tipo_op_vals,
            "ncm_padrao": [f"{rng.integers(10000000, 99999999)}" for _ in range(n_linhas)],
            "cest_padrao": [f"{rng.integers(1000000, 9999999):07d}" for _ in range(n_linhas)],
            "descr_padrao": [descricoes[i] for i in idx_item],
            "Cod_item": [cods_item[i] for i in idx_item],
            "Cod_barra": [f"{rng.integers(1000000000000, 9999999999999)}" for _ in range(n_linhas)],
            "Ncm": [f"{rng.integers(10000000, 99999999)}" for _ in range(n_linhas)],
            "Cest": [f"{rng.integers(1000000, 9999999):07d}" for _ in range(n_linhas)],
            "Tipo_item": [py_rng.choice(["00", "01", "02", "09"]) for _ in range(n_linhas)],
            "Descr_item": [descricoes[i] for i in idx_item],
            "Cfop": [str(py_rng.choice([5102, 5405, 6102, 1101, 2101])) for _ in range(n_linhas)],
            "co_sefin_agr": [f"AGR{rng.integers(1000, 9999)}" for _ in range(n_linhas)],
            "unid_ref": unid_vals,
            "fator": [float(py_rng.choice([1.0, 0.001, 12.0, 0.5, 100.0])) for _ in range(n_linhas)],
            "q_conv": q_conv.tolist(),
            "__q_conv_sinal__": (q_conv * q_sinal_sign).tolist(),
            "preco_item": rng.uniform(1.0, 500.0, size=n_linhas).tolist(),
            "dev_simples": pl.Series([""] * n_linhas, dtype=pl.Utf8),
            "dev_venda": pl.Series([""] * n_linhas, dtype=pl.Utf8),
            "dev_compra": pl.Series([""] * n_linhas, dtype=pl.Utf8),
            "dev_ent_simples": pl.Series([""] * n_linhas, dtype=pl.Utf8),
        }
    )


@pytest.fixture(scope="session")
def bench_descricoes_100k() -> list[str]:
    parquet_path = _DATA_DIR / "descricoes_100k.parquet"
    if parquet_path.exists():
        return pl.read_parquet(parquet_path)["descricao"].to_list()
    descricoes = _gerar_descricoes(100_000)
    pl.DataFrame({"descricao": descricoes}).write_parquet(parquet_path)
    return descricoes


@pytest.fixture(scope="session")
def bench_descricoes_1m(request: pytest.FixtureRequest) -> list[str]:
    if not request.config.getoption("--bench-1m", default=False):
        pytest.skip("use --bench-1m para ativar fixture de 1M descricoes")
    parquet_path = _DATA_DIR / "descricoes_1m.parquet"
    if parquet_path.exists():
        return pl.read_parquet(parquet_path)["descricao"].to_list()
    descricoes = _gerar_descricoes(1_000_000)
    pl.DataFrame({"descricao": descricoes}).write_parquet(parquet_path)
    return descricoes


@pytest.fixture(scope="session")
def bench_movimentacao_estoque_synthetic() -> pl.DataFrame:
    parquet_path = _DATA_DIR / "movimentacao_50k.parquet"
    if parquet_path.exists():
        return pl.read_parquet(parquet_path)
    df = _gerar_movimentacao_sintetica()
    df.write_parquet(parquet_path)
    return df


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--bench-1m",
        action="store_true",
        default=False,
        help="Ativa fixture bench_descricoes_1m (1 milhao de strings; lento)",
    )
