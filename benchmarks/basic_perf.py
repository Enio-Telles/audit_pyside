"""
Basic performance benchmark for Polars groupby/join operations.

Usage:
  python benchmarks/basic_perf.py --n 10000

Generates synthetic data and measures aggregation + join timings.
"""
from __future__ import annotations

import time
import argparse
import random
import polars as pl


def make_data(n: int, n_descr: int = 500) -> pl.DataFrame:
    random.seed(0)
    descrs = [f"Produto {i}" for i in range(n_descr)]
    rows = {
        "descricao": [random.choice(descrs) for _ in range(n)],
        "ncm": [random.choice([None, f"{random.randint(1000,9999)}"]) for _ in range(n)],
        "cest": [random.choice([None, f"{random.randint(1000,9999)}"]) for _ in range(n)],
        "gtin": [random.choice([None, f"{random.randint(1000000000000,9999999999999)}"]) for _ in range(n)],
        "co_sefin_item": [random.choice([None, f"CS{random.randint(1,9)}"]) for _ in range(n)],
    }
    return pl.DataFrame(rows)


def bench(n: int):
    print(f"Generating data n={n}...")
    df = make_data(n)

    print("Adding normalized column and aggregating by (norm, descricao)...")
    t0 = time.perf_counter()
    df_norm = df.with_columns(
        pl.col("descricao")
        .str.to_lowercase()
        .str.replace_all(r"[^a-z0-9]", "")
        .alias("__descricao_norm")
    )
    grouped = (
        df_norm.lazy()
        .with_columns(
            [
                pl.when(pl.col("ncm").is_not_null()).then(1).otherwise(0).alias("__has_ncm"),
                pl.when(pl.col("cest").is_not_null()).then(1).otherwise(0).alias("__has_cest"),
                pl.when(pl.col("gtin").is_not_null()).then(1).otherwise(0).alias("__has_gtin"),
            ]
        )
        .group_by(["__descricao_norm", "descricao"])
        .agg([
            pl.len().alias("count"),
            pl.col("__has_ncm").max().alias("has_ncm"),
            pl.col("__has_cest").max().alias("has_cest"),
            pl.col("__has_gtin").max().alias("has_gtin"),
        ])
        .collect()
    )
    t1 = time.perf_counter()
    print(f"Aggregation time: {t1 - t0:.3f}s")

    print("Building distinct counts per normalized description (simulated)...")
    t0 = time.perf_counter()
    listas = (
        df_norm.lazy()
        .group_by("__descricao_norm")
        .agg(
            [
                pl.col("ncm").n_unique().alias("n_ncm_distinct"),
                pl.col("cest").n_unique().alias("n_cest_distinct"),
                pl.col("gtin").n_unique().alias("n_gtin_distinct"),
            ]
        )
        .collect()
    )
    t1 = time.perf_counter()
    print(f"Distinct counts aggregation time: {t1 - t0:.3f}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=10000)
    args = parser.parse_args()
    bench(args.n)
