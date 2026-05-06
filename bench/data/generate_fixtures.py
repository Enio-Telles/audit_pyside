"""
generate_fixtures.py — Gerador de fixtures sintéticas de Parquet e Benchmark de Baseline.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from statistics import mean, median, stdev

import numpy as np
import polars as pl
import psutil
import pyarrow as pa
import pyarrow.parquet as pq

from interface_grafica.services.parquet_query_service import ParquetQueryService
from interface_grafica.services.parquet_service import FilterCondition


@dataclass
class BenchmarkResult:
    size_mb: int
    backend: str
    operation: str
    times_s: list[float] = field(default_factory=list)
    rss_delta_mb: list[float] = field(default_factory=list)

    @property
    def mean_s(self) -> float:
        return mean(self.times_s) if self.times_s else 0.0

    @property
    def median_s(self) -> float:
        return median(self.times_s) if self.times_s else 0.0

    @property
    def p95_s(self) -> float:
        if not self.times_s:
            return 0.0
        sorted_t = sorted(self.times_s)
        idx = int(0.95 * len(sorted_t))
        return sorted_t[min(idx, len(sorted_t) - 1)]

    @property
    def cv(self) -> float:
        if len(self.times_s) < 2 or self.mean_s == 0:
            return 0.0
        return (stdev(self.times_s) / self.mean_s) * 100

    @property
    def mean_rss_delta_mb(self) -> float:
        return mean(self.rss_delta_mb) if self.rss_delta_mb else 0.0

    def to_dict(self) -> dict:
        return {
            "size_mb": self.size_mb,
            "backend": self.backend,
            "operation": self.operation,
            "mean_s": round(self.mean_s, 4),
            "median_s": round(self.median_s, 4),
            "p95_s": round(self.p95_s, 4),
            "cv_pct": round(self.cv, 2),
            "mean_rss_delta_mb": round(self.mean_rss_delta_mb, 2),
            "rounds": len(self.times_s),
        }


def _gerar_parquet_sintetico(target: Path, size_mb: int) -> Path:
    print(f"  Gerando Parquet sintético de ~{size_mb} MB...")
    if size_mb <= 256:
        n_linhas = 7_000_000
        filename = "small_parquet_256mb.parquet"
    elif size_mb <= 1024:
        n_linhas = 25_000_000
        filename = "large_parquet_1gb.parquet"
    else:
        n_linhas = 40_000_000
        filename = "xlarge_parquet_2gb.parquet"

    parquet_path = target / filename
    rng = np.random.default_rng(42)
    chunk_size = 2_000_000
    n_chunks = (n_linhas + chunk_size - 1) // chunk_size
    writer = None

    try:
        for i in range(n_chunks):
            chunk_n = min(chunk_size, n_linhas - i * chunk_size)
            if chunk_n <= 0:
                break

            ids_int = rng.integers(0, 2**48, size=chunk_n, dtype=np.uint64)
            id_agrupado = [f"id_agrupado_auto_{v:012x}" for v in ids_int]
            id_agregado = [None] * chunk_n

            prod_ids = rng.integers(1, 10001, size=chunk_n)
            descricao = [f"PRODUTO {v:05d}" for v in prod_ids]
            cod_produto = [f"COD_{v:05d}" for v in prod_ids]
            und_choices = np.array(["UN", "KG", "LT", "CX", "PC", "MT"])
            unidade = und_choices[rng.integers(0, 6, size=chunk_n)]
            cnpj = ["12345678000199"] * chunk_n
            data_emissao = ["2024-01-01"] * chunk_n

            table = pa.table(
                {
                    "id_agrupado": id_agrupado,
                    "id_agregado": id_agregado,
                    "__qtd_decl_final_audit__": rng.uniform(0.001, 1000.0, size=chunk_n),
                    "q_conv": rng.uniform(0.001, 1000.0, size=chunk_n),
                    "q_conv_fisica": rng.uniform(0.001, 1000.0, size=chunk_n),
                    "CNPJ": cnpj,
                    "data_emissao": data_emissao,
                    "cod_produto": cod_produto,
                    "descricao": descricao,
                    "unidade": unidade,
                    "valor_unitario": rng.uniform(0.01, 500.0, size=chunk_n),
                    "valor_total": rng.uniform(0.01, 50000.0, size=chunk_n),
                }
            )

            if writer is None:
                writer = pq.ParquetWriter(str(parquet_path), table.schema, compression="snappy")
            writer.write_table(table)
            if (i + 1) % 5 == 0 or (i + 1) == n_chunks:
                print(f"    chunk {i + 1}/{n_chunks}")

    finally:
        if writer is not None:
            writer.close()

    actual_mb = parquet_path.stat().st_size / (1024 * 1024)
    print(f"  -> {parquet_path.name}: {actual_mb:.1f} MB")
    return parquet_path


def _get_rss_mb() -> float:
    return psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)


def _benchmark_operacao(service, path, operation, func, rounds, size_mb) -> BenchmarkResult:
    backend = "duckdb" if service.usa_duckdb(path) else "polars"
    result = BenchmarkResult(size_mb=size_mb, backend=backend, operation=operation)
    for r in range(rounds):
        rss_before = _get_rss_mb()
        t0 = time.perf_counter()
        func()
        elapsed = time.perf_counter() - t0
        rss_after = _get_rss_mb()
        result.times_s.append(elapsed)
        result.rss_delta_mb.append(rss_after - rss_before)
        print(f"    {operation} round {r + 1}: {elapsed:.3f}s")
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sizes", nargs="+", type=int, default=[256, 1024, 2048])
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--output", type=str, default="docs/baseline_performance.json")
    parser.add_argument("--temp-dir", type=str, default=None)
    args = parser.parse_args()

    service = ParquetQueryService()
    tmp_path = Path(args.temp_dir) if args.temp_dir else Path(tempfile.mkdtemp())
    tmp_path.mkdir(parents=True, exist_ok=True)
    all_results = []

    try:
        for size_mb in args.sizes:
            path = _gerar_parquet_sintetico(tmp_path, size_mb)
            all_results.append(
                _benchmark_operacao(
                    service,
                    path,
                    "ttfp",
                    lambda p=path: (
                        service.get_schema(p),
                        service.get_count(p),
                        service.get_page(p, None, None, 1, 200),
                    ),
                    args.rounds,
                    size_mb,
                )
            )
            all_results.append(
                _benchmark_operacao(
                    service,
                    path,
                    "page_change",
                    lambda p=path: service.get_page(p, None, None, 10, 200),
                    args.rounds,
                    size_mb,
                )
            )
            f = [FilterCondition(column="descricao", operator="contem", value="PRODUTO 00001")]
            all_results.append(
                _benchmark_operacao(
                    service,
                    path,
                    "filter_apply",
                    lambda p=path, cond=f: (
                        service.get_count(p, cond),
                        service.get_page(p, cond, None, 1, 200),
                    ),
                    args.rounds,
                    size_mb,
                )
            )
            export_target = tmp_path / f"export_{size_mb}.parquet"

            def op_export(p=path, t=export_target):
                df = pl.scan_parquet(p).limit(50000).collect()
                df.write_parquet(t)

            all_results.append(
                _benchmark_operacao(service, path, "export_50k", op_export, args.rounds, size_mb)
            )

    finally:
        if not args.temp_dir:
            import shutil

            shutil.rmtree(tmp_path, ignore_errors=True)

    with open(args.output, "w") as f:
        json.dump(
            {
                "meta": {
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                    "python": sys.version.split()[0],
                },
                "results": [r.to_dict() for r in all_results],
            },
            f,
            indent=2,
        )


if __name__ == "__main__":
    main()
