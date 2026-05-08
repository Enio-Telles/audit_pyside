"""
benchmark_gui.py — Benchmark oficial do ParquetQueryService (Fase E5)
"""
from __future__ import annotations
import argparse, json, os, platform, sys, tempfile, time, psutil
import numpy as np, polars as pl, pyarrow as pa, pyarrow.parquet as pq
from dataclasses import dataclass, field
from pathlib import Path
from statistics import mean, median, stdev

@dataclass
class BenchmarkResult:
    size_mb: int
    backend: str
    operation: str
    times_s: list[float] = field(default_factory=list)
    rss_delta_mb: list[float] = field(default_factory=list)

    def to_dict(self) -> dict:
        m_s = mean(self.times_s) if self.times_s else 0
        d = {
            "size_mb": self.size_mb,
            "backend": self.backend,
            "operation": self.operation,
            "mean_s": round(m_s, 4),
            "p50_s": round(median(self.times_s) if self.times_s else 0, 4),
            "cv_pct": round((stdev(self.times_s) / m_s * 100) if len(self.times_s) > 1 and m_s > 0 else 0, 2),
            "mean_rss_delta_mb": round(mean(self.rss_delta_mb) if self.rss_delta_mb else 0, 2),
            "rounds": len(self.times_s),
        }
        if len(self.times_s) >= 2:
            sorted_t = sorted(self.times_s)
            d["p95_s"] = round(sorted_t[min(int(0.95 * len(sorted_t)), len(sorted_t) - 1)], 4)
        return d

def _gerar_parquet_sintetico(target: Path, size_mb: int) -> Path:
    print(f"  Gerando Parquet sintetico de ~{size_mb} MB...")
    linhas_por_mb = 28_000
    n_linhas = linhas_por_mb * size_mb
    p_path = target / f"bench_{size_mb}mb.parquet"
    rng = np.random.default_rng(42)
    chunk_size = 500_000
    n_chunks = max(1, (n_linhas + chunk_size - 1) // chunk_size)
    writer = None
    try:
        for i in range(n_chunks):
            chunk_n = min(chunk_size, n_linhas - i * chunk_size)
            if chunk_n <= 0: break
            ids_int = rng.integers(0, 2**48, size=chunk_n, dtype=np.uint64)
            table = pa.table({
                "id_agrupado": [f"id_agrupado_auto_{v:012x}" for v in ids_int],
                "id_agregado": [None] * chunk_n,
                "__qtd_decl_final_audit__": rng.uniform(0, 1000, size=chunk_n),
                "q_conv": rng.uniform(0, 1000, size=chunk_n),
                "q_conv_fisica": rng.uniform(0, 1000, size=chunk_n),
                "descricao": [f"PRODUTO {v:05d}" for v in rng.integers(1, 10001, size=chunk_n)],
                "unidade": ["UN"] * chunk_n,
                "vlr_contabil": rng.uniform(0.01, 1000, size=chunk_n),
                "cnpj": ["12345678000199"] * chunk_n,
            })
            if writer is None:
                writer = pq.ParquetWriter(str(p_path), table.schema, compression="zstd")
            writer.write_table(table)
    finally:
        if writer: writer.close()
    return p_path

def benchmark_size(service, path, size_mb, rounds, tmp_path) -> list[BenchmarkResult]:
    from interface_grafica.services.parquet_service import FilterCondition
    results = []
    ops = [
        ("ttfp", lambda: (service.get_schema(path), service.get_count(path), service.get_page(path, None, None, 1, 200))),
        ("page_2", lambda: service.get_page(path, None, None, 2, 200)),
        ("filter_apply", lambda: (service.get_count(path, [FilterCondition("descricao", "contem", "PROD")]), service.get_page(path, [FilterCondition("descricao", "contem", "PROD")], None, 1, 200))),
        ("distinct", lambda: service.get_distinct_values(path, "unidade", "", 200)),
        ("export_50k", lambda: service.export_to_parquet(path, None, None, tmp_path / f"exp_{size_mb}.parquet")),
    ]
    for name, func in ops:
        res = BenchmarkResult(size_mb, "duckdb" if service.usa_duckdb(path) else "polars", name)
        for r in range(rounds):
            m0 = psutil.Process().memory_info().rss
            t0 = time.perf_counter()
            func()
            t1 = time.perf_counter()
            m1 = psutil.Process().memory_info().rss
            res.times_s.append(t1 - t0)
            res.rss_delta_mb.append((m1 - m0) / (1024 * 1024))
            print(f"    {name} round {r+1}: {t1-t0:.3f}s")
        results.append(res)
    return results

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sizes", nargs="+", type=int, default=[256])
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--output", default="docs/baseline_performance.json")
    parser.add_argument("--proxy", action="store_true")
    args = parser.parse_args()
    try:
        from interface_grafica.services.parquet_query_service import ParquetQueryService
    except ImportError:
        print("Erro: PYTHONPATH deve incluir 'src'"); sys.exit(1)
    service = ParquetQueryService()
    tmpdir = Path(tempfile.mkdtemp(prefix="bench_audit_"))
    all_res = []
    try:
        for s in args.sizes:
            p = _gerar_parquet_sintetico(tmpdir, s)
            results = benchmark_size(service, p, s, args.rounds, tmpdir)
            all_res.extend([r.to_dict() for r in results])
    finally:
        import shutil; shutil.rmtree(tmpdir, ignore_errors=True)
    import duckdb
    meta = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "os": f"{platform.system()} {platform.release()}",
        "cpu": platform.processor() or "x86_64",
        "ram_gb": round(psutil.virtual_memory().total / (1024**3), 2),
        "python": sys.version.split()[0],
        "polars_version": pl.__version__,
        "duckdb_version": duckdb.__version__,
        "pyarrow_version": pa.__version__,
        "rounds": args.rounds,
        "is_proxy": args.proxy,
    }
    Path(args.output).write_text(json.dumps({"meta": meta, "results": all_res}, indent=2))
    print(f"Resultados salvos em {args.output}")

if __name__ == "__main__": main()
