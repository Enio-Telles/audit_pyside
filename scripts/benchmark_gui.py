"""
benchmark_gui.py — Benchmark oficial do ParquetQueryService (Fase E5)

Mede performance dos backends Polars e DuckDB em Parquets de diferentes tamanhos.
Gera arquivo sintetico, mede operacoes de paginacao, filtro e distinct,
e exporta resultados em JSON para docs/baseline_performance.md.

Uso:
    set PYTHONPATH=src
    python scripts/benchmark_gui.py [--sizes 256 1024 2048] [--rounds 5] [--output docs/baseline_performance.json]

Saida:
    JSON com metricas: TTFP, delta RSS, p95 page change, p95 filter apply, p95 distinct.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from statistics import mean, median, stdev

import polars as pl
import psutil


# ---------------------------------------------------------------------------
# Metricas
# ---------------------------------------------------------------------------

@dataclass
class BenchmarkResult:
    """Resultados de uma rodada de benchmark."""
    size_mb: int
    backend: str  # "polars" ou "duckdb"
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
        """Coeficiente de variacao (%). < 10% e aceitavel."""
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


# ---------------------------------------------------------------------------
# Gerador de Parquet sintetico
# ---------------------------------------------------------------------------

def _gerar_parquet_sintetico(target: Path, size_mb: int) -> Path:
    """
    Gera um Parquet sintetico com ~size_mb de tamanho (numpy vectorizado).

    Estrutura de colunas semelhante ao dataset real:
    - id_agrupado, descricao, ncm, cfop, unidade
    - vlr_contabil, qtde, valor_unitario
    """
    import numpy as np

    print(f"  Gerando Parquet sintetico de ~{size_mb} MB (numpy vectorizado)...")

    # Estima linhas por MB (~30 bytes/row comprimido zstd)
    linhas_por_mb = 33_000
    n_linhas = linhas_por_mb * size_mb

    parquet_path = target / f"bench_{size_mb}mb.parquet"
    rng = np.random.default_rng(42)

    # Gera em chunks de 500k linhas para controlar memoria
    chunk_size = min(500_000, n_linhas)
    n_chunks = max(1, (n_linhas + chunk_size - 1) // chunk_size)
    writer = None

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        for i in range(n_chunks):
            chunk_n = min(chunk_size, n_linhas - i * chunk_size)
            if chunk_n <= 0:
                break

            # Colunas string com numpy (rapido)
            ids_int = rng.integers(0, 2**48, size=chunk_n, dtype=np.uint64)
            id_agrupado = [f"id_agrupado_auto_{v:012x}" for v in ids_int]

            prod_ids = rng.integers(1, 10001, size=chunk_n)
            descricao = [f"PRODUTO {v:05d}" for v in prod_ids]

            ncm = rng.integers(10000000, 100000000, size=chunk_n).astype(str).tolist()

            cfop_choices = ["1102", "1403", "2102", "5102", "5403", "6102"]
            cfop = [cfop_choices[v] for v in rng.integers(0, 6, size=chunk_n)]

            und_choices = ["UN", "KG", "LT", "CX", "PC", "MT"]
            unidade = [und_choices[v] for v in rng.integers(0, 6, size=chunk_n)]

            mes = rng.integers(1, 13, size=chunk_n)
            mes_ref = [f"{v:02d}/2024" for v in mes]

            cnpj = rng.integers(10000000000000, 100000000000000, size=chunk_n).astype(str).tolist()

            # Colunas numericas (puro numpy)
            vlr_contabil = np.round(rng.uniform(0.01, 99999.99, size=chunk_n), 2)
            qtde = np.round(rng.uniform(0.001, 9999.999, size=chunk_n), 3)
            valor_unitario = np.round(rng.uniform(0.01, 999.99, size=chunk_n), 2)

            table = pa.table({
                "id_agrupado": pa.array(id_agrupado, type=pa.string()),
                "descricao": pa.array(descricao, type=pa.string()),
                "ncm": pa.array(ncm, type=pa.string()),
                "cfop": pa.array(cfop, type=pa.string()),
                "unidade": pa.array(unidade, type=pa.string()),
                "vlr_contabil": pa.array(vlr_contabil, type=pa.float64()),
                "qtde": pa.array(qtde, type=pa.float64()),
                "valor_unitario": pa.array(valor_unitario, type=pa.float64()),
                "mes_ref": pa.array(mes_ref, type=pa.string()),
                "cnpj": pa.array(cnpj, type=pa.string()),
            })

            if writer is None:
                writer = pq.ParquetWriter(
                    str(parquet_path), table.schema,
                    compression="zstd",
                )
            writer.write_table(table)
            print(f"    chunk {i+1}/{n_chunks}: {chunk_n:,} linhas")

    finally:
        if writer is not None:
            writer.close()

    actual_mb = parquet_path.stat().st_size / (1024 * 1024)
    print(f"  -> {parquet_path.name}: {actual_mb:.1f} MB, {n_linhas:,} linhas")
    return parquet_path


# ---------------------------------------------------------------------------
# Execucao do benchmark
# ---------------------------------------------------------------------------

def _get_rss_mb() -> float:
    """RSS do processo atual em MB."""
    return psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)


def _benchmark_operacao(
    service,
    parquet_path: Path,
    operation: str,
    func,
    rounds: int,
    size_mb: int,
) -> BenchmarkResult:
    backend = "duckdb" if service.usa_duckdb(parquet_path) else "polars"
    result = BenchmarkResult(size_mb=size_mb, backend=backend, operation=operation)

    for r in range(rounds):
        rss_before = _get_rss_mb()
        t0 = time.perf_counter()
        func()
        elapsed = time.perf_counter() - t0
        rss_after = _get_rss_mb()

        result.times_s.append(elapsed)
        result.rss_delta_mb.append(rss_after - rss_before)

        status = "OK" if elapsed < 5.0 else "LENTO"
        print(f"    [{status}] {operation} round {r+1}/{rounds}: {elapsed:.3f}s, RSS delta: {rss_after - rss_before:+.1f} MB")

    return result


def benchmark_size(
    service,
    parquet_path: Path,
    size_mb: int,
    rounds: int,
) -> list[BenchmarkResult]:
    """Roda todos os benchmarks para um tamanho de Parquet."""
    print(f"\n{'='*60}")
    print(f"  Benchmark: {size_mb} MB ({parquet_path.name})")
    print(f"  Backend: {'DuckDB' if service.usa_duckdb(parquet_path) else 'Polars'}")
    print(f"{'='*60}")

    from interface_grafica.services.parquet_service import FilterCondition

    results = []

    # 1. TTFP — Time To First Page (schema + count + page 1)
    def op_ttfp():
        service.get_schema(parquet_path)
        service.get_count(parquet_path)
        service.get_page(parquet_path, None, None, 1, 200)

    results.append(_benchmark_operacao(service, parquet_path, "ttfp", op_ttfp, rounds, size_mb))

    # 2. Page change (p2, p3, p10)
    for pg in [2, 3, 10]:
        def op_page(p=pg):
            service.get_page(parquet_path, None, None, p, 200)

        results.append(_benchmark_operacao(service, parquet_path, f"page_{pg}", op_page, rounds, size_mb))

    # 3. Filter apply — filtro "contem" em descricao
    def op_filter():
        f = [FilterCondition(column="descricao", operator="contem", value="PROD")]
        service.get_count(parquet_path, f)
        service.get_page(parquet_path, f, None, 1, 200)

    results.append(_benchmark_operacao(service, parquet_path, "filter_contem", op_filter, rounds, size_mb))

    # 4. Distinct values
    def op_distinct():
        service.get_distinct_values(parquet_path, "unidade", "", 200)

    results.append(_benchmark_operacao(service, parquet_path, "distinct", op_distinct, rounds, size_mb))

    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Benchmark oficial — ParquetQueryService (Fase E5 Plano Mestre)"
    )
    parser.add_argument(
        "--sizes", nargs="+", type=int, default=[256],
        help="Tamanhos de Parquet sintetico em MB (default: 256). Ex: --sizes 256 1024 2048"
    )
    parser.add_argument(
        "--rounds", type=int, default=5,
        help="Numero de rounds por operacao (default: 5)"
    )
    parser.add_argument(
        "--output", type=str, default="docs/baseline_performance.json",
        help="Arquivo de saida JSON"
    )
    parser.add_argument(
        "--keep-parquet", action="store_true",
        help="Manter Parquets sinteticos apos benchmark"
    )
    args = parser.parse_args()

    # Import aqui para garantir PYTHONPATH
    from interface_grafica.services.parquet_query_service import ParquetQueryService

    service = ParquetQueryService()

    tmpdir = tempfile.mkdtemp(prefix="bench_audit_")
    tmp_path = Path(tmpdir)

    all_results: list[dict] = []
    meta = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "hostname": os.environ.get("COMPUTERNAME", "unknown"),
        "python": sys.version.split()[0],
        "threshold_mb": 512,
        "page_size": 200,
        "rounds": args.rounds,
    }

    print(f"Benchmark audit_pyside — {meta['timestamp']}")
    print(f"  Rounds: {args.rounds}, Tamanhos: {args.sizes}")
    print(f"  Threshold DuckDB: {meta['threshold_mb']} MB")
    print(f"  Diretorio temporario: {tmpdir}")

    try:
        for size_mb in args.sizes:
            parquet_path = _gerar_parquet_sintetico(tmp_path, size_mb)
            results = benchmark_size(service, parquet_path, size_mb, args.rounds)
            all_results.extend([r.to_dict() for r in results])

    finally:
        if not args.keep_parquet:
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)
            print(f"\nParquets temporarios removidos: {tmpdir}")

    # Salvar resultados
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report = {"meta": meta, "results": all_results}
    output_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nResultados salvos em: {output_path}")

    # Resumo
    print(f"\n{'='*60}")
    print("  RESUMO")
    print(f"{'='*60}")
    print(f"  {'Tamanho':>10} | {'Operacao':>20} | {'Backend':>7} | {'Mean':>7} | {'P95':>7} | {'CV%':>5} | {'RSS':>8}")
    print(f"  {'-'*10}-+-{'-'*20}-+-{'-'*7}-+-{'-'*7}-+-{'-'*7}-+-{'-'*5}-+-{'-'*8}")
    for r in all_results:
        print(
            f"  {r['size_mb']:>7} MB | {r['operation']:>20} | {r['backend']:>7} | "
            f"{r['mean_s']:>6.3f}s | {r['p95_s']:>6.3f}s | {r['cv_pct']:>4.1f}% | "
            f"{r['mean_rss_delta_mb']:>+6.1f} MB"
        )

    # KPIs do Plano Mestre
    print(f"\n{'='*60}")
    print("  VALIDACAO KPIs (Plano Mestre §7)")
    print(f"{'='*60}")
    for r in all_results:
        if r["operation"] == "ttfp":
            status = "PASS" if r["p95_s"] <= 5.0 else "FAIL"
            print(f"  [{status}] TTFP {r['size_mb']} MB: {r['p95_s']:.3f}s (meta: <= 5.0s)")
        elif r["operation"].startswith("page_"):
            status = "PASS" if r["p95_s"] <= 2.0 else "FAIL"
            print(f"  [{status}] Page change {r['size_mb']} MB ({r['operation']}): {r['p95_s']:.3f}s (meta: <= 2.0s)")
        elif r["operation"] == "filter_contem":
            status = "PASS" if r["p95_s"] <= 5.0 else "FAIL"
            print(f"  [{status}] Filter {r['size_mb']} MB: {r['p95_s']:.3f}s (meta: <= 5.0s)")
        elif r["operation"] == "distinct":
            status = "PASS" if r["p95_s"] <= 1.0 else "FAIL"
            print(f"  [{status}] Distinct {r['size_mb']} MB: {r['p95_s']:.3f}s (meta: <= 1.0s)")


if __name__ == "__main__":
    main()
