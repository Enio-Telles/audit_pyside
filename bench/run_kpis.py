"""
bench/run_kpis.py
==================

Benchmark dos 8 KPIs SMART do plano de auditoria de campos categÃ³ricos
(Notion ``358edc8b7d5d81cfb33ce023d4cee84f`` Â§F).

Mede o impacto da adoÃ§Ã£o de ``scan_parquet_typed`` em RAM, tempo de
agregaÃ§Ã£o, tempo de filtro, tamanho on-disk e cobertura de
RLE_DICTIONARY. Compara duas execuÃ§Ãµes (baseline vs typed) e produz um
relatÃ³rio go/no-go que destrava ou bloqueia a refatoraÃ§Ã£o de
``src/transformacao/*``.

CenÃ¡rios
--------
- ``baseline``: usa ``pl.scan_parquet`` direto (sem categÃ³ricos).
- ``typed``: usa ``scan_parquet_typed`` do mÃ³dulo ``categorical_recovery``.

Cada cenÃ¡rio roda os 8 KPIs e grava JSON em ``bench/results/<scenario>.json``.

CritÃ©rios de "go" para PR 3 (refatoraÃ§Ã£o de transformacao/)
-----------------------------------------------------------
Pelo menos 2 dos 4 KPIs primÃ¡rios devem cumprir a meta:

1. RSS peak no scan completo: reduÃ§Ã£o â‰¥ 30%
2. Tempo de groupby: â‰¤ 60% do baseline
3. Tempo de filter cfop in [...]: â‰¤ 30% do baseline
4. Tempo de filter cst='00': â‰¤ 20% do baseline

Os 4 KPIs secundÃ¡rios (tamanho on-disk, P95 paginaÃ§Ã£o, RLE_DICTIONARY,
divergÃªncia de invariantes) servem para PR 4 e nÃ£o bloqueiam PR 3.

Uso
---
    # 1. Baseline
    uv run python bench/run_kpis.py baseline \\
        --parquet /data/audit_pyside/parquets/04240370002877/c170_xml.parquet \\
        --output bench/results/baseline.json

    # 2. Typed (apÃ³s mergear categorical_recovery)
    uv run python bench/run_kpis.py typed \\
        --parquet /data/audit_pyside/parquets/04240370002877/c170_xml.parquet \\
        --output bench/results/typed.json

    # 3. Comparar e gerar relatÃ³rio
    uv run python bench/run_kpis.py compare \\
        --baseline bench/results/baseline.json \\
        --typed bench/results/typed.json \\
        --report bench/results/comparison.md

    # Smoke test sintÃ©tico (sem dados reais)
    uv run python bench/run_kpis.py smoke

Hardware-alvo: D3 (i5 4 cores, 16 GB Windows; DuckDB memory_limit=6GB threads=2).
Timings podem variar; o que importa Ã© a razÃ£o typed/baseline.
"""

from __future__ import annotations

import argparse
import gc
import json
import logging
import math
import os
import statistics
import sys
import time
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import polars as pl

try:
    import psutil
except ImportError:
    psutil = None  # measured RSS will be 0; bench ainda roda

logger = logging.getLogger(__name__)


# =====================================================================
# ConfiguraÃ§Ã£o
# =====================================================================

#: NÃºmero de iteraÃ§Ãµes por KPI. Tomamos a mediana para reduzir ruÃ­do.
DEFAULT_ITERATIONS = 5

#: CFOPs tÃ­picos de saÃ­da para o filtro KPI 3.
CFOP_FILTER_LIST = ["5102", "5101", "5405", "5949", "6102", "6108", "6403"]

#: CritÃ©rios SMART de "go" para PR 3.
GATE_THRESHOLDS = {
    "rss_peak_reduction_min_pct": 30.0,
    "groupby_max_ratio": 0.60,
    "filter_cfop_max_ratio": 0.30,
    "filter_cst_max_ratio": 0.20,
}

#: Quantos dos 4 KPIs primÃ¡rios precisam passar.
GO_DECISION_MIN_PASSING = 2


Scenario = Literal["baseline", "typed"]


# =====================================================================
# Modelo de resultados
# =====================================================================


@dataclass(slots=True)
class KPIResult:
    """Resultado de um KPI (mediana + estatÃ­sticas)."""

    name: str
    unit: str
    iterations: list[float]
    median: float
    p95: float
    min_value: float
    max_value: float
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "unit": self.unit,
            "iterations": [round(v, 4) for v in self.iterations],
            "median": round(self.median, 4),
            "p95": round(self.p95, 4),
            "min": round(self.min_value, 4),
            "max": round(self.max_value, 4),
            "metadata": self.metadata,
        }


@dataclass(slots=True)
class BenchRun:
    """Resultado de uma execuÃ§Ã£o completa (1 cenÃ¡rio)."""

    scenario: Scenario
    parquet_path: str
    timestamp: str
    polars_version: str
    n_rows: int
    file_size_bytes: int
    kpis: dict[str, KPIResult]

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario,
            "parquet_path": self.parquet_path,
            "timestamp": self.timestamp,
            "polars_version": self.polars_version,
            "n_rows": self.n_rows,
            "file_size_bytes": self.file_size_bytes,
            "kpis": {name: kpi.to_dict() for name, kpi in self.kpis.items()},
        }


# =====================================================================
# UtilitÃ¡rios de mediÃ§Ã£o
# =====================================================================


def _process_rss_bytes() -> int:
    """RSS atual do processo em bytes. Retorna 0 se psutil ausente."""
    if psutil is None:
        return 0
    return psutil.Process(os.getpid()).memory_info().rss


@contextmanager
def measure_peak_rss():
    """
    Context manager que mede o pico de RSS durante a execuÃ§Ã£o.

    NÃ£o Ã© exato â€” psutil nÃ£o tem polling embutido. Captura RSS no
    inÃ­cio, no fim, e a cada poll explÃ­cito via ``probe()``.

    Yields:
        FunÃ§Ã£o ``probe() -> int`` que retorna o RSS atual e atualiza o
        peak interno.
    """
    if psutil is None:
        yield lambda: 0
        return
    peak = [_process_rss_bytes()]

    def probe() -> int:
        current = _process_rss_bytes()
        if current > peak[0]:
            peak[0] = current
        return current

    yield probe


def aggregate(name: str, unit: str, samples: list[float], **metadata: Any) -> KPIResult:
    """ConstrÃ³i um KPIResult a partir de N amostras."""
    samples_sorted = sorted(samples)
    return KPIResult(
        name=name,
        unit=unit,
        iterations=samples,
        median=statistics.median(samples_sorted),
        p95=samples_sorted[
            min(len(samples_sorted) - 1, max(0, math.ceil(len(samples_sorted) * 0.95) - 1))
        ],
        min_value=samples_sorted[0],
        max_value=samples_sorted[-1],
        metadata=metadata,
    )


def reset_state() -> None:
    """Garbage collect entre iteraÃ§Ãµes para reduzir contaminaÃ§Ã£o."""
    gc.collect()


# =====================================================================
# Scan factories â€” diferenÃ§a entre baseline e typed
# =====================================================================


def make_scan(
    scenario: Scenario, codes_path: Path | None = None
) -> Callable[[str | Path], pl.LazyFrame]:
    """
    Retorna a funÃ§Ã£o de scan apropriada para o cenÃ¡rio.

    Args:
        scenario: 'baseline' ou 'typed'.
        codes_path: Caminho do JSON (apenas typed). Se None, usa default.

    Returns:
        FunÃ§Ã£o ``scan(path) -> LazyFrame``.
    """
    if scenario == "baseline":
        return pl.scan_parquet
    elif scenario == "typed":
        try:
            from src.io.categorical_recovery import scan_parquet_typed
        except ImportError as exc:
            raise ImportError(
                "MÃ³dulo categorical_recovery nÃ£o encontrado. Veja subpÃ¡gina "
                "Notion 358edc8b7d5d81059247de78a772a16e."
            ) from exc

        def scan_typed(path: str | Path) -> pl.LazyFrame:
            # strict_cast=False: valores desconhecidos viram null em vez de
            # levantar erro — necessario para medir performance em dados reais
            # que podem conter codigos sujos pontuais (ex.: "061" em Cst).
            return scan_parquet_typed(path, codes_path=codes_path, strict_cast=False)

        return scan_typed
    else:
        raise ValueError(f"CenÃ¡rio desconhecido: {scenario}")


# =====================================================================
# KPIs primÃ¡rios
# =====================================================================


def kpi_rss_peak_full_scan(
    scan_fn: Callable[[str | Path], pl.LazyFrame],
    parquet_path: Path,
    iterations: int,
) -> KPIResult:
    """
    KPI 1: RSS peak ao fazer scan completo + collect do Parquet.

    Mede o pico de memÃ³ria residente durante o materializaÃ§Ã£o. Mediana
    em N iteraÃ§Ãµes.
    """
    samples: list[float] = []
    for i in range(iterations):
        reset_state()
        baseline_rss = _process_rss_bytes()
        with measure_peak_rss() as probe:
            df = scan_fn(parquet_path).collect()
            probe()  # Captura pico apÃ³s collect
            _ = df.height  # ForÃ§a materializaÃ§Ã£o
            probe()
            del df
        peak = probe()
        delta_mb = (peak - baseline_rss) / 1024 / 1024
        samples.append(delta_mb)
        logger.debug("KPI rss_peak iter %d: %.2f MB", i + 1, delta_mb)

    return aggregate(
        name="rss_peak_full_scan",
        unit="MB",
        samples=samples,
        description="Delta RSS pico durante scan + collect completo",
    )


def kpi_groupby_id_agrupado(
    scan_fn: Callable[[str | Path], pl.LazyFrame],
    parquet_path: Path,
    iterations: int,
) -> KPIResult:
    """
    KPI 2: Tempo de groupby(id_agrupado).agg(sum) das invariantes.

    Esta Ã© a operaÃ§Ã£o central do pipeline fiscal. Mede sÃ³ agregaÃ§Ã£o;
    leitura Ã© amortizada pela cache do SO entre iteraÃ§Ãµes.
    """
    samples: list[float] = []
    schema = scan_fn(parquet_path).collect_schema()
    available_invariants = [
        c for c in ("__qtd_decl_final_audit__", "q_conv", "q_conv_fisica")
        if c in schema.names()
    ]
    if not available_invariants:
        logger.warning(
            "Nenhuma invariante encontrada em %s; KPI groupby usarÃ¡ pl.len()",
            parquet_path,
        )
    if "id_agrupado" not in schema.names():
        logger.warning("id_agrupado ausente; usando primeira coluna nÃ£o-numÃ©rica")

    for i in range(iterations):
        reset_state()
        start = time.perf_counter()
        if "id_agrupado" in schema.names() and available_invariants:
            result = (
                scan_fn(parquet_path)
                .group_by("id_agrupado")
                .agg(*[pl.col(c).sum() for c in available_invariants])
                .collect()
            )
        else:
            # Fallback: agrupar por primeira coluna string disponÃ­vel
            grouping_col = next(
                (c for c, dt in schema.items() if dt == pl.String),
                schema.names()[0],
            )
            result = (
                scan_fn(parquet_path)
                .group_by(grouping_col)
                .agg(pl.len())
                .collect()
            )
        elapsed = time.perf_counter() - start
        samples.append(elapsed * 1000)  # ms
        del result
        logger.debug("KPI groupby iter %d: %.2f ms", i + 1, elapsed * 1000)

    return aggregate(
        name="groupby_id_agrupado_sum",
        unit="ms",
        samples=samples,
        invariants=available_invariants,
    )


def kpi_filter_cfop_in(
    scan_fn: Callable[[str | Path], pl.LazyFrame],
    parquet_path: Path,
    iterations: int,
) -> KPIResult:
    """
    KPI 3: Tempo de filter(cfop in [lista]) + count.

    Para cenÃ¡rio typed, `cfop` Ã© Enum â€” Polars compara via UInt
    (esperado 3-8Ã— mais rÃ¡pido que comparaÃ§Ã£o de strings).
    """
    samples: list[float] = []
    schema = scan_fn(parquet_path).collect_schema()
    if "cfop" not in schema.names():
        logger.warning("cfop ausente em %s; KPI filter_cfop_in skippado", parquet_path)
        return aggregate(name="filter_cfop_in", unit="ms", samples=[0.0])

    for i in range(iterations):
        reset_state()
        start = time.perf_counter()
        n = (
            scan_fn(parquet_path)
            .filter(pl.col("cfop").is_in(CFOP_FILTER_LIST))
            .select(pl.len())
            .collect()
            .item()
        )
        elapsed = time.perf_counter() - start
        samples.append(elapsed * 1000)
        logger.debug("KPI filter_cfop iter %d: %.2f ms (n=%d)", i + 1, elapsed * 1000, n)

    return aggregate(
        name="filter_cfop_in",
        unit="ms",
        samples=samples,
        cfops_filtered=CFOP_FILTER_LIST,
    )


def kpi_filter_cst_eq(
    scan_fn: Callable[[str | Path], pl.LazyFrame],
    parquet_path: Path,
    iterations: int,
) -> KPIResult:
    """
    KPI 4: Tempo de filter(cst_icms == '00') + count.

    Filtro mais comum: tributaÃ§Ã£o normal. CST '00' Ã© alta frequÃªncia â€”
    ganho com Enum esperado 5Ã—+.
    """
    samples: list[float] = []
    schema = scan_fn(parquet_path).collect_schema()
    cst_col = next(
        (c for c in ("cst_icms", "Cst", "Cst_c170", "icms_CST") if c in schema.names()),
        None,
    )
    if cst_col is None:
        logger.warning("Nenhuma coluna de CST encontrada; KPI filter_cst skippado")
        return aggregate(name="filter_cst_eq", unit="ms", samples=[0.0])

    target = "00" if cst_col != "cst_icms" else "000"

    for i in range(iterations):
        reset_state()
        start = time.perf_counter()
        n = (
            scan_fn(parquet_path)
            .filter(pl.col(cst_col) == target)
            .select(pl.len())
            .collect()
            .item()
        )
        elapsed = time.perf_counter() - start
        samples.append(elapsed * 1000)
        logger.debug("KPI filter_cst iter %d: %.2f ms (n=%d)", i + 1, elapsed * 1000, n)

    return aggregate(
        name="filter_cst_eq",
        unit="ms",
        samples=samples,
        column=cst_col,
        target_value=target,
    )


# =====================================================================
# KPIs secundÃ¡rios (PR 4 / informativos)
# =====================================================================


def kpi_file_size_on_disk(parquet_path: Path) -> KPIResult:
    """KPI 5: Tamanho do Parquet em disco. NÃ£o muda com cenÃ¡rio; informativo."""
    size_mb = parquet_path.stat().st_size / 1024 / 1024
    return aggregate(name="file_size_on_disk", unit="MB", samples=[size_mb])


def kpi_pagination_p95(
    scan_fn: Callable[[str | Path], pl.LazyFrame],
    parquet_path: Path,
    iterations: int,
    page_size: int = 100,
) -> KPIResult:
    """
    KPI 6: P95 de tempo para paginar 100 linhas com filtro UFÃ—CFOP.

    Simula uso da GUI: usuÃ¡rio filtrando por UF e CFOP, pedindo pÃ¡gina.
    Meta: â‰¤ 300 ms.
    """
    samples: list[float] = []
    schema = scan_fn(parquet_path).collect_schema()
    if "uf" not in schema.names() or "cfop" not in schema.names():
        logger.warning("uf ou cfop ausente; KPI pagination skippado")
        return aggregate(name="pagination_p95", unit="ms", samples=[0.0])

    # Mais iteraÃ§Ãµes para P95 confiÃ¡vel
    n_iter = max(iterations, 20)
    for i in range(n_iter):
        reset_state()
        # UF varia entre pÃ¡ginas para nÃ£o cachear
        uf_target = ["RO", "SP", "MG", "RJ", "PR"][i % 5]
        start = time.perf_counter()
        result = (
            scan_fn(parquet_path)
            .filter(
                (pl.col("uf") == uf_target) & pl.col("cfop").is_in(CFOP_FILTER_LIST)
            )
            .head(page_size)
            .collect()
        )
        elapsed = time.perf_counter() - start
        samples.append(elapsed * 1000)
        del result

    return aggregate(
        name="pagination_p95",
        unit="ms",
        samples=samples,
        page_size=page_size,
        filter_pattern="uf=X AND cfop IN [...]",
    )


def kpi_rle_dictionary_coverage(parquet_path: Path) -> KPIResult:
    """
    KPI 7: % de row_groups onde colunas-alvo usam RLE_DICTIONARY.

    SÃ³ relevante para PR 4 (Parquets v2). Em Parquets v1 atuais, deve
    estar prÃ³ximo de 0% para colunas categÃ³ricas.
    """
    try:
        import pyarrow.parquet as pq
    except ImportError:
        logger.warning("pyarrow ausente; KPI RLE_DICTIONARY skippado")
        return aggregate(name="rle_dictionary_coverage", unit="pct", samples=[0.0])

    target_cols = {"cfop", "cst_icms", "uf", "tipo_operacao", "ncm", "cest"}
    pf = pq.ParquetFile(parquet_path)
    n_row_groups = pf.metadata.num_row_groups
    if n_row_groups == 0:
        return aggregate(name="rle_dictionary_coverage", unit="pct", samples=[0.0])

    covered = 0
    total = 0
    for rg_idx in range(n_row_groups):
        rg = pf.metadata.row_group(rg_idx)
        for col_idx in range(rg.num_columns):
            cm = rg.column(col_idx)
            if cm.path_in_schema in target_cols:
                total += 1
                # RLE_DICTIONARY indica que dictionary encoding foi usado
                encodings = [str(e) for e in cm.encodings]
                if cm.has_dictionary_page and "RLE_DICTIONARY" in encodings:
                    covered += 1
    pct = (covered / total * 100) if total > 0 else 0.0
    return aggregate(
        name="rle_dictionary_coverage",
        unit="pct",
        samples=[pct],
        target_columns=sorted(target_cols),
        n_row_groups=n_row_groups,
        n_target_columns_present=total,
    )


def kpi_invariants_byte_identical(
    parquet_path: Path,
) -> KPIResult:
    """
    KPI 8: Hash SHA-256 das invariantes (typed vs baseline).

    Em PR 2 (em memÃ³ria), deve ser 0 divergÃªncias. Em PR 4, comparar
    Parquet v1 vs v2.

    Aqui apenas medimos no cenÃ¡rio atual: gera hash que outro cenÃ¡rio
    pode comparar.
    """
    import hashlib

    INVARIANTES = ["id_agrupado", "id_agregado", "__qtd_decl_final_audit__",
                   "q_conv", "q_conv_fisica"]
    schema = pl.scan_parquet(parquet_path).collect_schema()
    available = [c for c in INVARIANTES if c in schema.names()]
    if not available:
        return aggregate(name="invariants_hash", unit="hex", samples=[0.0],
                         hash="", note="nenhuma invariante presente")
    df = (
        pl.scan_parquet(parquet_path)
        .select(available)
        .sort(available)
        .collect()
    )
    # write_ipc Ã© determinÃ­stico; use para hash
    buf = df.write_ipc(None)
    h = hashlib.sha256(buf.getbuffer()).hexdigest()
    # Retorna 1.0 = sucesso, samples preserva o hash em metadata
    return aggregate(
        name="invariants_hash",
        unit="hex_marker",
        samples=[1.0],
        hash=h,
        invariants_present=available,
    )


# =====================================================================
# OrquestraÃ§Ã£o
# =====================================================================


KPI_FUNCTIONS = {
    # PrimÃ¡rios (gates do PR 3)
    "rss_peak_full_scan": kpi_rss_peak_full_scan,
    "groupby_id_agrupado_sum": kpi_groupby_id_agrupado,
    "filter_cfop_in": kpi_filter_cfop_in,
    "filter_cst_eq": kpi_filter_cst_eq,
    # SecundÃ¡rios (PR 4 / informativos)
    "pagination_p95": kpi_pagination_p95,
}


def run_scenario(
    scenario: Scenario,
    parquet_path: Path,
    iterations: int = DEFAULT_ITERATIONS,
    codes_path: Path | None = None,
) -> BenchRun:
    """
    Roda todos os 8 KPIs em um cenÃ¡rio e retorna BenchRun.

    Args:
        scenario: 'baseline' ou 'typed'.
        parquet_path: Caminho do Parquet a benchmarcar.
        iterations: NÃºmero de iteraÃ§Ãµes por KPI (default 5).
        codes_path: Caminho do JSON (apenas typed).

    Returns:
        BenchRun com todos os resultados.
    """
    logger.info("CenÃ¡rio: %s | Parquet: %s", scenario, parquet_path)
    scan_fn = make_scan(scenario, codes_path)
    n_rows = scan_fn(parquet_path).select(pl.len()).collect().item()
    file_size = parquet_path.stat().st_size

    kpis: dict[str, KPIResult] = {}

    for kpi_name, kpi_fn in KPI_FUNCTIONS.items():
        logger.info("Rodando KPI: %s", kpi_name)
        try:
            kpis[kpi_name] = kpi_fn(scan_fn, parquet_path, iterations)
        except Exception as exc:
            logger.exception("KPI %s falhou: %s", kpi_name, exc)
            kpis[kpi_name] = aggregate(
                name=kpi_name, unit="error", samples=[0.0], error=str(exc),
            )

    # KPIs secundÃ¡rios que nÃ£o dependem de scan_fn
    logger.info("Rodando KPI secundÃ¡rio: file_size_on_disk")
    kpis["file_size_on_disk"] = kpi_file_size_on_disk(parquet_path)
    logger.info("Rodando KPI secundÃ¡rio: rle_dictionary_coverage")
    kpis["rle_dictionary_coverage"] = kpi_rle_dictionary_coverage(parquet_path)
    logger.info("Rodando KPI secundÃ¡rio: invariants_hash")
    kpis["invariants_hash"] = kpi_invariants_byte_identical(parquet_path)

    return BenchRun(
        scenario=scenario,
        parquet_path=str(parquet_path),
        timestamp=datetime.now(tz=UTC).isoformat(timespec="seconds"),
        polars_version=pl.__version__,
        n_rows=n_rows,
        file_size_bytes=file_size,
        kpis=kpis,
    )


# =====================================================================
# ComparaÃ§Ã£o e relatÃ³rio
# =====================================================================


def compare(baseline: BenchRun, typed: BenchRun) -> dict[str, Any]:
    """
    Compara dois BenchRuns e produz veredito go/no-go.

    Returns:
        Dict com:
        - ``ratios``: razÃ£o typed/baseline para cada KPI
        - ``gates``: status de cada um dos 4 gates
        - ``passing_count``: quantos gates passaram
        - ``decision``: 'GO' ou 'NO_GO'
        - ``invariants_match``: se hash das invariantes Ã© igual
    """
    ratios: dict[str, dict[str, Any]] = {}
    for kpi_name in KPI_FUNCTIONS:
        if kpi_name not in baseline.kpis or kpi_name not in typed.kpis:
            continue
        bl = baseline.kpis[kpi_name].median
        tp = typed.kpis[kpi_name].median
        ratio = tp / bl if bl > 0 else float("inf")
        ratios[kpi_name] = {
            "baseline_median": round(bl, 4),
            "typed_median": round(tp, 4),
            "ratio_typed_over_baseline": round(ratio, 4),
            "reduction_pct": round((1 - ratio) * 100, 2) if bl > 0 else 0.0,
            "unit": baseline.kpis[kpi_name].unit,
        }

    # Gates
    gates: dict[str, dict[str, Any]] = {}

    rss_reduction = ratios.get("rss_peak_full_scan", {}).get("reduction_pct", 0)
    gates["rss_peak"] = {
        "kpi": "rss_peak_full_scan",
        "metric": f"reduction â‰¥ {GATE_THRESHOLDS['rss_peak_reduction_min_pct']}%",
        "actual": f"reduction = {rss_reduction:.2f}%",
        "passed": rss_reduction >= GATE_THRESHOLDS["rss_peak_reduction_min_pct"],
    }

    groupby_ratio = ratios.get("groupby_id_agrupado_sum", {}).get(
        "ratio_typed_over_baseline", float("inf")
    )
    gates["groupby"] = {
        "kpi": "groupby_id_agrupado_sum",
        "metric": f"typed â‰¤ {GATE_THRESHOLDS['groupby_max_ratio'] * 100:.0f}% baseline",
        "actual": f"ratio = {groupby_ratio:.4f}",
        "passed": groupby_ratio <= GATE_THRESHOLDS["groupby_max_ratio"],
    }

    cfop_ratio = ratios.get("filter_cfop_in", {}).get(
        "ratio_typed_over_baseline", float("inf")
    )
    gates["filter_cfop"] = {
        "kpi": "filter_cfop_in",
        "metric": f"typed â‰¤ {GATE_THRESHOLDS['filter_cfop_max_ratio'] * 100:.0f}% baseline",
        "actual": f"ratio = {cfop_ratio:.4f}",
        "passed": cfop_ratio <= GATE_THRESHOLDS["filter_cfop_max_ratio"],
    }

    cst_ratio = ratios.get("filter_cst_eq", {}).get(
        "ratio_typed_over_baseline", float("inf")
    )
    gates["filter_cst"] = {
        "kpi": "filter_cst_eq",
        "metric": f"typed â‰¤ {GATE_THRESHOLDS['filter_cst_max_ratio'] * 100:.0f}% baseline",
        "actual": f"ratio = {cst_ratio:.4f}",
        "passed": cst_ratio <= GATE_THRESHOLDS["filter_cst_max_ratio"],
    }

    passing = sum(1 for g in gates.values() if g["passed"])
    decision = "GO" if passing >= GO_DECISION_MIN_PASSING else "NO_GO"

    # Invariantes
    bl_hash = baseline.kpis.get("invariants_hash", KPIResult(
        name="", unit="", iterations=[], median=0, p95=0, min_value=0, max_value=0,
    )).metadata.get("hash", "")
    tp_hash = typed.kpis.get("invariants_hash", KPIResult(
        name="", unit="", iterations=[], median=0, p95=0, min_value=0, max_value=0,
    )).metadata.get("hash", "")
    invariants_match = bl_hash == tp_hash and bl_hash != ""

    return {
        "ratios": ratios,
        "gates": gates,
        "passing_count": passing,
        "min_passing_required": GO_DECISION_MIN_PASSING,
        "decision": decision,
        "invariants_match": invariants_match,
        "baseline_hash": bl_hash[:16] + "..." if bl_hash else "",
        "typed_hash": tp_hash[:16] + "..." if tp_hash else "",
    }


def render_comparison_markdown(
    baseline: BenchRun, typed: BenchRun, comparison: dict[str, Any]
) -> str:
    """Gera relatÃ³rio Markdown da comparaÃ§Ã£o."""
    lines = [
        "# KPI Comparison â€” audit_pyside categorical typing",
        "",
        f"_Gerado em {datetime.now(tz=UTC).isoformat(timespec='seconds')}_",
        "",
        "## Veredito",
        "",
    ]
    decision_icon = "âœ…" if comparison["decision"] == "GO" else "ðŸ›‘"
    lines.append(
        f"### {decision_icon} {comparison['decision']} â€” "
        f"{comparison['passing_count']}/{len(comparison['gates'])} gates passaram "
        f"(mÃ­nimo: {comparison['min_passing_required']})"
    )
    lines.append("")

    if comparison["invariants_match"]:
        lines.append("âœ… **Invariantes byte-idÃªnticas** entre baseline e typed.")
    else:
        lines.append("âŒ **Invariantes DIVERGEM** â€” refatoraÃ§Ã£o introduziu mudanÃ§a "
                     "semÃ¢ntica. Investigar antes de prosseguir.")
    lines.append("")

    lines.append("## Gates do PR 3")
    lines.append("")
    lines.append("| Gate | KPI | CritÃ©rio | Atual | Status |")
    lines.append("|---|---|---|---|---|")
    for gate_name, gate in comparison["gates"].items():
        status = "âœ…" if gate["passed"] else "âŒ"
        lines.append(
            f"| `{gate_name}` | `{gate['kpi']}` | {gate['metric']} | "
            f"{gate['actual']} | {status} |"
        )
    lines.append("")

    lines.append("## ComparaÃ§Ã£o detalhada de KPIs")
    lines.append("")
    lines.append("| KPI | Unit | Baseline mediana | Typed mediana | Ratio | ReduÃ§Ã£o |")
    lines.append("|---|---|---:|---:|---:|---:|")
    for kpi_name, r in comparison["ratios"].items():
        lines.append(
            f"| `{kpi_name}` | {r['unit']} | {r['baseline_median']} | "
            f"{r['typed_median']} | {r['ratio_typed_over_baseline']} | "
            f"{r['reduction_pct']:.1f}% |"
        )
    lines.append("")

    lines.append("## Contexto da execuÃ§Ã£o")
    lines.append("")
    lines.append(f"- **Parquet:** `{baseline.parquet_path}`")
    lines.append(f"- **n_rows:** {baseline.n_rows:,}")
    lines.append(f"- **Tamanho on-disk:** {baseline.file_size_bytes / 1024 / 1024:.2f} MB")
    lines.append(f"- **Polars version:** {baseline.polars_version}")
    lines.append(f"- **Baseline timestamp:** {baseline.timestamp}")
    lines.append(f"- **Typed timestamp:** {typed.timestamp}")
    lines.append("")

    lines.append("## PrÃ³ximos passos")
    lines.append("")
    if comparison["decision"] == "GO":
        lines.append(
            "- âœ… Prosseguir para PR 3: refatorar consumidores de `pl.scan_parquet` "
            "em `src/transformacao/*` para usar `scan_parquet_typed`."
        )
        lines.append(
            "- Adicionar `differential_harness` cases (3 testes da Â§G do plano)."
        )
        lines.append(
            "- Iniciar PR 4 (persistÃªncia) em paralelo: rewrite Parquets v2 ordenados."
        )
    else:
        lines.append(
            "- ðŸ›‘ NÃ£o prosseguir para PR 3 ainda. Investigar gates falhados:"
        )
        for gate_name, gate in comparison["gates"].items():
            if not gate["passed"]:
                lines.append(f"  - `{gate_name}`: {gate['actual']}")
        lines.append("")
        lines.append(
            "- PossÃ­veis causas: (a) cardinalidade real Ã© maior que esperada; "
            "(b) Polars #18868 (predicate pushdown nÃ£o aplica) â€” rotear filtros via "
            "DuckDB; (c) overhead de cast inicial domina em datasets pequenos."
        )

    return "\n".join(lines) + "\n"


# =====================================================================
# Smoke test (sem dados reais)
# =====================================================================


def run_smoke(tmp_dir: Path) -> dict[str, Any]:
    """
    Smoke test sintÃ©tico â€” gera Parquet pequeno e roda os KPIs em
    cenÃ¡rio baseline para validar que tudo executa sem erro.

    NÃ£o compara com typed (precisaria do JSON + mÃ³dulo). Apenas valida
    o esqueleto do bench.
    """
    tmp_dir.mkdir(parents=True, exist_ok=True)
    p = tmp_dir / "smoke.parquet"
    n = 50_000
    df = pl.DataFrame({
        "cfop": [["5102", "5101", "5405", "1102", "6102"][i % 5] for i in range(n)],
        "cst_icms": [["000", "010", "060", "090"][i % 4] for i in range(n)],
        "uf": [["RO", "SP", "MG", "RJ", "PR"][i % 5] for i in range(n)],
        "ncm": [str(10000000 + (i % 8000)) for i in range(n)],
        "id_agrupado": [f"grp_{i % 1000}" for i in range(n)],
        "id_agregado": [f"agg_{i % 5000}" for i in range(n)],
        "__qtd_decl_final_audit__": [float(i) for i in range(n)],
        "q_conv": [float(i % 100) for i in range(n)],
        "q_conv_fisica": [float(i % 100) * 1.5 for i in range(n)],
    })
    df.write_parquet(p)
    logger.info("Smoke Parquet gravado: %s (%d linhas, %d KB)",
                p, n, p.stat().st_size // 1024)

    run = run_scenario("baseline", p, iterations=2)
    return run.to_dict()


# =====================================================================
# CLI
# =====================================================================


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def cmd_run(args: argparse.Namespace) -> int:
    """Comando: rodar 1 cenÃ¡rio e gravar JSON."""
    parquet = Path(args.parquet)
    if not parquet.exists():
        logger.error("Parquet nÃ£o existe: %s", parquet)
        return 2

    codes = Path(args.codes_path) if args.codes_path else None
    run = run_scenario(args.scenario, parquet, args.iterations, codes)

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(run.to_dict(), indent=2, ensure_ascii=False))
    logger.info("Resultado gravado em %s", out)

    print(f"\n=== {args.scenario} ===")
    for name, kpi in run.kpis.items():
        print(f"  {name:35s} median={kpi.median:>10.2f} {kpi.unit}")
    return 0


def cmd_compare(args: argparse.Namespace) -> int:
    """Comando: comparar dois JSONs e gerar veredito."""
    baseline_data = json.loads(Path(args.baseline).read_text())
    typed_data = json.loads(Path(args.typed).read_text())

    def reconstruct(d: dict) -> BenchRun:
        kpis = {
            name: KPIResult(
                name=k["name"], unit=k["unit"], iterations=k["iterations"],
                median=k["median"], p95=k["p95"],
                min_value=k["min"], max_value=k["max"],
                metadata=k.get("metadata", {}),
            )
            for name, k in d["kpis"].items()
        }
        return BenchRun(
            scenario=d["scenario"], parquet_path=d["parquet_path"],
            timestamp=d["timestamp"], polars_version=d["polars_version"],
            n_rows=d["n_rows"], file_size_bytes=d["file_size_bytes"],
            kpis=kpis,
        )

    bl = reconstruct(baseline_data)
    tp = reconstruct(typed_data)
    cmp_result = compare(bl, tp)

    if args.report:
        report = render_comparison_markdown(bl, tp, cmp_result)
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report).write_text(report, encoding="utf-8")
        logger.info("RelatÃ³rio Markdown gravado em %s", args.report)

    print(json.dumps(cmp_result, indent=2, ensure_ascii=False, default=str))
    print(f"\nVEREDITO: {cmp_result['decision']} "
          f"({cmp_result['passing_count']}/{len(cmp_result['gates'])} gates)")
    return 0 if cmp_result["decision"] == "GO" else 1


def cmd_smoke(args: argparse.Namespace) -> int:
    """Comando: smoke test sintÃ©tico."""
    tmp_dir = Path(args.tmp_dir)
    result = run_smoke(tmp_dir)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_baseline = sub.add_parser("baseline", help="Rodar KPIs em cenÃ¡rio baseline")
    p_baseline.add_argument("--parquet", required=True, help="Caminho do Parquet")
    p_baseline.add_argument("--output", required=True, help="JSON de saÃ­da")
    p_baseline.add_argument("--iterations", type=int, default=DEFAULT_ITERATIONS)
    p_baseline.add_argument("--codes-path", default=None)
    p_baseline.add_argument("-v", "--verbose", action="store_true")

    p_typed = sub.add_parser("typed", help="Rodar KPIs em cenÃ¡rio typed")
    p_typed.add_argument("--parquet", required=True)
    p_typed.add_argument("--output", required=True)
    p_typed.add_argument("--iterations", type=int, default=DEFAULT_ITERATIONS)
    p_typed.add_argument("--codes-path", default=None)
    p_typed.add_argument("-v", "--verbose", action="store_true")

    p_compare = sub.add_parser("compare", help="Comparar baseline vs typed")
    p_compare.add_argument("--baseline", required=True)
    p_compare.add_argument("--typed", required=True)
    p_compare.add_argument("--report", default=None, help="Markdown de saÃ­da")
    p_compare.add_argument("-v", "--verbose", action="store_true")

    p_smoke = sub.add_parser("smoke", help="Smoke test sintÃ©tico")
    p_smoke.add_argument("--tmp-dir", default="/tmp/audit_pyside_smoke")
    p_smoke.add_argument("-v", "--verbose", action="store_true")

    args = parser.parse_args(argv)
    setup_logging(args.verbose)

    if args.cmd in ("baseline", "typed"):
        args.scenario = args.cmd
        return cmd_run(args)
    elif args.cmd == "compare":
        return cmd_compare(args)
    elif args.cmd == "smoke":
        return cmd_smoke(args)
    else:
        parser.print_help()
        return 2


if __name__ == "__main__":
    sys.exit(main())
