"""
bench/capture_baseline.py
==========================

Wrapper de conveniência para captura inicial do baseline de KPIs
em CNPJ real, agregando múltiplos Parquets fiscais e gerando um
relatório consolidado.

Uso típico (executar uma vez por release/onda):

    uv run python bench/capture_baseline.py \\
        --root /data/audit_pyside/parquets/04240370002877 \\
        --output bench/results/baseline_committed.json \\
        --report bench/results/baseline_report.md \\
        --iterations 5

O que faz:
  1. Descobre todos os Parquets fiscais grandes (≥ N MB) em ``--root``
  2. Roda KPIs do ``run_kpis.py baseline`` em cada um
  3. Agrega resultados em um único JSON com mediana ponderada por linhas
  4. Gera relatório Markdown consolidado
  5. Recomenda commitar o JSON em ``bench/results/baseline_committed.json``

Por que existe:
  ``bench/run_kpis.py baseline`` roda em UM Parquet de cada vez.
  Para baseline confiável em produção, queremos média de várias
  tabelas (tb_documentos, c170_xml, calculos_mensais, etc.). Este
  wrapper faz a coleta automatizada.

Ver: Notion 358edc8b7d5d81cfb33ce023d4cee84f §F (KPIs SMART).
"""

from __future__ import annotations

import argparse
import json
import logging
import statistics
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

#: Tamanho mínimo do Parquet (em MB) para entrar na agregação.
#: Parquets pequenos têm overhead relativo grande e poluem a mediana.
DEFAULT_MIN_SIZE_MB = 100

#: Padrões de nomes de Parquet fiscal que queremos benchmarcar.
#: Outros (p.ex. fixtures, samples) são ignorados.
PARQUET_PATTERNS = [
    "tb_documentos",
    "c170_xml",
    "c176_xml",
    "movimentacao_estoque",
    "produtos_final",
    "calculos_mensais",
    "calculos_anuais",
    "fatores_conversao",
]


def discover_target_parquets(root: Path, min_size_mb: int) -> list[Path]:
    """
    Encontra Parquets fiscais relevantes para baseline.

    Critérios:
      1. Termina em .parquet
      2. Tamanho >= min_size_mb
      3. Stem ou parent name casa um dos PARQUET_PATTERNS
    """
    candidates: list[Path] = []
    for path in sorted(root.rglob("*.parquet")):
        size_mb = path.stat().st_size / 1024 / 1024
        if size_mb < min_size_mb:
            continue
        # Match por stem ou diretório pai
        haystack = f"{path.stem} {path.parent.name}".lower()
        if not any(p in haystack for p in PARQUET_PATTERNS):
            continue
        candidates.append(path)
    return candidates


def run_baseline_for_parquet(
    parquet: Path, output: Path, iterations: int, codes_path: Path | None
) -> dict[str, Any]:
    """
    Invoca ``bench/run_kpis.py baseline`` para um Parquet específico.

    Returns:
        Dicionário decodificado do JSON de saída.

    Raises:
        RuntimeError: Se o subprocess falhar.
    """
    cmd = [
        sys.executable, "-m", "bench.run_kpis", "baseline",
        "--parquet", str(parquet),
        "--output", str(output),
        "--iterations", str(iterations),
    ]
    if codes_path is not None:
        cmd.extend(["--codes-path", str(codes_path)])

    logger.info("Rodando baseline para %s", parquet.name)
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"baseline falhou para {parquet}: {result.stderr or result.stdout}"
        )
    return json.loads(output.read_text(encoding="utf-8"))


def aggregate_runs(
    runs: list[dict[str, Any]], parquet_paths: list[Path]
) -> dict[str, Any]:
    """
    Agrega múltiplos BenchRuns em um único baseline consolidado.

    Estratégia:
      - Para KPIs em ms ou MB: mediana das medianas (robusto a outliers)
      - Para KPIs em pct: mediana
      - Para hash: lista (não tem média semântica)
      - Metadados consolidados: total de linhas, total de bytes, lista
        de parquets benchmarcados
    """
    if not runs:
        return {}

    # Coletar nomes de KPIs do primeiro run (todos têm os mesmos)
    kpi_names = set(runs[0].get("kpis", {}).keys())
    for r in runs[1:]:
        kpi_names &= set(r.get("kpis", {}).keys())

    aggregated_kpis: dict[str, Any] = {}
    for name in sorted(kpi_names):
        medians = []
        units = set()
        n_iterations: list[int] = []
        for r in runs:
            kpi = r["kpis"][name]
            medians.append(kpi["median"])
            units.add(kpi["unit"])
            n_iterations.append(len(kpi.get("iterations", [])))

        aggregated_kpis[name] = {
            "name": name,
            "unit": next(iter(units)) if len(units) == 1 else "mixed",
            "median_of_medians": round(statistics.median(medians), 4),
            "p95_of_medians": round(
                sorted(medians)[max(0, int(len(medians) * 0.95) - 1)], 4
            ),
            "min_median": round(min(medians), 4),
            "max_median": round(max(medians), 4),
            "n_runs": len(runs),
            "per_run_medians": [round(m, 4) for m in medians],
        }

    # Sumário por Parquet
    per_parquet = []
    for path, run in zip(parquet_paths, runs, strict=False):
        per_parquet.append({
            "path": str(path),
            "n_rows": run.get("n_rows", 0),
            "file_size_bytes": run.get("file_size_bytes", 0),
            "kpis_summary": {
                k: round(v["median"], 4) for k, v in run["kpis"].items()
            },
        })

    return {
        "schema_version": 1,
        "captured_at": datetime.now(tz=UTC).isoformat(timespec="seconds"),
        "polars_version": runs[0].get("polars_version", "unknown"),
        "n_parquets": len(runs),
        "total_rows": sum(r.get("n_rows", 0) for r in runs),
        "total_size_bytes": sum(r.get("file_size_bytes", 0) for r in runs),
        "parquets": per_parquet,
        "aggregated_kpis": aggregated_kpis,
    }


def render_baseline_report(aggregated: dict[str, Any]) -> str:
    """Gera relatório Markdown legível do baseline."""
    if not aggregated:
        return "# Baseline\n\nNenhum dado.\n"

    total_mb = aggregated["total_size_bytes"] / 1024 / 1024
    lines = [
        "# Baseline KPIs — audit_pyside",
        "",
        f"_Capturado em {aggregated['captured_at']}._",
        "",
        "## Sumário",
        "",
        f"- **Parquets benchmarcados:** {aggregated['n_parquets']}",
        f"- **Total de linhas:** {aggregated['total_rows']:,}",
        f"- **Tamanho total on-disk:** {total_mb:,.1f} MB",
        f"- **Polars version:** {aggregated['polars_version']}",
        "",
        "## KPIs agregados (mediana das medianas por Parquet)",
        "",
        "| KPI | Unit | Mediana | P95 | Min | Max | Runs |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for kpi in aggregated["aggregated_kpis"].values():
        lines.append(
            f"| `{kpi['name']}` | {kpi['unit']} | "
            f"{kpi['median_of_medians']} | {kpi['p95_of_medians']} | "
            f"{kpi['min_median']} | {kpi['max_median']} | {kpi['n_runs']} |"
        )
    lines.append("")

    lines.append("## Detalhe por Parquet")
    lines.append("")
    for p in aggregated["parquets"]:
        size_mb = p["file_size_bytes"] / 1024 / 1024
        lines.append(f"### `{Path(p['path']).name}`")
        lines.append("")
        lines.append(f"- Path: `{p['path']}`")
        lines.append(f"- Linhas: {p['n_rows']:,}")
        lines.append(f"- Tamanho: {size_mb:,.1f} MB")
        lines.append("- KPIs (mediana):")
        for kpi_name, kpi_value in p["kpis_summary"].items():
            lines.append(f"  - `{kpi_name}`: {kpi_value}")
        lines.append("")

    lines.append("## Como usar este baseline")
    lines.append("")
    lines.append(
        "Após gerar este JSON, **comitá-lo** em "
        "`bench/results/baseline_committed.json` para que o workflow "
        "`perf-gates.yml` da CI tenha referência. Para regenerar:"
    )
    lines.append("")
    lines.append("```bash")
    lines.append("uv run python bench/capture_baseline.py \\")
    lines.append("    --root /data/audit_pyside/parquets/<CNPJ> \\")
    lines.append("    --output bench/results/baseline_committed.json \\")
    lines.append("    --report bench/results/baseline_report.md")
    lines.append("```")
    lines.append("")
    lines.append(
        "Repetir a cada release/onda significativa, ou anualmente "
        "quando o cadastro `ref/fiscal_codes_YYYY.json` for atualizado."
    )

    return "\n".join(lines) + "\n"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--root", type=Path, required=True,
        help="Diretório raiz com Parquets fiscais (ex.: /data/.../<CNPJ>).",
    )
    parser.add_argument(
        "--output", type=Path, required=True,
        help="JSON consolidado de saída (commitar em bench/results/).",
    )
    parser.add_argument(
        "--report", type=Path, default=None,
        help="Markdown report opcional (default: ao lado do --output).",
    )
    parser.add_argument(
        "--iterations", type=int, default=5,
        help="Iterações por KPI (default: 5).",
    )
    parser.add_argument(
        "--min-size-mb", type=int, default=DEFAULT_MIN_SIZE_MB,
        help=f"Tamanho mínimo de Parquet (default: {DEFAULT_MIN_SIZE_MB}).",
    )
    parser.add_argument(
        "--codes-path", type=Path, default=None,
        help="Caminho de fiscal_codes_YYYY.json (default: ref/fiscal_codes_2026.json).",
    )
    parser.add_argument(
        "--max-parquets", type=int, default=10,
        help="Limite de Parquets benchmarcados (default: 10).",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
    )

    if not args.root.exists():
        logger.error("--root não existe: %s", args.root)
        return 2

    parquets = discover_target_parquets(args.root, args.min_size_mb)
    if not parquets:
        logger.error(
            "Nenhum Parquet ≥ %d MB encontrado em %s. Tente --min-size-mb menor.",
            args.min_size_mb, args.root,
        )
        return 1

    if len(parquets) > args.max_parquets:
        logger.warning(
            "Encontrados %d Parquets; limitando aos %d primeiros (use --max-parquets)",
            len(parquets), args.max_parquets,
        )
        parquets = parquets[: args.max_parquets]

    logger.info("Rodando baseline em %d Parquets", len(parquets))
    runs = []
    tmp_dir = args.output.parent / ".capture_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    for i, p in enumerate(parquets, 1):
        tmp_out = tmp_dir / f"run_{i:02d}_{p.stem}.json"
        try:
            run = run_baseline_for_parquet(p, tmp_out, args.iterations, args.codes_path)
            runs.append(run)
            logger.info("✓ %d/%d %s", i, len(parquets), p.name)
        except Exception as exc:
            logger.error("✗ %s falhou: %s", p, exc)

    if not runs:
        logger.error("Nenhuma run bem-sucedida — abortando")
        return 1

    aggregated = aggregate_runs(runs, parquets[: len(runs)])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(aggregated, indent=2, ensure_ascii=False))
    logger.info("JSON agregado gravado em %s", args.output)

    report_path = args.report or args.output.with_suffix(".md")
    report_path.write_text(render_baseline_report(aggregated), encoding="utf-8")
    logger.info("Report Markdown gravado em %s", report_path)

    print("\n=== Baseline capturado ===")
    print(f"  Parquets: {len(runs)}")
    print(f"  Total rows: {aggregated['total_rows']:,}")
    print(f"  JSON: {args.output}")
    print(f"  Report: {report_path}")
    print(f"\nPróximo passo: commitar {args.output} no repo.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
