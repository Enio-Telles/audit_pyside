"""CLI operacional para batch rewrite side-by-side de Parquets v1 -> v2.

Este script encapsula ``src.io.categorical_writer.batch_rewrite_parquets`` sem
alterar paths de producao nem trocar consumidores. Use ``--dry-run`` para
gerar o plano antes de qualquer escrita.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.io.categorical_writer import batch_rewrite_parquets


@dataclass(frozen=True)
class RewritePlanItem:
    input_path: str
    output_path: str
    size_bytes: int


@dataclass(frozen=True)
class RewritePlan:
    input_root: str
    output_root: str
    dry_run: bool
    strict_cast: bool
    min_size_mb: float
    max_files: int | None
    files: list[RewritePlanItem]

    @property
    def total_size_bytes(self) -> int:
        return sum(item.size_bytes for item in self.files)


def discover_parquets(
    input_root: Path,
    output_root: Path,
    *,
    min_size_mb: float = 0,
    max_files: int | None = None,
) -> list[RewritePlanItem]:
    """Descobre Parquets v1 e calcula seus paths v2 preservando arvore relativa."""
    input_root = input_root.resolve()
    output_root = output_root.resolve()

    if not input_root.exists():
        raise FileNotFoundError(f"Diretorio de entrada nao encontrado: {input_root}")
    if input_root == output_root:
        raise ValueError("output_root deve ser diferente de input_root")

    parquet_files = sorted(input_root.rglob("*.parquet"))
    if min_size_mb > 0:
        min_bytes = min_size_mb * 1024 * 1024
        parquet_files = [path for path in parquet_files if path.stat().st_size >= min_bytes]
    if max_files is not None and max_files > 0:
        parquet_files = parquet_files[:max_files]

    return [
        RewritePlanItem(
            input_path=str(path),
            output_path=str(output_root / path.relative_to(input_root)),
            size_bytes=path.stat().st_size,
        )
        for path in parquet_files
    ]


def build_plan(args: argparse.Namespace) -> RewritePlan:
    input_root = Path(args.input_root)
    output_root = Path(args.output_root)
    return RewritePlan(
        input_root=str(input_root.resolve()),
        output_root=str(output_root.resolve()),
        dry_run=args.dry_run,
        strict_cast=args.strict_cast,
        min_size_mb=args.min_size_mb,
        max_files=args.max_files,
        files=discover_parquets(
            input_root,
            output_root,
            min_size_mb=args.min_size_mb,
            max_files=args.max_files,
        ),
    )


def plan_to_summary(
    plan: RewritePlan, results: list[dict[str, Any]] | None = None
) -> dict[str, Any]:
    ok_count = len(results or [])
    planned_count = len(plan.files)
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "input_root": plan.input_root,
        "output_root": plan.output_root,
        "dry_run": plan.dry_run,
        "strict_cast": plan.strict_cast,
        "min_size_mb": plan.min_size_mb,
        "max_files": plan.max_files,
        "planned_count": planned_count,
        "total_size_bytes": plan.total_size_bytes,
        "ok_count": ok_count if results is not None else None,
        "failed_or_skipped_count": planned_count - ok_count if results is not None else None,
        "files": [asdict(item) for item in plan.files],
        "results": results or [],
    }


def write_json_report(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")


def write_markdown_report(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Batch rewrite Parquets v2",
        "",
        f"- Gerado em: `{summary['generated_at']}`",
        f"- Entrada v1: `{summary['input_root']}`",
        f"- Saida v2: `{summary['output_root']}`",
        f"- Dry-run: `{summary['dry_run']}`",
        f"- Strict cast: `{summary['strict_cast']}`",
        f"- Arquivos planejados: `{summary['planned_count']}`",
        f"- Tamanho total planejado: `{summary['total_size_bytes']}` bytes",
    ]
    if summary["ok_count"] is not None:
        lines.extend(
            [
                f"- Rewrites OK: `{summary['ok_count']}`",
                f"- Falhas ou skips: `{summary['failed_or_skipped_count']}`",
            ]
        )
    lines.extend(["", "## Arquivos", "", "| Entrada | Saida | Bytes |", "|---|---|---|"])
    for item in summary["files"]:
        lines.append(
            f"| `{item['input_path']}` | `{item['output_path']}` | `{item['size_bytes']}` |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Planeja ou executa batch rewrite side-by-side de Parquets v1 para v2.",
    )
    parser.add_argument("--input-root", required=True, help="Diretorio raiz com Parquets v1.")
    parser.add_argument("--output-root", required=True, help="Diretorio raiz para Parquets v2.")
    parser.add_argument("--codes-path", type=Path, default=None, help="JSON fiscal opcional.")
    parser.add_argument(
        "--dry-run", action="store_true", help="Apenas planeja; nao escreve Parquets."
    )
    parser.add_argument("--no-strict-cast", dest="strict_cast", action="store_false")
    parser.set_defaults(strict_cast=True)
    parser.add_argument("--min-size-mb", type=float, default=0)
    parser.add_argument("--max-files", type=int, default=None)
    parser.add_argument("--compression", default="zstd")
    parser.add_argument("--compression-level", type=int, default=None)
    parser.add_argument("--row-group-size", type=int, default=None)
    parser.add_argument("--report-json", type=Path, default=None)
    parser.add_argument("--report-md", type=Path, default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    plan = build_plan(args)

    results: list[dict[str, Any]] | None = None
    if not args.dry_run:
        # Usar o mesmo plano de descoberta — zero divergencia
        file_list = [Path(item.input_path) for item in plan.files]
        results = batch_rewrite_parquets(
            plan.input_root,
            plan.output_root,
            file_list=file_list,
            codes_path=args.codes_path,
            strict_cast=args.strict_cast,
            compression=args.compression,
            compression_level=args.compression_level,
            row_group_size=args.row_group_size,
        )

    summary = plan_to_summary(plan, results)
    if args.report_json:
        write_json_report(args.report_json, summary)
    if args.report_md:
        write_markdown_report(args.report_md, summary)

    print(json.dumps({k: v for k, v in summary.items() if k not in {"files", "results"}}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
