#!/usr/bin/env python3
"""Gerar índice de arquivos Parquet no repositório.

Exemplo:
  python scripts/generate_parquet_references.py --root . --out-dir docs/referencias --max-rows 3

Use `--dry-run` para imprimir o resultado em stdout sem gravar arquivo.
"""

from __future__ import annotations
import argparse
from pathlib import Path
from datetime import datetime
from typing import Iterable

try:
    import polars as pl
except Exception as exc:  # pragma: no cover - runtime requirement
    raise RuntimeError("Polars is required to run this script") from exc


def human_size(n: int) -> str:
    n = float(n)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024.0:
            return f"{n:.1f}{unit}"
        n /= 1024.0
    return f"{n:.1f}PB"


def find_parquet_files(root: Path, pattern: str = "*.parquet", ignore: Iterable[str] | None = None) -> list[Path]:
    ignore_set = set(ignore or [])
    defaults = {"docs/referencias", ".venv", "venv", "__pycache__", ".git"}
    ignore_set |= defaults
    files = sorted([p for p in root.rglob(pattern) if not any(part in ignore_set for part in p.parts)])
    return files


def build_index(lines: list[str], files: list[Path], max_rows: int = 3) -> list[str]:
    for p in files:
        rel = p.as_posix()
        if rel.startswith("docs/referencias"):
            continue
        size = p.stat().st_size
        hr = human_size(size)
        lines.append(f"## {rel}\n")
        lines.append(f"- Caminho: {rel}\n")
        lines.append(f"- Tamanho: {hr} ({size} bytes)\n")
        try:
            df = pl.read_parquet(str(p))
            rows = df.height
            schema = df.schema
            lines.append(f"- Linhas: {rows}\n")
            lines.append("- Colunas:\n")
            for k, v in schema.items():
                lines.append(f"  - `{k}`: {v}\n")
            sample = df.head(max_rows)
            if sample.height > 0:
                cols = sample.columns
                header = "| " + " | ".join(cols) + " |"
                sep = "| " + " | ".join(["---"] * len(cols)) + " |"
                lines.append("\nAmostra (até {0} linhas):\n\n".format(max_rows))
                lines.append(header + "\n")
                lines.append(sep + "\n")
                for row in sample.to_dicts():
                    row_vals = []
                    for c in cols:
                        v = row.get(c, "")
                        if v is None:
                            s = ""
                        else:
                            s = str(v)
                        s = s.replace("\n", " ").replace("|", "\\|")
                        if len(s) > 120:
                            s = s[:117] + "..."
                        row_vals.append(s)
                    lines.append("| " + " | ".join(row_vals) + " |\n")
        except Exception as e:
            lines.append(f"- Erro ao ler: {e}\n")
        lines.append("\n---\n\n")
    return lines


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Generate Parquet references index for the repository")
    p.add_argument("--root", "-r", default=".", help="Root path to search for parquet files")
    p.add_argument("--out-dir", "-o", default="docs/referencias", help="Output directory for the index")
    p.add_argument("--out-file", default="parquet_referencias.md", help="Output markdown filename")
    p.add_argument("--pattern", default="*.parquet", help="Glob pattern to search for files")
    p.add_argument("--max-rows", "-n", type=int, default=3, help="Max rows to include as sample")
    p.add_argument("--ignore", "-i", action="append", default=[], help="Paths to ignore (can be repeated)")
    p.add_argument("--dry-run", action="store_true", help="Print result to stdout instead of writing file")
    args = p.parse_args(argv)

    root = Path(args.root)
    files = find_parquet_files(root, pattern=args.pattern, ignore=args.ignore)

    out_dir = Path(args.out_dir)
    out_md = out_dir / args.out_file

    header_lines: list[str] = []
    header_lines.append("# Referências de arquivos Parquet\n")
    header_lines.append(f"Gerado em {datetime.utcnow().isoformat()} UTC\n\n")
    header_lines.append("Listagem de arquivos .parquet detectados no repositório com metadados e amostras.\n\n")

    lines = build_index(header_lines, files, max_rows=args.max_rows)

    content = "".join(lines)
    if args.dry_run:
        print(content)
        print(f"Found {len(files)} parquet files")
    else:
        out_dir.mkdir(parents=True, exist_ok=True)
        out_md.write_text(content, encoding="utf-8")
        print(f"Gerado {out_md} — arquivos processados: {len(files)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
