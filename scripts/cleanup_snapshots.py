from __future__ import annotations

from pathlib import Path
import argparse
from typing import Dict, Any
import json


def cleanup_all_cnpjs_detailed(base_dir: Path | None = None, keep_last: int = 10, keep_days: int = 180, dry_run: bool = False) -> Dict[str, Dict[str, Any]]:
    """
    Iterate over CNPJ folders under `base_dir` and remove snapshot files.

    This script performs file-based cleanup without importing the UI service layer,
    so it can run in lightweight environments and in tests that don't expose
    the application package on sys.path.

        Returns a mapping of cnpj -> detailed result dict with keys:
            - removed: int
            - removed_files: list[str]
            - kept_files: list[str]
            - would_remove_files: list[str] (present only if dry_run True)
    """
    from datetime import datetime, timedelta

    base = Path(base_dir) if base_dir is not None else Path.cwd()
    results: Dict[str, Dict[str, Any]] = {}

    if not base.exists():
        return results

    cutoff = None
    if keep_days and keep_days > 0:
        cutoff = datetime.now() - timedelta(days=keep_days)

    for child in sorted(base.iterdir()):
        if not child.is_dir():
            continue
        cnpj = child.name
        snapshots_dir = child / "analises" / "produtos" / "snapshots"
        if not snapshots_dir.exists():
            continue

        snaps = list(snapshots_dir.glob(f"mapa_agrupamento_manual_{cnpj}_*.parquet"))
        if not snaps:
            results[cnpj] = {"removed": 0, "removed_files": [], "kept_files": [], "would_remove_files": []}
            continue

        snaps_sorted = sorted(snaps, key=lambda p: p.stat().st_mtime, reverse=True)
        removed = 0
        removed_files: list[str] = []
        kept_files: list[str] = []
        would_remove: list[str] = []

        for idx, p in enumerate(snaps_sorted):
            if idx < keep_last:
                kept_files.append(str(p))
                continue

            if dry_run:
                print(f"[DRY] would remove: {p}")
                would_remove.append(str(p))
                continue

            if cutoff is None:
                removed_files.append(str(p))
                p.unlink()
                removed += 1
            else:
                mtime = datetime.fromtimestamp(p.stat().st_mtime)
                if mtime < cutoff:
                    removed_files.append(str(p))
                    p.unlink()
                    removed += 1
                else:
                    kept_files.append(str(p))

        detail = {"removed": removed, "removed_files": removed_files, "kept_files": kept_files}
        if dry_run:
            detail["would_remove_files"] = would_remove
        results[cnpj] = detail
        print(f"{cnpj}: removed {removed} snapshot(s)")

    return results


def cleanup_all_cnpjs(base_dir: Path | None = None, keep_last: int = 10, keep_days: int = 180, dry_run: bool = False) -> Dict[str, int]:
    """Backward-compatible wrapper that returns mapping cnpj->removed_count."""
    detailed = cleanup_all_cnpjs_detailed(base_dir=base_dir, keep_last=keep_last, keep_days=keep_days, dry_run=dry_run)
    return {k: int(v.get("removed", 0)) for k, v in detailed.items()}


def _main():
    p = argparse.ArgumentParser(description="Cleanup manual-map snapshots for all CNPJs under a base folder")
    p.add_argument("--base", help="Base path for CNPJ folders (overrides config)")
    p.add_argument("--keep-last", type=int, default=10)
    p.add_argument("--keep-days", type=int, default=180)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--json-report", help="Path to write JSON report of removed snapshots (optional)")
    args = p.parse_args()

    base = Path(args.base) if args.base else None
    detailed = cleanup_all_cnpjs_detailed(base, keep_last=args.keep_last, keep_days=args.keep_days, dry_run=args.dry_run)

    if args.json_report:
        report_path = Path(args.json_report)
    else:
        report_path = Path("cleanup-removed-snapshots.json")

    # Serialize the detailed report
    out = {k: v for k, v in detailed.items()}
    try:
        with report_path.open("w", encoding="utf-8") as fh:
            json.dump(out, fh, ensure_ascii=False, indent=2)
        print(f"Wrote JSON report to {report_path}")
    except Exception as e:
        print(f"Failed to write JSON report: {e}")


if __name__ == "__main__":
    _main()
