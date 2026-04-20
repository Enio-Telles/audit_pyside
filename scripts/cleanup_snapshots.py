from __future__ import annotations

from pathlib import Path
import argparse
from typing import Dict


def cleanup_all_cnpjs(base_dir: Path | None = None, keep_last: int = 10, keep_days: int = 180, dry_run: bool = False) -> Dict[str, int]:
    """
    Iterate over CNPJ folders under `base_dir` and remove snapshot files.

    This script performs file-based cleanup without importing the UI service layer,
    so it can run in lightweight environments and in tests that don't expose
    the application package on sys.path.

    Returns a mapping of cnpj -> number of snapshots removed.
    """
    from datetime import datetime, timedelta

    base = Path(base_dir) if base_dir is not None else Path.cwd()
    results: Dict[str, int] = {}

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
            results[cnpj] = 0
            continue

        snaps_sorted = sorted(snaps, key=lambda p: p.stat().st_mtime, reverse=True)
        removed = 0

        for idx, p in enumerate(snaps_sorted):
            if idx < keep_last:
                continue
            if dry_run:
                print(f"[DRY] would remove: {p}")
                continue

            if cutoff is None:
                p.unlink()
                removed += 1
            else:
                mtime = datetime.fromtimestamp(p.stat().st_mtime)
                if mtime < cutoff:
                    p.unlink()
                    removed += 1

        results[cnpj] = removed
        print(f"{cnpj}: removed {removed} snapshot(s)")

    return results


def _main():
    p = argparse.ArgumentParser(description="Cleanup manual-map snapshots for all CNPJs under a base folder")
    p.add_argument("--base", help="Base path for CNPJ folders (overrides config)")
    p.add_argument("--keep-last", type=int, default=10)
    p.add_argument("--keep-days", type=int, default=180)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    base = Path(args.base) if args.base else None
    cleanup_all_cnpjs(base, keep_last=args.keep_last, keep_days=args.keep_days, dry_run=args.dry_run)


if __name__ == "__main__":
    _main()
