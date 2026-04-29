#!/usr/bin/env python3
"""
Convert Parquet files in docs/referencias/CO_SEFIN to CSV files.
Saves CSVs into docs/referencias/CO_SEFIN/csv

Run from the repository root:
    python scripts/convert_co_sefin_to_csv.py
"""
from pathlib import Path
import sys

try:
    import polars as pl
except Exception:
    pl = None


def main():
    repo_root = Path(__file__).resolve().parents[1]
    src_dir = repo_root / "docs" / "referencias" / "CO_SEFIN"
    dst_dir = src_dir / "csv"
    dst_dir.mkdir(parents=True, exist_ok=True)

    if not src_dir.exists():
        print(f"Source directory not found: {src_dir}")
        sys.exit(1)

    parquet_files = sorted([p for p in src_dir.iterdir() if p.suffix.lower() == ".parquet"])
    if not parquet_files:
        print(f"No parquet files found in {src_dir}")
        return

    for p in parquet_files:
        print(f"Reading {p.name}...")
        try:
            if pl is not None:
                df = pl.read_parquet(p)
            else:
                # fallback to pandas if polars is not available
                import pandas as pd
                df = pd.read_parquet(p)
        except Exception as e:
            print(f"Failed to read {p}: {e}")
            continue

        out_path = dst_dir / (p.stem + ".csv")
        print(f"Writing {out_path.name}...")
        try:
            # polars DataFrame has write_csv; pandas DataFrame has to_csv
            if pl is not None and hasattr(df, "write_csv"):
                df.write_csv(out_path)
            else:
                # pandas DataFrame
                df.to_csv(out_path, index=False)
        except Exception as e:
            print(f"Failed to write {out_path}: {e}")
            continue

    print("Conversion finished.")


if __name__ == "__main__":
    main()
