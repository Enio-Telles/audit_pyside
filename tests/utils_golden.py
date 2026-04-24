from __future__ import annotations

import hashlib
import json
import math
import os
from datetime import datetime
from pathlib import Path

import polars as pl

_HASHES_PATH = Path(__file__).with_name("golden_hashes.json")


def _normalize_for_hash(value: object) -> object:
    """Convert nested or non-JSON-native values into deterministic structures."""
    if isinstance(value, dict):
        return {str(key): _normalize_for_hash(val) for key, val in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_normalize_for_hash(item) for item in value]
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    return value


def calculate_df_hash(df: pl.DataFrame, cols: list[str] | None = None) -> str:
    """Return a deterministic SHA256 hash for a Polars DataFrame."""
    if cols:
        selected_cols = [col for col in cols if col in df.columns]
        if not selected_cols:
            raise ValueError("No requested golden columns exist in the DataFrame.")
        df = df.select(selected_cols)

    try:
        if df.columns:
            df = df.sort(df.columns, nulls_last=True)
        payload = df.write_csv()
    except Exception:
        row_payloads = [
            json.dumps(
                _normalize_for_hash(row),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            )
            for row in df.to_dicts()
        ]
        payload = "\n".join(sorted(row_payloads))

    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def verify_golden_hash(
    df: pl.DataFrame,
    test_id: str,
    cols: list[str] | None = None,
    update: bool = False,
) -> bool:
    """Compare the current DataFrame hash against the persisted golden hash."""
    current_hash = calculate_df_hash(df, cols)

    golden_hashes: dict[str, dict[str, object]] = {}
    if _HASHES_PATH.exists():
        try:
            golden_hashes = json.loads(_HASHES_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            golden_hashes = {}

    should_update = update or test_id not in golden_hashes or bool(os.environ.get("UPDATE_GOLDEN"))
    if should_update:
        golden_hashes[test_id] = {
            "hash": current_hash,
            "cols": cols,
            "last_updated": datetime.now().isoformat(),
        }
        _HASHES_PATH.write_text(
            json.dumps(golden_hashes, indent=4, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        return True

    expected_hash = str(golden_hashes[test_id]["hash"])
    if current_hash != expected_hash:
        raise AssertionError(
            f"GOLDEN HASH MISMATCH for '{test_id}'!\n"
            f"Expected: {expected_hash}\n"
            f"Actual:   {current_hash}\n"
            "Hint: if the change is intentional, run with UPDATE_GOLDEN=1."
        )

    return True
