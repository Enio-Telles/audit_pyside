"""
Migrate app_state JSON files by removing any persisted `_active_load_workers` keys
that may contain lists and later cause runtime attribute type errors.

This script:
- Scans `workspace/app_state` for `.json` files (recursively).
- Loads each JSON file and recursively removes any key named `_active_load_workers`.
- Writes a backup of the original file as `<file>.bak.YYYYmmddHHMMSS` before updating.

Run from repository root:
  python scripts/migrate_app_state_active_load_workers.py

"""

from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime
import shutil


ROOT = Path(__file__).resolve().parents[1]
APP_STATE_DIR = ROOT / "workspace" / "app_state"


def traverse_and_remove(obj) -> bool:
    """Recursively remove keys named '_active_load_workers'.

    Returns True if mutation occurred.
    """
    changed = False
    if isinstance(obj, dict):
        if "_active_load_workers" in obj:
            del obj["_active_load_workers"]
            changed = True
        # Recurse into remaining values
        for k, v in list(obj.items()):
            if traverse_and_remove(v):
                changed = True
    elif isinstance(obj, list):
        for item in obj:
            if traverse_and_remove(item):
                changed = True
    return changed


def migrate_file(path: Path) -> bool:
    try:
        text = path.read_text(encoding="utf-8")
        data = json.loads(text)
    except Exception:
        return False

    if not traverse_and_remove(data):
        return False

    # backup
    stamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    bak = path.with_suffix(path.suffix + f".bak.{stamp}")
    shutil.copy2(path, bak)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    return True


def main():
    if not APP_STATE_DIR.exists():
        print(f"App state directory not found: {APP_STATE_DIR}")
        return

    json_files = list(APP_STATE_DIR.rglob("*.json"))
    if not json_files:
        print("No JSON files found in app_state.")
        return

    migrated = []
    for jf in json_files:
        try:
            ok = migrate_file(jf)
        except Exception as e:
            print(f"Error migrating {jf}: {e}")
            ok = False
        if ok:
            migrated.append(jf)

    print(f"Scanned {len(json_files)} JSON file(s). Migrated: {len(migrated)}")
    for p in migrated:
        print(f"  - {p}")


if __name__ == "__main__":
    main()
