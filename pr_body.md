Fix startup migration and remove stale frontend references

Summary:
- Adds an idempotent, non-fatal migration for legacy `app_state` entries at UI startup by invoking `scripts/migrate_app_state_active_load_workers.py` from `src/interface_grafica/config.py`.
- Clarifies that this repository's UI is a PySide6 desktop application (`src/interface_grafica/`) — there is no React frontend in this repo.

Changes:
- `src/interface_grafica/config.py`
  - Invoke `scripts/migrate_app_state_active_load_workers.py` on startup; the migration is idempotent, logs errors, and is non-fatal.
- `scripts/migrate_app_state_active_load_workers.py`
  - Migration helper (no behavioral changes intended).
- Minor: `tmp_import_*` test files touched for import hygiene.

Rationale:
- Make UI startup tolerant of legacy persisted `app_state` formats without crashing and remove stale references to a non-existent React frontend.

How tested:
- Python import test:
  - `python -c "import sys; sys.path.insert(0,'src'); import importlib; importlib.import_module('interface_grafica.config')"`
  - Confirm migration runs without raising exceptions and logs non-fatal issues.

Notes:
- No behavioral changes expected aside from improved startup tolerance for legacy `app_state` entries.
- Please request review from the PySide6 UI maintainers (package `src/interface_grafica`).

Follow-up changes:
- `src/transformacao/calculos_mensais_pkg/calculos_mensais.py` and `src/transformacao/calculos_anuais_pkg/calculos_anuais.py`
  - Replace deprecated `str.concat(";")` with `str.join(";")` to remove deprecation warnings from Polars.
