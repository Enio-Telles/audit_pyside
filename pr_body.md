Fix frontend DataTable lint and startup migration

This PR fixes parsing and ESLint issues in frontend/src/components/table/DataTable.tsx, adjusts useEffect dependencies, and integrates an idempotent app_state migration at UI startup.

Changes:
- frontend/src/components/table/DataTable.tsx
  - Fix JSX fragment parsing error introduced by virtualization refactor.
  - Replace fragment return with explicit array of nodes to avoid mismatched fragments.
  - Add explicit React import and suppress react-hooks/incompatible-library false-positive for useReactTable.
  - Fix useEffect dependency array to depend on ref objects.
- src/interface_grafica/config.py
  - Invoke scripts/migrate_app_state_active_load_workers.py at startup (idempotent, logs errors, non-fatal).
- scripts/migrate_app_state_active_load_workers.py
  - Migration helper (already present).
- Other minor files (tmp_import_* test files).

Why:
- Resolve linting error failing local pnpm lint and make startup tolerant of legacy persisted app_state entries.

How tested:
- python -c "import sys; sys.path.insert(0,'src'); import importlib; importlib.import_module('interface_grafica.config')" -> migration executed safely.
- cd frontend; pnpm install; pnpm exec tsc --noEmit; pnpm lint -> no lint errors (warnings only).

Notes:
- No functional changes expected aside from migration behavior.
- Request review from frontend maintainers.
