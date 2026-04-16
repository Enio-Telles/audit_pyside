fix(tests): make backend testable and fix failing tests

Summary
-------
This branch prepares the codebase for reliable test execution and fixes several failing unit
tests by improving testability and stabilizing behavior used in tests.

Key changes
-----------
- **`src/utilitarios/perf_monitor.py`**: make timestamp generation testable by adding a
  `_now_iso()` wrapper (robust to common monkeypatch import names) and make `caminho_log_performance`
  resolution resilient to tests that patch the module under different dotted names.
- **`src/transformacao/rastreabilidade_produtos/id_agrupados.py`**: separate complementary
  descriptions into `lista_desc_compl` (keeps `lista_descricoes` for main descriptions) and
  normalize/clean list columns consistently.
- **`src/interface_grafica/services/pipeline_funcoes_service.py`**: add `efd_atomizacao` to the
  UI catalog (`TABELAS_DISPONIVEIS`).
- **`src/orquestrador_pipeline.py`**: register `efd_atomizacao` in `REGISTO_TABELAS` so the
  pipeline order tests recognize it.
- **`src/transformacao/co_sefin_class.py`**: replace the old proxy with a `sys.modules` swap
  that points the proxy name to the real implementation module (so `monkeypatch` targets the
  expected module object).
- Remove temporary debug files: `tmp_debug_perf.py`, `tmp_debug_perf2.py`, `tmp_debug_co_sefin.py`.

Testing
-------
- Local test run: `PYTHONPATH=src python -m pytest -q` -> **166 passed, 214 warnings**.
- Ran relevant targeted tests during development (`tests/test_perf_monitor.py`,
  `tests/test_id_agrupados.py`, `tests/test_pipeline_efd_atomizacao_registro.py`).

Notes / follow-ups
------------------
- There are repo warnings worth addressing in follow-up PRs:
  - Several uses of `pl.Expr.map_elements` should be vectorized for performance.
  - Deprecated `str.concat` usage in `calculos_mensais`/`calculos_anuais` should be
    replaced by `str.join`.
  - Some tests emit `PytestReturnNotNoneWarning` (tests returning values instead of using
    `assert`) — minor test cleanups recommended.
- The branch contains focused testability fixes; please review the diff carefully, especially
  any large deletions (some files were changed/moved during prior work). The branch is
  `fix/tests-backend-2026-04-16` on the remote.

How to verify locally
---------------------
Run the full test suite (from repository root):

```powershell
$env:PYTHONPATH='src'; python -m pytest -q
```

Open PR (note)
--------------
I could not open the PR automatically because the GitHub CLI on this machine is not
authenticated (HTTP 401). You can open the PR in your browser using:

https://github.com/Enio-Telles/audit_pyside/pull/new/fix/tests-backend-2026-04-16

If you prefer I can try to create the PR via `gh` after you run `gh auth login` locally.

Suggested reviewers
-------------------
- Backend maintainers and owners of the ETL modules: `@<backend-team>` (please adjust).

Files of interest
-----------------
- `src/utilitarios/perf_monitor.py`
- `src/transformacao/rastreabilidade_produtos/id_agrupados.py`
- `src/interface_grafica/services/pipeline_funcoes_service.py`
- `src/orquestrador_pipeline.py`
- `src/transformacao/co_sefin_class.py`

Thanks — happy to iterate on follow-up cleanups (vectorization, deprecation fixes, CI notes).
