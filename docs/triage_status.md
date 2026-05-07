# Triage Status Report - 2026-05-07

## PR Classification

| PR # | Category | Status | Action / Reason |
| :--- | :--- | :--- | :--- |
| **#189** | CI/GUI Windows | **FIXED** | Skip logic verified in `tests/ui/test_main_window_smoke.py`. |
| **#227** | Security | **FIXED** | Command injection via `explorer` replaced with `os.startfile`. |
| **#230** | Security | **FIXED** | Oracle identifier validation implemented in `SqlService`. |
| **#238** | Performance | **FIXED** | `None` values regression in HTML table fixed. |
| **#223** | Fiscal | **BLOCKED** | Touches read-only fiscal files; requires ADR and Diff Harness. |
| **#240** | Fiscal | **BLOCKED** | Requires full fiscal gate validation. |
| **#225** | Documentation | **MERGE** | Hygiene and docstrings batch. |
| **#242** | Documentation | **MERGE** | Hygiene and docstrings batch. |
| **#244** | Documentation | **MERGE** | Hygiene and docstrings batch. |
| **#245** | Documentation | **MERGE** | Hygiene and docstrings batch. |
| **#246** | Documentation | **MERGE** | Hygiene and docstrings batch. |
| **#222** | CI/GUI | **DECIDE** | Decision needed between #222 and #243 regarding pytest-qt location. |
| **#243** | CI/GUI | **DECIDE** | Decision needed between #222 and #243. |
| **#235** | Performance | **REVIEW** | Risk of schema divergence and memory spikes. |
| **#247** | Performance | **REVIEW** | Needs benchmark validation. |
| **#248** | Performance | **REVIEW** | Needs human review of benchmark methodology. |
| **#233** | Quality | **REVIEW** | Duplicate test names identified. |
| **#241** | Feature | **DRAFT** | Auto-update requires signing, hash, and release policy. |

## Immediate Blockers Status

- **#223:** Touches `_produtos_final_impl.py`; gate complete required.
- **#230:** Regex and constant implementation DONE. Tests decoupled DONE.
- **#227:** Fix separated from artifacts. Branch cleanup of artifacts performed.
- **#222:** On hold (waiting for GUI smoke Windows decision).
- **#235:** Needs verification of schema consistency across chunks.
- **#238:** Regression with `None` generating empty lines DONE.
- **#233:** Requires fixing duplicate test names.
- **#241:** Awaiting release policy definition.
- **#248:** Awaiting benchmark methodology review.

## Branch Cleanup Preparedness

- Merged/Closed PR branches can be safely deleted using `scripts/cleanup-merged-branches.ps1` after this triage PR is merged.
- **DO NOT** delete branches for BLOCKED or REVIEW PRs.
