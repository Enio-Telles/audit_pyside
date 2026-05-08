# GUI Testing Policy

This document defines the strategy for testing the PySide6 interface in the `audit-pyside` project, ensuring stability across different environments (local vs. CI).

## 1. Test Isolation

To avoid accidental imports of PySide6 in environments where Qt is not installed (e.g., standard CI jobs or headless servers), all tests that depend on PySide6 or `pytest-qt` must be located in:

```text
tests/ui/
```

This includes both full GUI integration tests and unit tests for modules that import PySide6 at the top level.

## 2. Standard Header for GUI Tests

Every test file in `tests/ui/` MUST follow this structure to ensure it can be collected and executed safely:

```python
import os
import sys
import pytest

# 1. Set headless platform before any PySide6 imports
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

# 2. Guard against missing PySide6 (prevents collection errors)
pytest.importorskip("PySide6")

# 3. Skip on Windows CI due to known DLL instability (0xc0000139)
if sys.platform == "win32" and os.getenv("GITHUB_ACTIONS"):
    pytest.skip("Skipping GUI tests on Windows CI due to DLL instability", allow_module_level=True)
```

## 3. Dependency Management

PySide6 is a core dependency, but `pytest-qt` is managed in a separate dependency group to keep standard test runners lean.

- **Standard sync:** `uv sync` (installs `dev` group, excludes `test-gui`).
- **GUI sync:** `uv sync --group test-gui` (required to run GUI tests).

## 4. CI Strategy

| Environment | Runner | Job | Action |
|---|---|---|---|
| **Ubuntu CI** | `ubuntu-latest` | `test-gui-smoke` | Runs all tests in `tests/ui/` with `QT_QPA_PLATFORM=offscreen`. |
| **Windows CI** | `windows-latest` | `test-windows` | **Skips** GUI tests to avoid fatal crashes. |
| **Windows Bundle**| `windows-latest`| `bundle-smoke` | Verifies the built `.exe` starts correctly (Manual/Main only). |

## 5. Local Execution

To run GUI tests locally:

1. Ensure dependencies are installed:
   ```bash
   uv sync --group test-gui
   ```
2. Run tests:
   ```bash
   # Headless (standard)
   uv run pytest tests/ui/

   # With visible windows (if you have a display)
   QT_QPA_PLATFORM=windows uv run pytest tests/ui/  # Windows
   QT_QPA_PLATFORM=xcb uv run pytest tests/ui/      # Linux
   ```
