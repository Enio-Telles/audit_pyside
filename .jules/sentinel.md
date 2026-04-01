## YYYY-MM-DD - [Prevent Information Exposure Through Error Messages]
**Vulnerability:** The exception `e` or `exc` was directly cast to string and emitted to the UI or standard output (`rprint`) in `src/interface_grafica/services/query_worker.py` and `src/utilitarios/conectar_oracle.py`.
**Learning:** Emitting the raw `str(exc)` from `oracledb` could expose internal database schema or connection details (Information Exposure Through an Error Message - CWE-209).
**Prevention:** Avoid exposing `str(e)` directly to the user or standard output. Always use generic error messages for the UI/stdout and log the actual error internally for debugging.

## 2024-04-01 - Prevent Information Leakage in UI Error Handlers
**Vulnerability:** A `try...except` block in the PySide6 UI layer was directly printing a raw stack trace via `traceback.print_exc()` to standard output and passing the raw exception object (`str(e)`) to the user-facing `QMessageBox` via `self.show_error()`. This risked disclosing sensitive internal application state, file paths, and potential data formats to unprivileged users.
**Learning:** PySide6 components must not leak raw exceptions to the UI. Conversely, observability must not be destroyed by completely removing error logging. The correct approach is to log the detailed error and traceback securely on the backend (using `utilitarios.perf_monitor.registrar_evento_performance`) and present only generic, sanitized error messages to the UI.
**Prevention:** Always audit `except Exception as e:` blocks in user-facing code to ensure `str(e)` or `traceback.format_exc()` are strictly routed to internal telemetry systems and never rendered directly in the graphical interface.
