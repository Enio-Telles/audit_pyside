## YYYY-MM-DD - [Prevent Information Exposure Through Error Messages]
**Vulnerability:** The exception `e` or `exc` was directly cast to string and emitted to the UI or standard output (`rprint`) in `src/interface_grafica/services/query_worker.py` and `src/utilitarios/conectar_oracle.py`.
**Learning:** Emitting the raw `str(exc)` from `oracledb` could expose internal database schema or connection details (Information Exposure Through an Error Message - CWE-209).
**Prevention:** Avoid exposing `str(e)` directly to the user or standard output. Always use generic error messages for the UI/stdout and log the actual error internally for debugging.

## 2024-04-01 - Prevent Information Leakage in UI Error Handlers
**Vulnerability:** A `try...except` block in the PySide6 UI layer was directly printing a raw stack trace via `traceback.print_exc()` to standard output and passing the raw exception object (`str(e)`) to the user-facing `QMessageBox` via `self.show_error()`. This risked disclosing sensitive internal application state, file paths, and potential data formats to unprivileged users.
**Learning:** PySide6 components must not leak raw exceptions to the UI. Conversely, observability must not be destroyed by completely removing error logging. The correct approach is to log the detailed error and traceback securely on the backend (using `utilitarios.perf_monitor.registrar_evento_performance`) and present only generic, sanitized error messages to the UI.
**Prevention:** Always audit `except Exception as e:` blocks in user-facing code to ensure `str(e)` or `traceback.format_exc()` are strictly routed to internal telemetry systems and never rendered directly in the graphical interface.
## 2025-02-14 - Fix Path Disclosure in Traceback Logging
**Vulnerability:** Raw exception tracebacks were being written using `traceback.print_exc()` to a hardcoded local file path (`c:\funcoes - Copia\traceback.txt`) in `calculos_mensais.py`, `calculos_anuais.py`, and `movimentacao_estoque.py`, leading to Information Disclosure (CWE-209).
**Learning:** Exception handling `__main__` blocks in legacy extraction scripts bypassed the centralized logging utility, causing unsafe hardcoded paths and leaked application internals.
**Prevention:** Always use the centralized application logger (e.g., `log_exception(e)` from `transformacao.auxiliares.logs`) rather than ad-hoc local files. Avoid hardcoded developer paths in `except` blocks.
## 2024-03-05 - Fix Information Disclosure in Orchestrator
**Vulnerability:** `src/orquestrador_pipeline.py` leaked raw exception details and full stack traces directly to `stdout` (`rprint`), potentially exposing sensitive internal application structure, paths, and database execution states during exceptions.
**Learning:** Even CLI utilities or top-level orchestrators must fail securely. Exposing `{e}` or `traceback.format_exc()` on standard output provides attackers with critical footprinting information.
**Prevention:** Catch blocks should output generic, sanitized error messages to the user while logging the full exception internally using a secure centralized logging utility, such as `transformacao.auxiliares.logs.log_exception`.
