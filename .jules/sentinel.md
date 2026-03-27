## YYYY-MM-DD - [Prevent Information Exposure Through Error Messages]
**Vulnerability:** The exception `e` or `exc` was directly cast to string and emitted to the UI or standard output (`rprint`) in `src/interface_grafica/services/query_worker.py` and `src/utilitarios/conectar_oracle.py`.
**Learning:** Emitting the raw `str(exc)` from `oracledb` could expose internal database schema or connection details (Information Exposure Through an Error Message - CWE-209).
**Prevention:** Avoid exposing `str(e)` directly to the user or standard output. Always use generic error messages for the UI/stdout and log the actual error internally for debugging.
