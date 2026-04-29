## 2024-05-18 - Prevent Information Leakage in UI Error Messages
**Vulnerability:** The application was exposing raw database exceptions (`str(exc)`) directly to the PySide UI in `QueryWorker.run()`, which could leak sensitive information such as database schema, query structures, or connection credentials to end users.
**Learning:** PySide asynchronous workers often propagate exceptions back to the main thread UI via signals (`self.failed.emit`). If these strings are not sanitized, they create Information Disclosure vulnerabilities.
**Prevention:** Always catch exceptions in worker threads, log the full traceback securely on the backend (using `rprint` or a logging framework), and emit only generic, safe error messages to the UI.

## 2024-05-24 - Fix Information Disclosure in Oracle Connection
**Vulnerability:** Information Exposure (CWE-209) through `rprint` outputting detailed exception string (`{e}`) in `conectar_oracle.py`.
**Learning:** Detailed runtime exceptions regarding the environment setup were logged directly to the end-user CLI when they failed, potentially exposing host environment data.
**Prevention:** Catch exception context using the standard logging module for developers and display generic, sanitized errors via user-facing functions like `rprint` to avoid Information Disclosure.

## 2025-02-13 - Removal of Hardcoded Internal Infrastructure Details
**Vulnerability:** Internal Oracle database hostnames, ports, and service names were hardcoded as defaults in the codebase, potentially exposing internal network architecture and making the application less flexible and secure.
**Learning:** Hardcoding infrastructure details, even as defaults for environment variables, can lead to information disclosure if the source code is accessed.
**Prevention:** Always enforce the use of environment variables or external configuration files for infrastructure details, and implement strict validation to ensure the application fails fast if they are missing.

## 2024-05-18 - Prevented exposure of internal Oracle config parameters
**Vulnerability:** Internal Oracle database hostnames (`exa01-scan.sefin.ro.gov.br`), ports (`1521`), and service names (`sefindw`) were hardcoded as fallback default values in several database connection modules and UI elements.
**Learning:** Hardcoding connection details inside the application can expose sensitive internal network architecture data to unintended audiences (Information Disclosure). Applications should securely retrieve environmental configuration settings at runtime without exposing fallback internal domains.
**Prevention:** Always enforce strict reliance on `.env` configuration files for database settings. Do not provide default fallback values that represent internal infrastructure. Secure applications will raise robust initialization errors if required configurations are missing.

## 2026-04-20 - Prevent SQL Injection by Restricting Arbitrary SQL Execution
**Vulnerability:** The `/execute` endpoint in `backend/routers/sql_query.py` was previously receiving arbitrary `sql` query strings from the client via the `SqlRequest` model and executing them directly against the database via `SqlService.executar_sql()`, introducing a severe SQL Injection risk.
**Learning:** Exposing raw SQL execution to an API endpoint is highly unsafe. By restricting execution to predefined queries registered in the application's catalog, you can effectively mitigate SQL injection.
**Prevention:** Always accept an `sql_id` or similar identifier in requests to execute SQL, and look up the registered query content (e.g. via `SqlService.read_sql()`) before execution. Do not pass client-provided arbitrary query strings directly to database cursors.
<<<<<<< HEAD
=======

## 2024-05-18 - Prevented exposure of internal Oracle config parameters
**Vulnerability:** Internal Oracle database hostnames (`exa01-scan.sefin.ro.gov.br`), ports (`1521`), and service names (`sefindw`) were hardcoded as fallback default values in several database connection modules and UI elements.
**Learning:** Hardcoding connection details inside the application can expose sensitive internal network architecture data to unintended audiences (Information Disclosure). Applications should securely retrieve environmental configuration settings at runtime without exposing fallback internal domains.
**Prevention:** Always enforce strict reliance on `.env` configuration files for database settings. Do not provide default fallback values that represent internal infrastructure. Secure applications will raise robust initialization errors if required configurations are missing.
## 2026-04-20 - Prevent SQL Injection by Restricting Arbitrary SQL Execution
**Vulnerability:** The `/execute` endpoint in `backend/routers/sql_query.py` was previously receiving arbitrary `sql` query strings from the client via the `SqlRequest` model and executing them directly against the database via `SqlService.executar_sql()`, introducing a severe SQL Injection risk.
**Learning:** Exposing raw SQL execution to an API endpoint is highly unsafe. By restricting execution to predefined queries registered in the application's catalog, you can effectively mitigate SQL injection.
**Prevention:** Always accept an `sql_id` or similar identifier in requests to execute SQL, and look up the registered query content (e.g. via `SqlService.read_sql()`) before execution. Do not pass client-provided arbitrary query strings directly to database cursors.
>>>>>>> upstream/main
