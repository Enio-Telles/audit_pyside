## 2024-05-24 - Fix Information Disclosure in Oracle Connection
**Vulnerability:** Information Exposure (CWE-209) through `rprint` outputting detailed exception string (`{e}`) in `conectar_oracle.py`.
**Learning:** Detailed runtime exceptions regarding the environment setup were logged directly to the end-user CLI when they failed, potentially exposing host environment data.
**Prevention:** Catch exception context using the standard logging module for developers and display generic, sanitized errors via user-facing functions like `rprint` to avoid Information Disclosure.

## 2024-05-18 - Prevent Information Leakage in UI Error Messages
**Vulnerability:** The application was exposing raw database exceptions (`str(exc)`) directly to the PySide UI in `QueryWorker.run()`, which could leak sensitive information such as database schema, query structures, or connection credentials to end users.
**Learning:** PySide asynchronous workers often propagate exceptions back to the main thread UI via signals (`self.failed.emit`). If these strings are not sanitized, they create Information Disclosure vulnerabilities.
**Prevention:** Always catch exceptions in worker threads, log the full traceback securely on the backend (using `rprint` or a logging framework), and emit only generic, safe error messages to the UI.

## 2025-02-13 - Removal of Hardcoded Internal Infrastructure Details
**Vulnerability:** Internal Oracle database hostnames, ports, and service names were hardcoded as defaults in the codebase, potentially exposing internal network architecture and making the application less flexible and secure.
**Learning:** Hardcoding infrastructure details, even as defaults for environment variables, can lead to information disclosure if the source code is accessed.
**Prevention:** Always enforce the use of environment variables or external configuration files for infrastructure details, and implement strict validation to ensure the application fails fast if they are missing.
