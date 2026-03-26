## 2024-05-18 - Prevent Information Leakage in UI Error Messages
**Vulnerability:** The application was exposing raw database exceptions (`str(exc)`) directly to the PySide UI in `QueryWorker.run()`, which could leak sensitive information such as database schema, query structures, or connection credentials to end users.
**Learning:** PySide asynchronous workers often propagate exceptions back to the main thread UI via signals (`self.failed.emit`). If these strings are not sanitized, they create Information Disclosure vulnerabilities.
**Prevention:** Always catch exceptions in worker threads, log the full traceback securely on the backend (using `rprint` or a logging framework), and emit only generic, safe error messages to the UI.
