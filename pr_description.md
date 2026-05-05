🧪 Add Unit Tests for QueryWorker

🎯 **What:** Implemented the missing unit tests for `src/interface_grafica/services/query_worker.py`. The `QueryWorker` manages asynchronous database queries using PySide6's `QThread`.

📊 **Coverage:** The new test file `tests/ui/test_query_worker.py` covers the following key scenarios:
- **Happy Path**: Verifies successful execution, DataFrame construction, and signal emission (`progress`, `finished_ok`).
- **Cancellation**: Ensures that queries interrupted by the user (`isInterruptionRequested` returning True) gracefully abort and emit the appropriate `failed` signal.
- **Error Path (Sentinel Check)**: Validates that exceptions raised during query execution are caught and a generic, sanitized error message is emitted to the UI via the `failed` signal, ensuring internal database details are not leaked.
- **Fallback Path**: Confirms that when the main connection import fails, the worker seamlessly delegates to the fallback mechanism `_conectar_oracle_fallback`.

✨ **Result:** Test coverage for `query_worker.py` is established. The testing approach synchronously invokes `.run()` while mocking threading dependencies to keep tests fast, robust, and deterministic. Also addressed an environment issue causing PySide6 tests to abort in CI by dynamically installing `pytest-qt` and forcing headless mode (`QT_QPA_PLATFORM=offscreen`).
