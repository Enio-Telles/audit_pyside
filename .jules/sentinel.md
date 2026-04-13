## 2024-05-24 - Fix Information Disclosure in Oracle Connection
**Vulnerability:** Information Exposure (CWE-209) through `rprint` outputting detailed exception string (`{e}`) in `conectar_oracle.py`.
**Learning:** Detailed runtime exceptions regarding the environment setup were logged directly to the end-user CLI when they failed, potentially exposing host environment data.
**Prevention:** Catch exception context using the standard logging module for developers and display generic, sanitized errors via user-facing functions like `rprint` to avoid Information Disclosure.
