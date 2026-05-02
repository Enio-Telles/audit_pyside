"""
Configuração centralizada do MCP audit-pyside-perf.
"""

import os
from pathlib import Path


class Config:
    """Configuração do servidor MCP."""

    PROJECT_ROOT = Path(os.environ.get("AUDIT_PYSIDE_ROOT", ".")).resolve()

    # Limites de segurança
    MAX_QUERY_ROWS = 200
    MAX_FILE_SIZE_MB = 100
    SQL_TIMEOUT_SEC = 30

    # Oracle
    ORACLE_USER = os.environ.get("ORACLE_USER", "")
    ORACLE_PASSWORD = os.environ.get("ORACLE_PASSWORD", "")
    ORACLE_DSN = os.environ.get("ORACLE_DSN", "")

    # Paths permitidos
    ALLOWED_DIRS = {
        PROJECT_ROOT,
        PROJECT_ROOT / "src",
        PROJECT_ROOT / "tests",
        PROJECT_ROOT / "dados",
        PROJECT_ROOT / "output",
        PROJECT_ROOT / "bench",
    }

    # Paths bloqueados
    BLOCKED_DIRS = {".venv", "__pycache__", ".git", ".pytest_cache", "node_modules"}


def validate_config() -> bool:
    """Valida que a configuração está completa."""
    if not Config.PROJECT_ROOT.exists():
        raise RuntimeError(f"AUDIT_PYSIDE_ROOT não existe: {Config.PROJECT_ROOT}")

    if not Config.ORACLE_USER or not Config.ORACLE_PASSWORD or not Config.ORACLE_DSN:
        return False  # Oracle é opcional

    return True
