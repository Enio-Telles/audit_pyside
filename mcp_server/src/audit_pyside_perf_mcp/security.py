"""
Guardrails de segurança para o MCP.
"""

import re
from pathlib import Path
from .config import Config


class SecurityError(Exception):
    """Exceção de segurança."""
    pass


class SqlSecurityError(SecurityError):
    """SQL bloqueado."""
    pass


class PathSecurityError(SecurityError):
    """Caminho bloqueado."""
    pass


# SQL bloqueado
BLOCKED_SQL_WORDS = {
    "insert", "update", "delete", "drop", "alter", "truncate",
    "merge", "grant", "revoke", "create", "replace", "execute",
    "call", "begin", "declare", "pragma"
}


def is_safe_sql(sql: str) -> bool:
    """Valida que SQL é apenas SELECT/WITH sem DDL/DML."""
    cleaned = sql.strip().lower()

    # Não permite múltiplos statements
    if ";" in cleaned:
        return False

    # Deve começar com SELECT ou WITH
    if not (cleaned.startswith("select") or cleaned.startswith("with")):
        return False

    # Verifica bloqueio de palavras-chave
    tokens = set(re.findall(r"[a-z_]+", cleaned))
    return not bool(tokens & BLOCKED_SQL_WORDS)


def is_safe_path(path: str | Path) -> bool:
    """Valida que o caminho está dentro de ALLOWED_DIRS e não é bloqueado."""
    try:
        full_path = (Config.PROJECT_ROOT / path).resolve()
    except Exception:
        return False

    # Verifica que está dentro de um diretório permitido
    try:
        full_path.relative_to(Config.PROJECT_ROOT)
    except ValueError:
        return False

    # Verifica que não tem partes bloqueadas
    for part in full_path.parts:
        if part in Config.BLOCKED_DIRS:
            return False

    return True


def guard_sql(sql: str) -> str:
    """Valida SQL e retorna erro claro se inválido."""
    if not is_safe_sql(sql):
        raise SqlSecurityError(
            "SQL bloqueado. Apenas SELECT/WITH sem ponto-e-vírgula é permitido. "
            "Não é permitido: INSERT, UPDATE, DELETE, DROP, ALTER, etc."
        )
    return sql


def guard_path(path: str | Path) -> Path:
    """Valida caminho e retorna Path seguro."""
    if not is_safe_path(path):
        raise PathSecurityError(
            f"Caminho bloqueado ou fora do projeto: {path}. "
            f"Use caminho relativo dentro de {Config.PROJECT_ROOT}"
        )
    return (Config.PROJECT_ROOT / path).resolve()
