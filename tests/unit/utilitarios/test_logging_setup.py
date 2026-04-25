from __future__ import annotations

import structlog

from utilitarios.logging_setup import configure_structlog, get_logger


def test_configure_structlog_default_runs() -> None:
    configure_structlog()


def test_configure_structlog_json_mode() -> None:
    configure_structlog(json=True)
    configure_structlog()  # reset to console for subsequent tests


def test_configure_structlog_env_var_activates_json(monkeypatch) -> None:
    monkeypatch.setenv("AUDIT_PYSIDE_LOG_JSON", "1")
    configure_structlog()
    monkeypatch.delenv("AUDIT_PYSIDE_LOG_JSON")
    configure_structlog()  # reset


def test_configure_structlog_custom_level() -> None:
    configure_structlog(level="DEBUG")
    configure_structlog()  # reset to INFO


def test_get_logger_returns_logger() -> None:
    logger = get_logger("test_module")
    assert logger is not None


def test_get_logger_no_name() -> None:
    logger = get_logger()
    assert logger is not None
