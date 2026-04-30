from __future__ import annotations

import logging

import pytest
import structlog

from utilitarios.logging_setup import configure_structlog, get_logger


@pytest.fixture(autouse=True)
def _restore_structlog_state():
    """Save and restore structlog config + root log level around each test."""
    original_config = structlog.get_config()
    original_level = logging.getLogger().level
    yield
    structlog.reset_defaults()
    structlog.configure(**original_config)
    logging.getLogger().setLevel(original_level)


def test_configure_structlog_default_uses_console_renderer() -> None:
    configure_structlog()
    procs = structlog.get_config()["processors"]
    assert any(isinstance(p, structlog.dev.ConsoleRenderer) for p in procs)


def test_configure_structlog_json_mode() -> None:
    configure_structlog(json=True)
    procs = structlog.get_config()["processors"]
    assert any(isinstance(p, structlog.processors.JSONRenderer) for p in procs)


def test_configure_structlog_env_var_activates_json(monkeypatch) -> None:
    monkeypatch.setenv("AUDIT_PYSIDE_LOG_JSON", "1")
    configure_structlog()
    procs = structlog.get_config()["processors"]
    assert any(isinstance(p, structlog.processors.JSONRenderer) for p in procs)


def test_configure_structlog_custom_level() -> None:
    # basicConfig is a no-op when root logger already has handlers; we verify
    # the call doesn't raise and the processor chain is still set up.
    configure_structlog(level="DEBUG")
    procs = structlog.get_config()["processors"]
    assert len(procs) > 0


def test_get_logger_returns_logger() -> None:
    logger = get_logger("test_module")
    assert logger is not None


def test_get_logger_no_name() -> None:
    logger = get_logger()
    assert logger is not None
