"""Idempotent structlog configuration for audit_pyside GUI.

Call ``configure_structlog()`` once at application startup (in ``app.py``
before ``QApplication`` is created).  Subsequent calls are no-ops.

Set ``AUDIT_LOG_JSON=1`` to emit newline-delimited JSON (useful for
log-shipping pipelines).  Default output is the human-readable
ConsoleRenderer.
"""
from __future__ import annotations

import logging
import os

import structlog

_configured = False


def configure_structlog(level: str = "INFO") -> None:
    """Configure structlog once; subsequent calls are silently ignored."""
    global _configured
    if _configured:
        return

    log_level = getattr(logging, level.upper(), logging.INFO)
    use_json = os.environ.get("AUDIT_LOG_JSON", "").strip() == "1"

    shared_processors: list = [
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
    ]

    if use_json:
        shared_processors.append(structlog.processors.format_exc_info)
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    logging.basicConfig(format="%(message)s", level=log_level)
    _configured = True
