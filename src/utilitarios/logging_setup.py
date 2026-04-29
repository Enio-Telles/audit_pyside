"""Simple structlog configuration helper used by GUI workers/controllers.

Provide `configure_structlog(level="INFO", json=False)` and `get_logger()`.
The CI can enable JSON output by setting `AUDIT_PYSIDE_LOG_JSON=1`.
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import structlog


def configure_structlog(level: str = "INFO", json: bool = False) -> None:
    log_level = getattr(logging, level.upper(), logging.INFO)

    processors = [
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.stdlib.add_log_level,
    ]

    if json or os.environ.get("AUDIT_PYSIDE_LOG_JSON") == "1":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    logging.basicConfig(format="%(message)s", level=log_level)
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: Optional[str] = None):
    return structlog.get_logger(name)
