"""Idempotent structlog configuration for audit_pyside GUI.

Call ``configure_structlog()`` once at application startup (in ``app.py``
before ``QApplication`` is created).  Subsequent calls are no-ops.

Call ``install_fallback_hooks()`` after ``configure_structlog()`` to route
unhandled Python exceptions and Qt runtime messages to structlog.

Set ``AUDIT_LOG_JSON=1`` to emit newline-delimited JSON (useful for
log-shipping pipelines).  Default output is the human-readable
ConsoleRenderer.
"""
from __future__ import annotations

import logging
import os
import sys
import types

import structlog

_configured = False
_hooks_installed = False

_logger = structlog.get_logger(__name__)


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


def install_fallback_hooks() -> None:
    """Register fallback handlers that route uncaught signals to structlog.

    Installs:
    - ``sys.excepthook``: logs unhandled Python exceptions before the
      process exits, so they appear in structlog output rather than only
      on stderr.
    - Qt message handler (if PySide6 is importable): routes qWarning /
      qCritical / qFatal messages to structlog at the appropriate level.

    Idempotent — subsequent calls are no-ops.
    """
    global _hooks_installed
    if _hooks_installed:
        return

    _original_excepthook = sys.excepthook

    def _structlog_excepthook(
        exc_type: type[BaseException],
        exc_value: BaseException,
        exc_tb: types.TracebackType | None,
    ) -> None:
        if issubclass(exc_type, KeyboardInterrupt):
            _original_excepthook(exc_type, exc_value, exc_tb)
            return
        try:
            _logger.critical(
                "gui.unhandled_exception",
                exc_type=exc_type.__name__,
                exc_info=(exc_type, exc_value, exc_tb),
            )
        except Exception:
            pass
        finally:
            _original_excepthook(exc_type, exc_value, exc_tb)

    sys.excepthook = _structlog_excepthook

    _try_install_qt_message_handler()

    _hooks_installed = True


def _try_install_qt_message_handler() -> None:
    """Install a Qt message handler that forwards to structlog.

    Silently skipped when PySide6 is not importable (e.g. in unit tests
    that run without a Qt environment).
    """
    try:
        from PySide6.QtCore import QtMsgType, qInstallMessageHandler
    except ImportError:
        return

    _qt_level_map = {
        QtMsgType.QtDebugMsg: "debug",
        QtMsgType.QtInfoMsg: "info",
        QtMsgType.QtWarningMsg: "warning",
        QtMsgType.QtCriticalMsg: "error",
        QtMsgType.QtFatalMsg: "critical",
    }

    def _qt_message_handler(msg_type: QtMsgType, context: object, message: str) -> None:
        level = _qt_level_map.get(msg_type, "warning")
        getattr(_logger, level)(
            "qt.message",
            message=message,
            category=getattr(context, "category", None),
            file=getattr(context, "file", None),
            line=getattr(context, "line", None),
            function=getattr(context, "function", None),
        )

    qInstallMessageHandler(_qt_message_handler)
