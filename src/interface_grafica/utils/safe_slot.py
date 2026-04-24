from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from typing import ParamSpec, TypeVar

import structlog

try:
    from PySide6.QtWidgets import QMessageBox
except Exception:  # pragma: no cover - test stubs may provide partial QtWidgets
    class QMessageBox:  # type: ignore[no-redef]
        @staticmethod
        def critical(parent, title: str, message: str) -> None:
            return None

P = ParamSpec("P")
R = TypeVar("R")

log = structlog.get_logger(__name__)


def _resolve_dialog_parent(args: tuple[object, ...]) -> object | None:
    if not args:
        return None

    candidate = args[0]
    is_widget_type = getattr(candidate, "isWidgetType", None)
    if callable(is_widget_type):
        try:
            if bool(is_widget_type()):
                return candidate
        except Exception:
            return None
    return None


def safe_slot(func: Callable[P, R]) -> Callable[P, R | None]:
    """Protege slots Qt contra excecoes inesperadas na thread da GUI."""

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R | None:
        try:
            return func(*args, **kwargs)
        except Exception as exc:  # pragma: no cover - GUI defensive path
            log.exception("gui.slot.failed", slot=func.__qualname__)
            QMessageBox.critical(
                _resolve_dialog_parent(args),
                "Erro inesperado",
                str(exc),
            )
            return None

    return wrapper
