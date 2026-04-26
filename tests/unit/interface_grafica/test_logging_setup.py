from __future__ import annotations

import sys
import types

import pytest

import interface_grafica.logging_setup as _mod
from interface_grafica.logging_setup import configure_structlog, install_fallback_hooks


@pytest.fixture(autouse=True)
def _reset_module_state():
    """Restore module-level flags and sys.excepthook after each test."""
    original_excepthook = sys.excepthook
    original_configured = _mod._configured
    original_hooks_installed = _mod._hooks_installed
    yield
    sys.excepthook = original_excepthook
    _mod._configured = original_configured
    _mod._hooks_installed = original_hooks_installed


def test_configure_structlog_is_idempotent():
    _mod._configured = False
    configure_structlog()
    assert _mod._configured is True
    # Second call must not raise and must remain True
    configure_structlog()
    assert _mod._configured is True


def test_install_fallback_hooks_replaces_excepthook():
    _mod._hooks_installed = False
    original = sys.excepthook
    install_fallback_hooks()
    assert sys.excepthook is not original
    assert _mod._hooks_installed is True


def test_install_fallback_hooks_is_idempotent():
    _mod._hooks_installed = False
    install_fallback_hooks()
    hook_after_first = sys.excepthook
    install_fallback_hooks()
    assert sys.excepthook is hook_after_first


def test_excepthook_logs_and_delegates(monkeypatch):
    _mod._hooks_installed = False

    delegate_calls: list[tuple] = []

    def fake_original(exc_type, exc_value, exc_tb):
        delegate_calls.append((exc_type, exc_value, exc_tb))

    monkeypatch.setattr(sys, "excepthook", fake_original)
    install_fallback_hooks()

    logged: list[dict] = []

    def fake_critical(event, **kw):
        logged.append({"event": event, **kw})

    monkeypatch.setattr(_mod._logger, "critical", fake_critical)

    exc = ValueError("boom")
    tb: types.TracebackType | None = None
    try:
        raise exc
    except ValueError:
        import sys as _sys
        _, _, tb = _sys.exc_info()

    sys.excepthook(ValueError, exc, tb)

    assert len(logged) == 1
    assert logged[0]["event"] == "gui.unhandled_exception"
    assert logged[0]["exc_type"] == "ValueError"
    assert logged[0]["exc_info"] == (ValueError, exc, tb)
    assert delegate_calls == [(ValueError, exc, tb)]


def test_excepthook_keyboard_interrupt_delegates_without_logging(monkeypatch):
    _mod._hooks_installed = False

    delegate_calls: list[tuple] = []

    def fake_original(exc_type, exc_value, exc_tb):
        delegate_calls.append((exc_type,))

    monkeypatch.setattr(sys, "excepthook", fake_original)
    install_fallback_hooks()

    logged: list[dict] = []
    monkeypatch.setattr(_mod._logger, "critical", lambda *a, **kw: logged.append(kw))

    sys.excepthook(KeyboardInterrupt, KeyboardInterrupt(), None)

    assert logged == []
    assert delegate_calls == [(KeyboardInterrupt,)]


def test_try_install_qt_message_handler_skipped_when_no_pyside6(monkeypatch):
    import builtins
    real_import = builtins.__import__

    def _block_pyside6(name, *args, **kwargs):
        if name.startswith("PySide6"):
            raise ImportError("PySide6 not available")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _block_pyside6)
    # Should not raise
    _mod._try_install_qt_message_handler()
