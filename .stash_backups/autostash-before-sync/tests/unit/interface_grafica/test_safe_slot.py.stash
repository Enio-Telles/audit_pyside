from __future__ import annotations

from structlog.testing import capture_logs

from interface_grafica.utils import safe_slot as safe_slot_module
from interface_grafica.utils.safe_slot import safe_slot


def test_safe_slot_logs_and_swallows_exception(monkeypatch) -> None:
    calls: list[tuple[object | None, str, str]] = []

    def fake_critical(parent, title: str, message: str) -> None:
        calls.append((parent, title, message))

    monkeypatch.setattr(safe_slot_module.QMessageBox, "critical", fake_critical)

    @safe_slot
    def exploding_slot() -> None:
        raise ValueError("falha controlada")

    with capture_logs() as entries:
        result = exploding_slot()

    assert result is None
    assert len(entries) == 1
    assert entries[0]["event"] == "gui.slot.failed"
    assert entries[0]["slot"].endswith("exploding_slot")
    assert entries[0]["log_level"] == "error"
    assert entries[0]["exc_info"] is True
    assert calls == [(None, "Erro inesperado", "falha controlada")]


def test_safe_slot_uses_widget_like_parent(monkeypatch) -> None:
    calls: list[tuple[object | None, str, str]] = []

    def fake_critical(parent, title: str, message: str) -> None:
        calls.append((parent, title, message))

    monkeypatch.setattr(safe_slot_module.QMessageBox, "critical", fake_critical)

    class DummyWidget:
        def isWidgetType(self) -> bool:
            return True

        @safe_slot
        def exploding_slot(self) -> None:
            raise RuntimeError("falha com parent")

    widget = DummyWidget()

    result = widget.exploding_slot()

    assert result is None
    assert calls == [(widget, "Erro inesperado", "falha com parent")]
