from __future__ import annotations

from structlog.testing import capture_logs

from interface_grafica.utils import safe_slot as safe_slot_module
from interface_grafica.utils.safe_slot import safe_slot


def test_safe_slot_with_structlog_capture(monkeypatch) -> None:
    calls: list[tuple] = []
    monkeypatch.setattr(
        safe_slot_module.QMessageBox,
        "critical",
        lambda parent, title, msg: calls.append((parent, title, msg)),
    )

    @safe_slot
    def slot_raises() -> None:
        raise ValueError("structlog test")

    with capture_logs() as entries:
        slot_raises()

    assert len(entries) == 1
    assert len(calls) == 1
    assert entries[0]["event"] == "gui.slot.failed"
    assert entries[0]["log_level"] == "error"
    assert entries[0]["exc_info"] is True
    assert "slot_raises" in entries[0]["slot"]
    assert calls[0][1] == "Erro inesperado"
    assert calls[0][2] == "structlog test"

