from __future__ import annotations

import pytest
from structlog.testing import capture_logs

from interface_grafica.utils.retry import retry_on_io


def test_retry_on_io_logs_warnings_and_success_after_retry() -> None:
    calls = {"count": 0}

    @retry_on_io(max_attempts=3, backoff_seconds=(0, 0, 0), exceptions=(OSError,))
    def flaky() -> str:
        calls["count"] += 1
        if calls["count"] < 3:
            raise OSError("temporario")
        return "ok"

    with capture_logs() as entries:
        assert flaky() == "ok"

    assert calls["count"] == 3
    assert [entry["event"] for entry in entries] == [
        "io.retry.attempt",
        "io.retry.attempt",
        "io.retry.success",
    ]
    assert [entry["log_level"] for entry in entries] == ["warning", "warning", "info"]
    assert entries[-1]["attempts_used"] == 3


def test_retry_on_io_logs_exhaustion_and_reraises_last_exception() -> None:
    @retry_on_io(max_attempts=3, backoff_seconds=(0, 0, 0), exceptions=(OSError,))
    def always_fails() -> None:
        raise OSError("falha persistente")

    with capture_logs() as entries, pytest.raises(OSError, match="falha persistente"):
        always_fails()

    assert [entry["event"] for entry in entries] == [
        "io.retry.attempt",
        "io.retry.attempt",
        "io.retry.attempt",
        "io.retry.exhausted",
    ]
    assert [entry["log_level"] for entry in entries] == [
        "warning",
        "warning",
        "warning",
        "error",
    ]
    assert entries[-1]["attempts"] == 3
