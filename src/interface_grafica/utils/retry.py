from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from time import sleep
from typing import ParamSpec, TypeVar

import oracledb
import polars as pl
import structlog

P = ParamSpec("P")
R = TypeVar("R")

log = structlog.get_logger(__name__)
_POLARS_IO_ERROR = getattr(pl.exceptions, "PolarsPanicError", pl.exceptions.PolarsError)
DEFAULT_IO_EXCEPTIONS = (OSError, oracledb.Error, _POLARS_IO_ERROR)


def retry_on_io(
    max_attempts: int = 3,
    backoff_seconds: tuple[float, ...] = (1, 2, 4),
    exceptions: tuple[type[BaseException], ...] = DEFAULT_IO_EXCEPTIONS,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Retry pequeno para leituras GUI de Oracle/Parquet com logging estruturado."""
    if max_attempts < 1:
        raise ValueError("max_attempts deve ser maior ou igual a 1.")

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            last_exc: BaseException | None = None
            for attempt in range(1, max_attempts + 1):
                try:
                    result = func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    log.warning(
                        "io.retry.attempt",
                        attempt=attempt,
                        max=max_attempts,
                        exception_type=type(exc).__name__,
                    )
                    if attempt >= max_attempts:
                        log.error("io.retry.exhausted", attempts=max_attempts)
                        raise
                    delay = backoff_seconds[min(attempt - 1, len(backoff_seconds) - 1)]
                    if delay > 0:
                        sleep(delay)
                    continue
                if attempt > 1:
                    log.info("io.retry.success", attempts_used=attempt)
                return result
            raise RuntimeError("Retry finalizado sem resultado.") from last_exc

        return wrapper

    return decorator
