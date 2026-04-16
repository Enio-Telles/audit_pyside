from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any

_WRITE_LOCK = Lock()


def _root_dir() -> Path:
    # Allow tests to monkeypatch _root_dir; default is project root
    return Path(__file__).resolve().parents[2]


def caminho_log_performance() -> Path:
    # Resolve the performance module that tests may patch under different import names
    import sys

    perf_mod = None
    for candidate in ("src.utilitarios.perf_monitor", "utilitarios.perf_monitor", __name__):
        perf_mod = sys.modules.get(candidate)
        if perf_mod is not None:
            break

    if perf_mod is not None and hasattr(perf_mod, "_root_dir"):
        root = perf_mod._root_dir()
    else:
        root = _root_dir()

    path = Path(root) / "logs" / "performance" / "perf_events.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _now_iso() -> str:
    """Return current ISO timestamp (seconds). Tests can monkeypatch module-level
    `datetime` to control this value (e.g. `mocker.patch("src.utilitarios.perf_monitor.datetime")`).
    """
    # Support tests that may patch the module under different import names.
    import sys

    for candidate in ("src.utilitarios.perf_monitor", "utilitarios.perf_monitor", __name__):
        mod = sys.modules.get(candidate)
        if mod is not None and hasattr(mod, "datetime"):
            dt = getattr(mod, "datetime")
            try:
                return dt.now().isoformat(timespec="seconds")
            except Exception:
                # Fall back to module-level datetime if the patched object behaves unexpectedly
                break

    return datetime.now().isoformat(timespec="seconds")


def _serializar_valor(valor: Any) -> Any:
    if isinstance(valor, Path):
        return str(valor)
    if isinstance(valor, dict):
        return {str(chave): _serializar_valor(conteudo) for chave, conteudo in valor.items()}
    if isinstance(valor, (list, tuple, set)):
        return [_serializar_valor(item) for item in valor]
    if isinstance(valor, (str, int, float, bool)) or valor is None:
        return valor
    return str(valor)


def registrar_evento_performance(
    evento: str,
    duracao_s: float | None = None,
    contexto: dict[str, Any] | None = None,
    status: str = "ok",
) -> None:
    # Use the `_now_iso()` wrapper so tests can monkeypatch `datetime` or `_now_iso` reliably.
    timestamp = _now_iso()

    registro: dict[str, Any] = {
        "timestamp": timestamp,
        "evento": str(evento),
        "status": str(status),
    }
    if duracao_s is not None:
        registro["duracao_s"] = round(float(duracao_s), 6)
    if contexto:
        registro["contexto"] = _serializar_valor(contexto)

    try:
        # Allow tests to patch caminho_log_performance under different module names
        import sys

        caminho_fn = None
        for candidate in ("src.utilitarios.perf_monitor", "utilitarios.perf_monitor", __name__):
            mod = sys.modules.get(candidate)
            if mod is not None and hasattr(mod, "caminho_log_performance"):
                caminho_fn = getattr(mod, "caminho_log_performance")
                break
        if caminho_fn is None:
            caminho_fn = caminho_log_performance

        destino = caminho_fn()
        with _WRITE_LOCK:
            with Path(destino).open("a", encoding="utf-8") as arquivo:
                arquivo.write(json.dumps(registro, ensure_ascii=False) + "\n")
    except Exception:
        # Instrumentacao de performance nunca deve interromper o fluxo principal.
        return
