import logging
from pathlib import Path

LARGE_PARQUET_THRESHOLD_MB = 512
_log = logging.getLogger(__name__)


def log_parquet_open(path: str | Path, progresso=None) -> None:
    """Registra aviso quando um Parquet grande e aberto.

    Args:
        path: caminho do arquivo Parquet.
        progresso: callback opcional que recebe uma string de mensagem
                   (ex.: funcao de progresso do worker/service).
    """
    try:
        mb = Path(path).stat().st_size / 1_048_576
    except OSError:
        return
    if mb >= LARGE_PARQUET_THRESHOLD_MB:
        msg = f"[LARGE-PARQUET] {Path(path).name} {mb:.1f} MB"
        _log.warning(msg)
        if progresso is not None:
            progresso(msg)
