from __future__ import annotations

import os
import re
from datetime import date, datetime
from pathlib import Path

from utilitarios.validar_cnpj import validar_cnpj as _validar_cnpj

_DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y")


def validate_cnpj(cnpj: str) -> str:
    """Aceita CNPJ com ou sem mascara; retorna string de 14 digitos ou levanta ValueError."""
    digits = re.sub(r"[^0-9]", "", cnpj)
    if len(digits) != 14:
        raise ValueError(
            f"CNPJ deve conter 14 digitos numericos, recebido {len(digits)}: {cnpj!r}"
        )
    if not _validar_cnpj(digits):
        raise ValueError(f"CNPJ invalido (digitos verificadores incorretos): {cnpj!r}")
    return digits


def validate_path_exists(path: str | Path) -> Path:
    """Verifica existencia e permissao de leitura; levanta ValueError se invalido."""
    if isinstance(path, str) and not path.strip():
        raise ValueError("Caminho vazio.")
    p = Path(path)
    if not p.exists():
        raise ValueError(f"Caminho nao encontrado: {p}")
    if not os.access(p, os.R_OK):
        raise ValueError(f"Sem permissao de leitura: {p}")
    try:
        if p.is_file():
            with p.open("rb"):
                pass
        else:
            next(p.iterdir(), None)
    except OSError as exc:
        raise ValueError(f"Sem permissao de leitura: {p}") from exc
    return p


def _parse_date(s: str) -> date:
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    raise ValueError(
        f"Formato de data invalido: {s!r}. Use YYYY-MM-DD ou dd/mm/aaaa."
    )


def validate_date_range(start: str, end: str) -> tuple[date, date]:
    """Aceita ISO (YYYY-MM-DD) ou dd/mm/aaaa; garante start <= end."""
    d_start = _parse_date(start)
    d_end = _parse_date(end)
    if d_start > d_end:
        raise ValueError(
            f"Data inicial ({start}) deve ser anterior ou igual a data final ({end})."
        )
    return d_start, d_end
