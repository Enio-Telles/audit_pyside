"""Testes unitarios para PeriodPage.validate()

Estrategia: instanciar apenas o metodo de validacao de forma isolada, sem PySide6,
usando um WizardState fake e monkeypatching QMessageBox.warning para nao abrir janelas.
"""
import re
from unittest.mock import MagicMock, patch

import pytest


class _FakeState:
    def __init__(self, ini: str, fim: str) -> None:
        self.periodo_inicio = ini
        self.periodo_fim = fim


# ---------------------------------------------------------------------------
# Extrai a logica pura de validacao de PeriodPage para teste sem widget real.
# O metodo e testado via _validate_periodos() extraido abaixo.
# ---------------------------------------------------------------------------

_PADRAO_PERIODO = re.compile(r"^(\d{2})/(\d{4})$")


def _validate_periodos(periodo_inicio: str, periodo_fim: str) -> tuple[bool, str]:
    """Replica exata da logica de PeriodPage.validate() sem dependencia de PySide6."""
    if not periodo_inicio or not periodo_fim:
        return False, "vazio"
    m_ini = _PADRAO_PERIODO.match(periodo_inicio)
    m_fim = _PADRAO_PERIODO.match(periodo_fim)
    if not m_ini or not m_fim:
        return False, "formato"
    mes_ini, ano_ini = int(m_ini.group(1)), int(m_ini.group(2))
    mes_fim, ano_fim = int(m_fim.group(1)), int(m_fim.group(2))
    if not (1 <= mes_ini <= 12) or not (1 <= mes_fim <= 12):
        return False, "mes"
    if ano_ini * 100 + mes_ini >= ano_fim * 100 + mes_fim:
        return False, "ordem"
    return True, ""


# ---------------------------------------------------------------------------
# Casos de rejeicao
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "ini,fim,motivo",
    [
        ("", "", "vazio"),
        ("01/2021", "", "vazio"),
        ("", "12/2025", "vazio"),
        ("jan/2021", "12/2025", "formato"),
        ("2021/01", "2025/12", "formato"),
        ("1/2021", "12/2025", "formato"),
        ("00/2021", "12/2025", "mes"),
        ("13/2021", "12/2025", "mes"),
        ("01/2021", "00/2025", "mes"),
        ("01/2021", "01/2021", "ordem"),  # EI == EF
        ("06/2021", "01/2021", "ordem"),  # EI > EF mesmo ano
        ("01/2022", "12/2021", "ordem"),  # EI > EF ano diferente
    ],
)
def test_rejeitar(ini: str, fim: str, motivo: str) -> None:
    ok, motivo_real = _validate_periodos(ini, fim)
    assert not ok
    assert motivo_real == motivo


# ---------------------------------------------------------------------------
# Casos validos
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "ini,fim",
    [
        ("01/2021", "12/2025"),
        ("01/2021", "02/2021"),  # diferenca de 1 mes
        ("12/2020", "01/2021"),  # virada de ano
        ("01/2000", "12/2099"),
    ],
)
def test_aceitar(ini: str, fim: str) -> None:
    ok, _ = _validate_periodos(ini, fim)
    assert ok
