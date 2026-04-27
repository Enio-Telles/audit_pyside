from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from interface_grafica.utils.validators import (
    validate_cnpj,
    validate_date_range,
    validate_path_exists,
)

# CNPJs conhecidos: 11222333000181 e 00000000000191 sao validos (cf. test_validar_cnpj.py)
CNPJ_VALIDO = "11222333000181"
CNPJ_INVALIDO_CHECKSUM = "11222333000182"


def test_validate_cnpj_digits_only() -> None:
    assert validate_cnpj(CNPJ_VALIDO) == CNPJ_VALIDO


def test_validate_cnpj_masked() -> None:
    assert validate_cnpj("11.222.333/0001-81") == CNPJ_VALIDO


def test_validate_cnpj_empty() -> None:
    with pytest.raises(ValueError, match="14 digitos"):
        validate_cnpj("")


def test_validate_cnpj_too_short() -> None:
    with pytest.raises(ValueError, match="14 digitos"):
        validate_cnpj("1234567890123")


def test_validate_cnpj_rejects_cpf_length() -> None:
    with pytest.raises(ValueError, match="14 digitos"):
        validate_cnpj("12345678901")


def test_validate_cnpj_invalid_checksum() -> None:
    with pytest.raises(ValueError, match="invalido"):
        validate_cnpj(CNPJ_INVALIDO_CHECKSUM)


def test_validate_path_exists_file(tmp_path: Path) -> None:
    f = tmp_path / "arquivo.parquet"
    f.write_bytes(b"dados")
    result = validate_path_exists(f)
    assert result == f


def test_validate_path_exists_missing(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="nao encontrado"):
        validate_path_exists(tmp_path / "inexistente.parquet")


def test_validate_path_exists_blank_string() -> None:
    with pytest.raises(ValueError, match="Caminho vazio"):
        validate_path_exists("")


def test_validate_date_range_iso() -> None:
    s, e = validate_date_range("2024-01-01", "2024-12-31")
    assert s == date(2024, 1, 1)
    assert e == date(2024, 12, 31)


def test_validate_date_range_ptbr() -> None:
    s, e = validate_date_range("01/01/2024", "31/12/2024")
    assert s == date(2024, 1, 1)
    assert e == date(2024, 12, 31)

def test_validate_date_range_inverted() -> None:
    with pytest.raises(ValueError, match="anterior ou igual"):
        validate_date_range("2024-12-31", "2024-01-01")


def test_validate_date_range_invalid_format() -> None:
    with pytest.raises(ValueError, match="Formato de data invalido"):
        validate_date_range("nao-e-data", "2024-01-01")
