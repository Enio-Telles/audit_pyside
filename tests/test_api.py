import pytest
from fastapi import HTTPException
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../server/python'))
from api import _normalizar_cnpj

def test_normalizar_cnpj_valid_with_mask():
    assert _normalizar_cnpj("12.345.678/0001-90") == "12345678000190"

def test_normalizar_cnpj_valid_numeric_only():
    assert _normalizar_cnpj("12345678000190") == "12345678000190"

def test_normalizar_cnpj_invalid_length_short():
    with pytest.raises(HTTPException) as exc_info:
        _normalizar_cnpj("123")
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "CNPJ deve conter 14 digitos"

def test_normalizar_cnpj_invalid_length_long():
    with pytest.raises(HTTPException) as exc_info:
        _normalizar_cnpj("123456780001901")
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "CNPJ deve conter 14 digitos"

def test_normalizar_cnpj_empty_string():
    with pytest.raises(HTTPException) as exc_info:
        _normalizar_cnpj("")
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "CNPJ deve conter 14 digitos"

def test_normalizar_cnpj_none_raises_type_error():
    with pytest.raises(TypeError):
        _normalizar_cnpj(None)
