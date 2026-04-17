import pytest
from src.utilitarios.validar_cnpj import validar_cnpj


def test_validar_cnpj_valido_formatado():
    """Testa um CNPJ válido com formatação."""
    assert validar_cnpj("11.222.333/0001-81") is True
    assert validar_cnpj("00.000.000/0001-91") is True


def test_validar_cnpj_valido_nao_formatado():
    """Testa um CNPJ válido sem formatação."""
    assert validar_cnpj("11222333000181") is True
    assert validar_cnpj("00000000000191") is True


def test_validar_cnpj_digitos_invalidos():
    """Testa CNPJs com dígitos verificadores incorretos."""
    assert validar_cnpj("11.222.333/0001-82") is False
    assert validar_cnpj("00.000.000/0001-92") is False


def test_validar_cnpj_tamanho_incorreto():
    """Testa CNPJs com tamanho diferente de 14 dígitos numéricos."""
    assert validar_cnpj("11.222.333/0001-8") is False  # 13 dígitos
    assert validar_cnpj("11.222.333/0001-811") is False  # 15 dígitos
    assert validar_cnpj("12345") is False


def test_validar_cnpj_digitos_iguais():
    """Testa CNPJs com todos os dígitos iguais (inválidos por regra)."""
    assert validar_cnpj("00.000.000/0000-00") is False
    assert validar_cnpj("11.111.111/1111-11") is False
    assert validar_cnpj("99.999.999/9999-99") is False


def test_validar_cnpj_vazio():
    """Testa CNPJ com string vazia."""
    assert validar_cnpj("") is False


def test_validar_cnpj_caracteres_extras():
    """Testa se a função remove corretamente caracteres não numéricos e valida."""
    # "11a222b333/0001-81" deve ser limpo para "11222333000181" e ser válido
    assert validar_cnpj("11a222b333/0001-81") is True
