import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from interface_grafica.fisconforme.extrator_oracle import ExtratorOracle


@pytest.fixture
def mock_extrator():
    # Mocking __init__ to avoid requiring Oracle environment variables
    with patch.object(ExtratorOracle, "__init__", lambda self: None):
        extrator = ExtratorOracle()
        extrator.pasta_dados = Path("/mock/path")
        extrator.logger = MagicMock()
        return extrator


def test_extrair_tabela_valid_identifiers(mock_extrator):
    # Should not raise any ValueError
    # We mock _executar_com_retry so it doesn't actually try to connect to a DB
    with patch.object(mock_extrator, "_executar_com_retry", return_value=True):
        with patch.object(mock_extrator, "_validar_parquet", return_value=True):
            result = mock_extrator.extrair_tabela("BI", "DM_PESSOA", "CO_CNPJ_CPF", "1234")
            assert result is True


def test_extrair_tabela_invalid_schema(mock_extrator):
    with pytest.raises(ValueError, match="Nome de schema inválido"):
        mock_extrator.extrair_tabela("BI; DROP TABLE", "DM_PESSOA")


def test_extrair_tabela_invalid_tabela(mock_extrator):
    with pytest.raises(ValueError, match="Nome de tabela inválido"):
        mock_extrator.extrair_tabela("BI", "DM_PESSOA;--")


def test_extrair_tabela_invalid_filtro_coluna(mock_extrator):
    with pytest.raises(ValueError, match="Nome de coluna de filtro inválido"):
        mock_extrator.extrair_tabela("BI", "DM_PESSOA", "CO_CNPJ_CPF;--", "1234")


def test_extrair_tabela_spaces_not_allowed(mock_extrator):
    with pytest.raises(ValueError, match="Nome de schema inválido"):
        mock_extrator.extrair_tabela("MY SCHEMA", "TABLE")
