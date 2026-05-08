import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from interface_grafica.fisconforme.extrator_oracle import ExtratorOracle


@pytest.fixture
def mock_extrator(tmp_path):
    # Mocking __init__ to avoid requiring Oracle environment variables
    with patch.object(ExtratorOracle, "__init__", lambda self: None):
        extrator = ExtratorOracle()
        extrator.pasta_dados = tmp_path
        extrator.logger = MagicMock()
        extrator.MAX_TENTATIVAS = 3
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


def test_oracle_identifiers_special_chars(mock_extrator):
    """Validar identificadores Oracle com _, $, #."""
    with patch.object(mock_extrator, "_executar_com_retry", return_value=True):
        with patch.object(mock_extrator, "_validar_parquet", return_value=True):
            # Devem ser aceitos
            assert mock_extrator.extrair_tabela("SCHEMA_1", "TABELA$2", "COLUNA#3", "123") is True


@pytest.mark.parametrize(
    "invalid_id",
    [
        "schema.name",  # ponto não permitido
        "table ",  # espaço no fim
        " table",  # espaço no início
        "table'name",  # aspas simples
        'table"name',  # aspas duplas
        "table--",  # comentário
        "table/*",  # comentário bloco
        "table/",  # barra
        "",  # vazio
    ],
)
def test_oracle_identifiers_rejection(mock_extrator, invalid_id):
    """Validar rejeição de ., espaços, aspas, comentários, barras e strings vazias."""
    with pytest.raises(ValueError):
        mock_extrator.extrair_tabela(invalid_id, "TABELA")
    with pytest.raises(ValueError):
        mock_extrator.extrair_tabela("SCHEMA", invalid_id)
    with pytest.raises(ValueError):
        mock_extrator.extrair_tabela("SCHEMA", "TABELA", invalid_id, "valor")


def test_valor_filtro_bind_parameter(mock_extrator):
    """Validar que valor_filtro continua usando bind parameter."""
    # Precisamos capturar a chamada ao cursor.execute
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    # Mocking cursor used in context manager: with conn.cursor() as cursor
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_conn.__enter__.return_value = mock_conn  # for 'with self.obter_conexao() as conn'
    mock_extrator.obter_conexao = MagicMock(return_value=mock_conn)

    # Mock _gravar_cursor_em_parquet para evitar escrita real e garantir que caia no else
    with patch(
        "interface_grafica.fisconforme.extrator_oracle._gravar_cursor_em_parquet", new=MagicMock()
    ) as mock_gravar:
        # We need to ensure _gravar_cursor_em_parquet is NOT None inside the module
        import interface_grafica.fisconforme.extrator_oracle as eo

        with patch.object(eo, "_gravar_cursor_em_parquet", mock_gravar):
            with patch.object(mock_extrator, "_validar_parquet", return_value=True):
                # Desabilitar retry para facilitar teste
                with patch.object(mock_extrator, "MAX_TENTATIVAS", 1):
                    mock_extrator.extrair_tabela(
                        "SCHEMA", "TABELA", "COLUNA", "valor_sensivel' OR 1=1"
                    )

    # Verifica se o cursor.execute foi chamado com o bind :valor e se o valor foi passado
    # Pode ser chamado via cursor.execute(sql, params)
    call_args_list = mock_cursor.execute.call_args_list
    # In extrair_tabela, it calls execute if _gravar_cursor_em_parquet is NOT None
    assert len(call_args_list) > 0, "cursor.execute não foi chamado"

    found_bind = False
    for args, kwargs in call_args_list:
        sql = args[0]
        params = args[1] if len(args) > 1 else kwargs.get("parameters") or kwargs.get("params")
        if "WHERE COLUNA = :valor" in sql and params == {"valor": "valor_sensivel' OR 1=1"}:
            found_bind = True
            break

    assert found_bind, f"Bind parameter não encontrado nas chamadas: {call_args_list}"
