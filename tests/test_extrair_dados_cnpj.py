import pytest
import threading

from extracao.extrair_dados_cnpj import get_thread_connection, thread_local

def test_get_thread_connection_returns_none_when_connection_fails(mocker):
    # Ensure a clean state for thread_local before the test
    if hasattr(thread_local, "conexao"):
        del thread_local.conexao

    # Patch conectar_oracle to return None
    mock_conectar = mocker.patch("extracao.extrair_dados_cnpj.conectar_oracle", return_value=None)

    # Patch the logger
    mock_logger = mocker.patch("extracao.extrair_dados_cnpj.logger")

    # Call the function
    result = get_thread_connection()

    # Assertions
    assert result is None
    mock_conectar.assert_called_once()
    mock_logger.error.assert_called_once_with(f"[{threading.current_thread().name}] Falha ao criar conexão com banco de dados.")


def test_get_thread_connection_returns_none_when_connection_test_fails(mocker):
    # Ensure a clean state for thread_local before the test
    if hasattr(thread_local, "conexao"):
        del thread_local.conexao

    # Patch conectar_oracle to return a mock connection that raises on cursor.execute
    mock_conn = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.execute.side_effect = Exception("Test exception")

    mock_conectar = mocker.patch("extracao.extrair_dados_cnpj.conectar_oracle", return_value=mock_conn)

    # Patch the logger
    mock_logger = mocker.patch("extracao.extrair_dados_cnpj.logger")

    # Call the function
    result = get_thread_connection()

    # Assertions
    assert result is None
    mock_conectar.assert_called_once()
    mock_logger.error.assert_called_once()
    assert "Erro ao testar conexão: Test exception" in mock_logger.error.call_args[0][0]
