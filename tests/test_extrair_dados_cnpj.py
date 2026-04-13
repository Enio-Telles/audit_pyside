import pytest
import threading
from unittest.mock import MagicMock

from extracao.extrair_dados_cnpj import close_thread_connection, get_thread_connection, thread_local

def test_close_thread_connection_success():
    # Setup mock connection
    mock_conn = MagicMock()
    thread_local.conexao = mock_conn

    # Call the function
    close_thread_connection()

    # Verify close was called
    mock_conn.close.assert_called_once()

    # Verify the connection was set to None
    assert getattr(thread_local, "conexao", "Not found") is None

def test_close_thread_connection_exception():
    # Setup mock connection that raises an exception on close
    mock_conn = MagicMock()
    mock_conn.close.side_effect = Exception("Failed to close")
    thread_local.conexao = mock_conn

    # Call the function - it should catch the exception and proceed
    close_thread_connection()

    # Verify close was attempted
    mock_conn.close.assert_called_once()

    # Verify the connection was STILL set to None despite the exception
    assert getattr(thread_local, "conexao", "Not found") is None

def test_close_thread_connection_is_none():
    # Setup thread_local with conexao explicitly set to None
    thread_local.conexao = None

    # Call the function
    close_thread_connection()

    # Verify it remains None and didn't crash
    assert getattr(thread_local, "conexao", "Not found") is None

def test_close_thread_connection_no_attr():
    # Setup thread_local without the conexao attribute
    if hasattr(thread_local, "conexao"):
        delattr(thread_local, "conexao")

    # Call the function
    close_thread_connection()

    # Verify it didn't crash and the attribute is still not there
    assert hasattr(thread_local, "conexao") is False


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
