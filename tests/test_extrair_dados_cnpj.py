import pytest
from unittest.mock import MagicMock
from src.extracao.extrair_dados_cnpj import close_thread_connection, thread_local

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
