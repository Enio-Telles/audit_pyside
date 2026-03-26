import pytest
import threading
from unittest.mock import patch, MagicMock

from src.extracao.extrair_dados_cnpj import get_thread_connection, close_thread_connection, thread_local

def test_get_thread_connection_creates_new():
    # Ensure thread_local is clean
    if hasattr(thread_local, "conexao"):
        del thread_local.conexao

    with patch("src.extracao.extrair_dados_cnpj.conectar") as mock_conectar:
        mock_conn = MagicMock()
        mock_conectar.return_value = mock_conn

        conn = get_thread_connection()

        assert conn is mock_conn
        assert mock_conectar.call_count == 1
        assert thread_local.conexao is mock_conn

def test_get_thread_connection_reuses_existing():
    if hasattr(thread_local, "conexao"):
        del thread_local.conexao

    with patch("src.extracao.extrair_dados_cnpj.conectar") as mock_conectar:
        mock_conn = MagicMock()
        mock_conectar.return_value = mock_conn

        conn1 = get_thread_connection()
        conn2 = get_thread_connection()

        assert conn1 is mock_conn
        assert conn2 is mock_conn
        assert mock_conectar.call_count == 1

def test_get_thread_connection_thread_isolation():
    if hasattr(thread_local, "conexao"):
        del thread_local.conexao

    with patch("src.extracao.extrair_dados_cnpj.conectar") as mock_conectar:
        # Each call returns a new mock object
        mock_conectar.side_effect = lambda: MagicMock()

        conn_main = get_thread_connection()

        conn_thread = []
        def worker():
            conn_thread.append(get_thread_connection())

        t = threading.Thread(target=worker)
        t.start()
        t.join()

        assert len(conn_thread) == 1
        assert conn_main is not conn_thread[0]
        assert mock_conectar.call_count == 2

def test_close_thread_connection():
    if hasattr(thread_local, "conexao"):
        del thread_local.conexao

    with patch("src.extracao.extrair_dados_cnpj.conectar") as mock_conectar:
        mock_conn = MagicMock()
        mock_conectar.return_value = mock_conn

        get_thread_connection()
        assert thread_local.conexao is mock_conn

        close_thread_connection()

        assert mock_conn.close.call_count == 1
        assert getattr(thread_local, "conexao", None) is None

def test_close_thread_connection_exception_handling():
    if hasattr(thread_local, "conexao"):
        del thread_local.conexao

    with patch("src.extracao.extrair_dados_cnpj.conectar") as mock_conectar:
        mock_conn = MagicMock()
        mock_conn.close.side_effect = Exception("Fake DB Close Error")
        mock_conectar.return_value = mock_conn

        get_thread_connection()
        assert thread_local.conexao is mock_conn

        # This should not raise the exception
        close_thread_connection()

        assert mock_conn.close.call_count == 1
        assert getattr(thread_local, "conexao", None) is None

def test_close_thread_connection_no_connection():
    # Ensure thread_local is clean
    if hasattr(thread_local, "conexao"):
        del thread_local.conexao

    # This shouldn't raise any errors
    close_thread_connection()
