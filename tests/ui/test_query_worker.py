import os
import polars as pl
import pytest
from unittest.mock import MagicMock, patch

# Configure Qt for headless tests
os.environ["QT_QPA_PLATFORM"] = "offscreen"

from src.interface_grafica.services.query_worker import QueryWorker, QueryCancelledError


def test_query_worker_happy_path(mocker):
    """
    Test the successful execution of a query in QueryWorker.
    """
    worker = QueryWorker("SELECT * FROM DUAL", {})
    mocker.patch.object(worker, "isInterruptionRequested", return_value=False)

    # Mock signals
    progress_msgs = []
    finished_dfs = []
    failed_msgs = []

    worker.progress.connect(progress_msgs.append)
    worker.finished_ok.connect(finished_dfs.append)
    worker.failed.connect(failed_msgs.append)

    # Mock the database connection and cursor
    conn_mock = MagicMock()
    cursor_mock = MagicMock()
    conn_mock.cursor.return_value.__enter__.return_value = cursor_mock
    cursor_mock.description = [("DUMMY",)]
    cursor_mock.fetchmany.side_effect = [[("X",)], []]

    mocker.patch("src.interface_grafica.services.query_worker.conectar_oracle", return_value=conn_mock)

    worker.run()

    assert len(failed_msgs) == 0
    assert len(finished_dfs) == 1

    df = finished_dfs[0]
    assert isinstance(df, pl.DataFrame)
    assert df.height == 1
    assert df.columns == ["DUMMY"]
    assert df.to_dicts() == [{"DUMMY": "X"}]

    assert "Conectando ao Oracle..." in progress_msgs
    assert "Executando consulta..." in progress_msgs


def test_query_worker_cancellation(mocker):
    """
    Test that cancelling the QueryWorker correctly aborts the query and emits a failed signal.
    """
    worker = QueryWorker("SELECT * FROM DUAL", {})
    # Return True to simulate cancellation
    mocker.patch.object(worker, "isInterruptionRequested", return_value=True)

    # Mock signals
    progress_msgs = []
    failed_msgs = []

    worker.progress.connect(progress_msgs.append)
    worker.failed.connect(failed_msgs.append)

    conn_mock = MagicMock()
    mocker.patch("src.interface_grafica.services.query_worker.conectar_oracle", return_value=conn_mock)

    worker.run()

    assert len(failed_msgs) == 1
    assert failed_msgs[0] == "Consulta cancelada pelo usuario."
    assert "Consulta cancelada." in progress_msgs


def test_query_worker_error_path(mocker):
    """
    Test that an exception during query execution is caught and a sanitized error message is emitted.
    """
    worker = QueryWorker("SELECT * FROM DUAL", {})
    mocker.patch.object(worker, "isInterruptionRequested", return_value=False)

    # Mock signals
    failed_msgs = []
    worker.failed.connect(failed_msgs.append)

    # Mock to throw an exception
    mocker.patch("src.interface_grafica.services.query_worker.conectar_oracle", side_effect=Exception("Simulated internal error"))

    worker.run()

    assert len(failed_msgs) == 1
    # Check that the error message is sanitized, as mandated by the Sentinel rule.
    assert failed_msgs[0] == "Ocorreu um erro ao executar a consulta no banco de dados. Verifique os logs para mais detalhes."


def test_query_worker_fallback_path(mocker):
    """
    Test the fallback path when conectar_oracle is None.
    """
    worker = QueryWorker("SELECT * FROM DUAL", {})
    mocker.patch.object(worker, "isInterruptionRequested", return_value=False)

    # Mock signals
    finished_dfs = []
    worker.finished_ok.connect(finished_dfs.append)

    # Set conectar_oracle to None to force fallback
    mocker.patch("src.interface_grafica.services.query_worker.conectar_oracle", None)

    conn_mock = MagicMock()
    cursor_mock = MagicMock()
    conn_mock.cursor.return_value.__enter__.return_value = cursor_mock
    cursor_mock.description = [("DUMMY",)]
    cursor_mock.fetchmany.side_effect = [[("X",)], []]

    fallback_mock = mocker.patch("src.interface_grafica.services.query_worker._conectar_oracle_fallback", return_value=conn_mock)

    worker.run()

    assert fallback_mock.called
    assert len(finished_dfs) == 1

    df = finished_dfs[0]
    assert df.to_dicts() == [{"DUMMY": "X"}]
