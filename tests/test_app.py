import sys
from unittest.mock import patch, MagicMock

import app

@patch("app.QApplication")
@patch("app.MainWindow")
def test_main(mock_main_window_class, mock_qapplication_class):
    # Setup mocks
    mock_app_instance = MagicMock()
    mock_qapplication_class.return_value = mock_app_instance
    mock_app_instance.exec.return_value = 0

    mock_window_instance = MagicMock()
    mock_main_window_class.return_value = mock_window_instance

    # Execute the function
    result = app.main()

    # Verify interactions
    mock_qapplication_class.assert_called_once_with(sys.argv)
    mock_app_instance.setApplicationName.assert_called_once_with("Fiscal Parquet Analyzer (Refatorado)")

    mock_main_window_class.assert_called_once()
    mock_window_instance.show.assert_called_once()

    mock_app_instance.exec.assert_called_once()

    # Verify return value
    assert result == 0

@patch("app.QApplication")
@patch("app.MainWindow")
def test_main_error_code(mock_main_window_class, mock_qapplication_class):
    # Setup mocks for error code return
    mock_app_instance = MagicMock()
    mock_qapplication_class.return_value = mock_app_instance
    mock_app_instance.exec.return_value = 1

    mock_window_instance = MagicMock()
    mock_main_window_class.return_value = mock_window_instance

    # Execute the function
    result = app.main()

    # Verify interactions
    mock_qapplication_class.assert_called_once_with(sys.argv)
    mock_app_instance.setApplicationName.assert_called_once_with("Fiscal Parquet Analyzer (Refatorado)")

    mock_main_window_class.assert_called_once()
    mock_window_instance.show.assert_called_once()

    mock_app_instance.exec.assert_called_once()

    # Verify return value
    assert result == 1
