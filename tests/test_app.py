import sys
from types import ModuleType
from unittest.mock import MagicMock, patch


qtwidgets_stub = ModuleType("PySide6.QtWidgets")
qtwidgets_stub.QApplication = MagicMock(name="QApplication")

main_window_stub = ModuleType("interface_grafica.ui.main_window")
main_window_stub.MainWindow = MagicMock(name="MainWindow")

sys.modules.setdefault("PySide6", ModuleType("PySide6"))
sys.modules["PySide6.QtWidgets"] = qtwidgets_stub
sys.modules.setdefault("interface_grafica", ModuleType("interface_grafica"))
sys.modules.setdefault("interface_grafica.ui", ModuleType("interface_grafica.ui"))
sys.modules["interface_grafica.ui.main_window"] = main_window_stub

import app  # noqa: E402


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
    mock_app_instance.setApplicationName.assert_called_once_with(
        "Fiscal Parquet Analyzer (Refatorado)"
    )

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
    mock_app_instance.setApplicationName.assert_called_once_with(
        "Fiscal Parquet Analyzer (Refatorado)"
    )

    mock_main_window_class.assert_called_once()
    mock_window_instance.show.assert_called_once()

    mock_app_instance.exec.assert_called_once()

    # Verify return value
    assert result == 1
