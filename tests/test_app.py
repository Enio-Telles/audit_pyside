import importlib
import sys
from types import ModuleType
from unittest.mock import MagicMock

import pytest


@pytest.fixture()
def app_module(monkeypatch):
    qtwidgets_stub = ModuleType("PySide6.QtWidgets")
    qtwidgets_stub.QApplication = MagicMock(name="QApplication")

    logging_setup_stub = ModuleType("interface_grafica.logging_setup")
    logging_setup_stub.configure_structlog = MagicMock(name="configure_structlog")
    logging_setup_stub.install_fallback_hooks = MagicMock(name="install_fallback_hooks")

    patches_stub = ModuleType("interface_grafica.patches")
    similaridade_patch_stub = ModuleType(
        "interface_grafica.patches.similaridade_agregacao"
    )
    similaridade_patch_stub.apply_similarity_patch = MagicMock(
        name="apply_similarity_patch"
    )

    interface_grafica_stub = ModuleType("interface_grafica")
    interface_grafica_stub.__path__ = []

    windows_stub = ModuleType("interface_grafica.windows")
    windows_main_window_stub = ModuleType("interface_grafica.windows.main_window")
    windows_main_window_stub.MainWindow = MagicMock(name="MainWindow")

    monkeypatch.setitem(sys.modules, "PySide6", ModuleType("PySide6"))
    monkeypatch.setitem(sys.modules, "PySide6.QtWidgets", qtwidgets_stub)
    monkeypatch.setitem(sys.modules, "interface_grafica", interface_grafica_stub)
    monkeypatch.setitem(sys.modules, "interface_grafica.logging_setup", logging_setup_stub)
    monkeypatch.setitem(sys.modules, "interface_grafica.patches", patches_stub)
    monkeypatch.setitem(
        sys.modules,
        "interface_grafica.patches.similaridade_agregacao",
        similaridade_patch_stub,
    )
    monkeypatch.setitem(sys.modules, "interface_grafica.windows", windows_stub)
    monkeypatch.setitem(
        sys.modules, "interface_grafica.windows.main_window", windows_main_window_stub
    )
    monkeypatch.delitem(sys.modules, "app", raising=False)

    module = importlib.import_module("app")
    yield module
    sys.modules.pop("app", None)


def test_main(app_module):
    mock_qapplication_class = app_module.QApplication
    mock_main_window_class = app_module.MainWindow

    # Setup mocks
    mock_app_instance = MagicMock()
    mock_qapplication_class.return_value = mock_app_instance
    mock_app_instance.exec.return_value = 0

    mock_window_instance = MagicMock()
    mock_main_window_class.return_value = mock_window_instance

    # Execute the function
    result = app_module.main()

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


def test_main_error_code(app_module):
    mock_qapplication_class = app_module.QApplication
    mock_main_window_class = app_module.MainWindow

    # Setup mocks for error code return
    mock_app_instance = MagicMock()
    mock_qapplication_class.return_value = mock_app_instance
    mock_app_instance.exec.return_value = 1

    mock_window_instance = MagicMock()
    mock_main_window_class.return_value = mock_window_instance

    # Execute the function
    result = app_module.main()

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
