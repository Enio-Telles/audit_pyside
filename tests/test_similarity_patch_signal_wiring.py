from __future__ import annotations

import importlib
import sys
from types import ModuleType


class _DummySignal:
    def __init__(self) -> None:
        self.connect_calls = 0
        self.disconnect_calls = 0

    def connect(self, _slot) -> None:
        self.connect_calls += 1

    def disconnect(self, _slot=None) -> None:
        self.disconnect_calls += 1
        raise RuntimeWarning("disconnect should not be called")


class _DummyButton:
    def __init__(self) -> None:
        self.clicked = _DummySignal()


def test_similarity_patch_connects_sort_button_once_without_disconnect(monkeypatch):
    qtwidgets_stub = ModuleType("PySide6.QtWidgets")
    qtwidgets_stub.QCheckBox = object
    qtwidgets_stub.QComboBox = object
    qtwidgets_stub.QLabel = object
    qtwidgets_stub.QPushButton = object
    monkeypatch.setitem(sys.modules, "PySide6", ModuleType("PySide6"))
    monkeypatch.setitem(sys.modules, "PySide6.QtWidgets", qtwidgets_stub)
    monkeypatch.delitem(
        sys.modules,
        "interface_grafica.patches.similaridade_agregacao",
        raising=False,
    )

    class AgregacaoControllerMixin:
        pass

    class AgregacaoWindowMixin:
        def _build_tab_agregacao(self):
            return object()

    class MainWindowSignalWiringCoreMixin:
        def _connect_consulta_agregacao_signals(self) -> None:
            self.original_connect_calls += 1

    controller_module = ModuleType("interface_grafica.controllers.agregacao_controller")
    controller_module.AgregacaoControllerMixin = AgregacaoControllerMixin
    window_module = ModuleType("interface_grafica.windows.aba_agregacao")
    window_module.AgregacaoWindowMixin = AgregacaoWindowMixin
    wiring_module = ModuleType("interface_grafica.windows.main_window_signal_wiring_core")
    wiring_module.MainWindowSignalWiringCoreMixin = MainWindowSignalWiringCoreMixin

    monkeypatch.setitem(
        sys.modules,
        "interface_grafica.controllers.agregacao_controller",
        controller_module,
    )
    monkeypatch.setitem(sys.modules, "interface_grafica.windows.aba_agregacao", window_module)
    monkeypatch.setitem(
        sys.modules,
        "interface_grafica.windows.main_window_signal_wiring_core",
        wiring_module,
    )

    patch_module = importlib.import_module("interface_grafica.patches.similaridade_agregacao")
    patch_module.apply_similarity_patch()

    class MainWindow(MainWindowSignalWiringCoreMixin, AgregacaoControllerMixin):
        def __init__(self) -> None:
            self.original_connect_calls = 0
            self.btn_ordenar_similaridade_desc = _DummyButton()

    window = MainWindow()
    window._connect_consulta_agregacao_signals()
    window._connect_consulta_agregacao_signals()

    signal = window.btn_ordenar_similaridade_desc.clicked
    assert window.original_connect_calls == 2
    assert signal.connect_calls == 1
    assert signal.disconnect_calls == 0
