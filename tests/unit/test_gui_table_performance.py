from __future__ import annotations

import importlib
import sys
from types import ModuleType

import polars as pl
import pytest


def _stub_support_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    qtcore = ModuleType("PySide6.QtCore")
    qtcore.QThread = object
    qtcore.Qt = object
    qtcore.QTimer = object
    qtgui = ModuleType("PySide6.QtGui")
    qtgui.QGuiApplication = object
    qtgui.QKeySequence = object
    qtgui.QShortcut = object
    qtwidgets = ModuleType("PySide6.QtWidgets")
    for name in ("QMenu", "QMessageBox", "QPushButton", "QTableView"):
        setattr(qtwidgets, name, object)
    monkeypatch.setitem(sys.modules, "PySide6.QtCore", qtcore)
    monkeypatch.setitem(sys.modules, "PySide6.QtGui", qtgui)
    monkeypatch.setitem(sys.modules, "PySide6.QtWidgets", qtwidgets)

    table_model = ModuleType("interface_grafica.models.table_model")
    table_model.PolarsTableModel = object
    monkeypatch.setitem(sys.modules, "interface_grafica.models.table_model", table_model)

    parquet_service = ModuleType("interface_grafica.services.parquet_service")
    parquet_service.FilterCondition = object
    monkeypatch.setitem(
        sys.modules, "interface_grafica.services.parquet_service", parquet_service
    )

    detached = ModuleType("interface_grafica.widgets.detached_table_window")
    detached.DetachedTableWindow = object
    monkeypatch.setitem(
        sys.modules, "interface_grafica.widgets.detached_table_window", detached
    )
    sys.modules.pop("interface_grafica.windows.main_window_support", None)


def _load_support_mixin(monkeypatch: pytest.MonkeyPatch):
    _stub_support_dependencies(monkeypatch)
    return importlib.import_module(
        "interface_grafica.windows.main_window_support"
    ).MainWindowSupportMixin


def _stub_table_model_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    class SignalStub:
        def emit(self, *_args, **_kwargs) -> None:
            return None

    class AbstractModelStub:
        def __init__(self, *_args, **_kwargs) -> None:
            self.layoutAboutToBeChanged = SignalStub()
            self.layoutChanged = SignalStub()
            self.dataChanged = SignalStub()

        def beginResetModel(self) -> None:  # noqa: N802
            return None

        def endResetModel(self) -> None:  # noqa: N802
            return None

        def flags(self, _index):
            return 0

    class QtStub:
        DisplayRole = 0
        ToolTipRole = 1
        ForegroundRole = 2
        BackgroundRole = 3
        FontRole = 4
        CheckStateRole = 5
        EditRole = 6
        Checked = 2
        Unchecked = 0
        DescendingOrder = 1
        AscendingOrder = 0
        CheckState = int
        ItemIsEnabled = 1
        ItemIsSelectable = 2
        ItemIsUserCheckable = 4
        ItemIsEditable = 8
        ItemFlags = int
        Orientation = int
        SortOrder = int

    qtcore = ModuleType("PySide6.QtCore")
    qtcore.QAbstractTableModel = AbstractModelStub
    qtcore.QModelIndex = object
    qtcore.Qt = QtStub
    qtgui = ModuleType("PySide6.QtGui")
    qtgui.QBrush = lambda value: value
    qtgui.QColor = lambda value: value
    qtgui.QFont = object
    monkeypatch.setitem(sys.modules, "PySide6.QtCore", qtcore)
    monkeypatch.setitem(sys.modules, "PySide6.QtGui", qtgui)
    sys.modules.pop("interface_grafica.models.table_model", None)


def _load_table_model(monkeypatch: pytest.MonkeyPatch):
    _stub_table_model_dependencies(monkeypatch)
    return importlib.import_module(
        "interface_grafica.models.table_model"
    ).PolarsTableModel


class HeaderSpy:
    def __init__(self) -> None:
        self.precision = 10_000
        self.precisions: list[int] = []
        self.blocked = False
        self.block_calls: list[bool] = []

    def resizeContentsPrecision(self) -> int:  # noqa: N802
        return self.precision

    def setResizeContentsPrecision(self, value: int) -> None:  # noqa: N802
        self.precision = value
        self.precisions.append(value)

    def blockSignals(self, blocked: bool) -> bool:  # noqa: N802
        previous = self.blocked
        self.blocked = blocked
        self.block_calls.append(blocked)
        return previous


class TableSpy:
    def __init__(self) -> None:
        self.header = HeaderSpy()
        self.resize_calls = 0
        self.resize_precision_seen: int | None = None
        self.signals_blocked_during_resize: bool | None = None

    def horizontalHeader(self) -> HeaderSpy:  # noqa: N802
        return self.header

    def resizeColumnsToContents(self) -> None:  # noqa: N802
        self.resize_calls += 1
        self.resize_precision_seen = self.header.precision
        self.signals_blocked_during_resize = self.header.blocked


def test_resize_table_once_limits_scan_and_suppresses_header_signals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mixin_cls = _load_support_mixin(monkeypatch)
    mixin = mixin_cls()
    mixin._auto_resized_tables = set()
    table = TableSpy()

    mixin._resize_table_once(table, "mov_estoque")

    assert table.resize_calls == 1
    assert table.resize_precision_seen == mixin_cls._TABLE_RESIZE_CONTENTS_PRECISION
    assert table.signals_blocked_during_resize is True
    assert table.header.precision == 10_000
    assert table.header.block_calls == [True, False]

    mixin._resize_table_once(table, "mov_estoque")
    assert table.resize_calls == 1


def test_polars_table_model_reuses_row_dict_until_dataframe_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model_cls = _load_table_model(monkeypatch)
    model = model_cls(pl.DataFrame({"id_agrupado": ["A"], "valor": [1]}))

    first = model.row_as_dict(0)
    second = model.row_as_dict(0)

    assert second is first

    model.set_dataframe(pl.DataFrame({"id_agrupado": ["B"], "valor": [2]}))
    refreshed = model.row_as_dict(0)

    assert refreshed is not first
    assert refreshed == {"id_agrupado": "B", "valor": 2}


def test_polars_table_model_row_dict_cache_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model_cls = _load_table_model(monkeypatch)
    monkeypatch.setattr(model_cls, "_ROW_DICT_CACHE_MAX", 2)
    model = model_cls(pl.DataFrame({"id_agrupado": ["A", "B", "C"], "valor": [1, 2, 3]}))

    first = model.row_as_dict(0)
    model.row_as_dict(1)
    model.row_as_dict(2)

    assert model.row_as_dict(0) == first
    assert model.row_as_dict(0) is not first
