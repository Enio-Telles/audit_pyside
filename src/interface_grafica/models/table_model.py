from __future__ import annotations

from typing import Any, Callable

import polars as pl
from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt
from PySide6.QtGui import QBrush, QColor, QFont

from utilitarios.text import display_cell


class PolarsTableModel(QAbstractTableModel):
    def __init__(
        self,
        df: pl.DataFrame | None = None,
        checkable: bool = False,
        editable_columns: set[str] | None = None,
        foreground_resolver: Callable[[dict[str, Any], str], QColor | str | None] | None = None,
        background_resolver: Callable[[dict[str, Any], str], QColor | str | None] | None = None,
        font_resolver: Callable[[dict[str, Any], str], QFont | None] | None = None,
    ) -> None:
        super().__init__()
        self._df = df if df is not None else pl.DataFrame()
        self._checkable = checkable
        self._checked_rows: set[int] = set()
        self._checked_keys: set[tuple[str, ...]] = set()
        self._editable_columns: set[str] = set(editable_columns or set())
        self._foreground_resolver = foreground_resolver
        self._background_resolver = background_resolver
        self._font_resolver = font_resolver
        self._last_sort_column: str | None = None
        self._last_sort_order: Qt.SortOrder | None = None

    def set_dataframe(self, df: pl.DataFrame) -> None:
        if self._checkable:
            self._refresh_checked_keys_from_rows()
        self.beginResetModel()
        self._df = df
        self._rebuild_checked_rows_from_keys()
        self.endResetModel()

    def set_editable_columns(self, columns: set[str] | None) -> None:
        self._editable_columns = set(columns or set())

    def set_foreground_resolver(
        self,
        resolver: Callable[[dict[str, Any], str], QColor | str | None] | None,
    ) -> None:
        self.beginResetModel()
        self._foreground_resolver = resolver
        self.endResetModel()

    def set_background_resolver(
        self,
        resolver: Callable[[dict[str, Any], str], QColor | str | None] | None,
    ) -> None:
        self.beginResetModel()
        self._background_resolver = resolver
        self.endResetModel()

    def set_font_resolver(
        self,
        resolver: Callable[[dict[str, Any], str], QFont | None] | None,
    ) -> None:
        self.beginResetModel()
        self._font_resolver = resolver
        self.endResetModel()

    @property
    def dataframe(self) -> pl.DataFrame:
        return self._df

    def get_dataframe(self) -> pl.DataFrame:
        return self._df

    @property
    def checkable(self) -> bool:
        return self._checkable

    @property
    def editable_columns(self) -> set[str]:
        return set(self._editable_columns)

    @property
    def foreground_resolver(self):
        return self._foreground_resolver

    @property
    def background_resolver(self):
        return self._background_resolver

    @property
    def font_resolver(self):
        return self._font_resolver

    def clone_configuration(self, df: pl.DataFrame | None = None) -> "PolarsTableModel":
        return PolarsTableModel(
            df=df if df is not None else self._df.clone(),
            checkable=self._checkable,
            editable_columns=set(self._editable_columns),
            foreground_resolver=self._foreground_resolver,
            background_resolver=self._background_resolver,
            font_resolver=self._font_resolver,
        )

    def _key_candidates(self) -> list[tuple[str, ...]]:
        return [
            ("__row_id__",),
            ("id_produtos", "unid"),
            ("id_agrupado", "unid"),
            ("id_agregado", "ano", "mes"),
            ("id_agregado", "ano"),
            ("id_agrupado",),
            ("id_agregado",),
            ("id_produtos",),
            ("id_descricao",),
            ("descricao_normalizada",),
        ]

    def _active_key_columns(self) -> tuple[str, ...]:
        cols = set(self._df.columns)
        for candidate in self._key_candidates():
            if all(col in cols for col in candidate):
                return candidate
        return tuple()

    def _row_key(self, row: int) -> tuple[str, ...] | None:
        key_cols = self._active_key_columns()
        if not key_cols or row < 0 or row >= self._df.height:
            return None
        values = []
        for col in key_cols:
            try:
                value = self._df[row, col]
            except Exception:
                return None
            values.append("" if value is None else str(value))
        return tuple(values)

    def _refresh_checked_keys_from_rows(self) -> None:
        if not self._checkable:
            return
        keys: set[tuple[str, ...]] = set()
        for row in sorted(self._checked_rows):
            key = self._row_key(row)
            if key is not None:
                keys.add(key)
        self._checked_keys = keys

    def _rebuild_checked_rows_from_keys(self) -> None:
        if not self._checkable:
            self._checked_rows.clear()
            return
        if not self._checked_keys:
            self._checked_rows.clear()
            return
        rows: set[int] = set()
        for row in range(self._df.height):
            key = self._row_key(row)
            if key is not None and key in self._checked_keys:
                rows.add(row)
        self._checked_rows = rows

    def rowCount(self, parent: QModelIndex | None = None) -> int:
        if parent and parent.isValid():
            return 0
        return self._df.height

    def columnCount(self, parent: QModelIndex | None = None) -> int:
        if parent and parent.isValid():
            return 0
        count = self._df.width
        if self._checkable:
            count += 1
        return count

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:
        if not index.isValid():
            return None

        row = index.row()
        col = index.column()

        if self._checkable:
            if col == 0:
                if role == Qt.CheckStateRole:
                    return Qt.Checked if row in self._checked_rows else Qt.Unchecked
                if role == Qt.DisplayRole:
                    return ""
                return None
            col -= 1  # Offset for the checkbox column

        if role == Qt.ForegroundRole and self._foreground_resolver is not None:
            try:
                row_data = self.row_as_dict(row)
                color = self._foreground_resolver(row_data, self._df.columns[col])
                if color is None:
                    return None
                if isinstance(color, QColor):
                    return QBrush(color)
                return QBrush(QColor(str(color)))
            except Exception:
                return None

        if role == Qt.BackgroundRole and self._background_resolver is not None:
            try:
                row_data = self.row_as_dict(row)
                color = self._background_resolver(row_data, self._df.columns[col])
                if color is None:
                    return None
                if isinstance(color, QColor):
                    return QBrush(color)
                return QBrush(QColor(str(color)))
            except Exception:
                return None

        if role == Qt.FontRole and self._font_resolver is not None:
            try:
                row_data = self.row_as_dict(row)
                return self._font_resolver(row_data, self._df.columns[col])
            except Exception:
                return None

        if role not in (Qt.DisplayRole, Qt.ToolTipRole):
            return None

        value = self._df[row, col]
        return display_cell(value, self._df.columns[col])

    def setData(self, index: QModelIndex, value: Any, role: int = Qt.EditRole) -> bool:
        if self._checkable and index.column() == 0 and role == Qt.CheckStateRole:
            row = index.row()
            # Handle both enum values and integers
            if isinstance(value, Qt.CheckState):
                is_checked = (value == Qt.CheckState.Checked)
            else:
                is_checked = (value == Qt.Checked or value == 2) # 2 is usually Qt.Checked
                
            if is_checked:
                self._checked_rows.add(row)
                key = self._row_key(row)
                if key is not None:
                    self._checked_keys.add(key)
            else:
                self._checked_rows.discard(row)
                key = self._row_key(row)
                if key is not None:
                    self._checked_keys.discard(key)
            
            self.dataChanged.emit(index, index, [Qt.CheckStateRole])
            return True

        if role == Qt.EditRole and index.isValid():
            row = index.row()
            col = index.column()

            if self._checkable:
                if col == 0:
                    return False
                col -= 1

            if col < 0 or col >= self._df.width or row < 0 or row >= self._df.height:
                return False

            col_name = self._df.columns[col]
            if col_name not in self._editable_columns:
                return False

            dtype = self._df.schema[col_name]
            raw = value if value is not None else ""
            if not isinstance(raw, str):
                raw = str(raw)
            raw = raw.strip()

            try:
                if dtype == pl.Float64 or dtype == pl.Float32:
                    if raw == "":
                        parsed = None
                    else:
                        raw = raw.replace(',', '.')
                        if '/' in raw:
                            numerador, denominador = raw.split('/', 1)
                            parsed = float(numerador) / float(denominador)
                        else:
                            parsed = float(raw)
                elif dtype in (pl.Int64, pl.Int32, pl.Int16, pl.Int8, pl.UInt64, pl.UInt32, pl.UInt16, pl.UInt8):
                    parsed = None if raw == "" else int(float(raw))
                else:
                    parsed = raw
            except Exception:
                return False

            values = self._df.get_column(col_name).to_list()
            values[row] = parsed
            self._df = self._df.with_columns(pl.Series(col_name, values, dtype=dtype))
            self.dataChanged.emit(index, index, [Qt.DisplayRole, Qt.EditRole])
            return True
        return False

    def flags(self, index: QModelIndex) -> Qt.ItemFlags:
        f = super().flags(index)
        if self._checkable and index.column() == 0:
            # Explicitly set necessary flags for interactivity
            return Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsUserCheckable
        col = index.column() - (1 if self._checkable else 0)
        if 0 <= col < self._df.width and self._df.columns[col] in self._editable_columns:
            return f | Qt.ItemIsEditable
        return f

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole) -> Any:
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal:
            col = section
            if self._checkable:
                if col == 0:
                    return "Visto"
                col -= 1
            return self._df.columns[col] if col < len(self._df.columns) else None
        return str(section + 1)

    def sort(self, column: int, order: Qt.SortOrder = Qt.AscendingOrder) -> None:
        if self._df.is_empty():
            return

        data_col = column - (1 if self._checkable else 0)
        if data_col < 0 or data_col >= self._df.width:
            return

        col_name = self._df.columns[data_col]
        descending = order == Qt.DescendingOrder

        try:
            self.layoutAboutToBeChanged.emit()
            self._refresh_checked_keys_from_rows()
            self._df = self._df.sort(col_name, descending=descending, nulls_last=True)
            self._rebuild_checked_rows_from_keys()
            self._last_sort_column = col_name
            self._last_sort_order = order
            self.layoutChanged.emit()
        except Exception:
            pass

    def row_as_dict(self, row: int) -> dict[str, Any]:
        if row < 0 or row >= self._df.height:
            return {}
        return self._df.row(row, named=True)

    def get_checked_rows(self) -> list[dict[str, Any]]:
        results = []
        for r in sorted(list(self._checked_rows)):
            if r < self._df.height:
                results.append(self.row_as_dict(r))
        return results

    def set_checked_keys(self, keys: set[tuple[str, ...]] | list[tuple[str, ...]]) -> None:
        self._checked_keys = set(keys or set())
        self._rebuild_checked_rows_from_keys()
        self.layoutChanged.emit()

    def clear_checked(self) -> None:
        self._checked_rows.clear()
        self._checked_keys.clear()
        self.layoutChanged.emit()
