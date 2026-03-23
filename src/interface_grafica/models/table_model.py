from __future__ import annotations

from typing import Any

import polars as pl
from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

from utilitarios.text import display_cell


class PolarsTableModel(QAbstractTableModel):
    def __init__(
        self,
        df: pl.DataFrame | None = None,
        checkable: bool = False,
        editable_columns: set[str] | None = None,
    ) -> None:
        super().__init__()
        self._df = df if df is not None else pl.DataFrame()
        self._checkable = checkable
        self._checked_rows: set[int] = set()
        self._editable_columns: set[str] = set(editable_columns or set())

    def set_dataframe(self, df: pl.DataFrame) -> None:
        self.beginResetModel()
        self._df = df
        self._checked_rows.clear()
        self.endResetModel()

    def set_editable_columns(self, columns: set[str] | None) -> None:
        self._editable_columns = set(columns or set())

    @property
    def dataframe(self) -> pl.DataFrame:
        return self._df

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

        if role not in (Qt.DisplayRole, Qt.ToolTipRole):
            return None

        value = self._df[row, col]
        return display_cell(value)

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
            else:
                self._checked_rows.discard(row)
            
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

    def clear_checked(self) -> None:
        self._checked_rows.clear()
        self.layoutChanged.emit()
