from __future__ import annotations

from dataclasses import replace
from typing import Any, List

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt
from PySide6.QtGui import QColor, QBrush

from .state import CNPJRecord, ProcessingResult
from .theme import COLORS


class CNPJTableModel(QAbstractTableModel):
    HEADERS = ["#", "CNPJ", "Razão Social", "Município/UF", "Situação", "Origem", "Status"]

    def __init__(self):
        super().__init__()
        self._records: List[CNPJRecord] = []

    def rowCount(self, parent=QModelIndex()):  # noqa: N802
        return 0 if parent.isValid() else len(self._records)

    def columnCount(self, parent=QModelIndex()):  # noqa: N802
        return 0 if parent.isValid() else len(self.HEADERS)

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None
        record = self._records[index.row()]
        values = [
            str(record.seq),
            record.cnpj,
            record.razao_social or "—",
            record.municipio_uf or "—",
            record.situacao or "—",
            record.origem or "—",
            record.status,
        ]

        if role in (Qt.DisplayRole, Qt.EditRole):
            return values[index.column()]
        if role == Qt.ToolTipRole:
            return record.tooltip or "\n".join(
                [
                    f"CNPJ: {record.cnpj}",
                    f"Razão Social: {record.razao_social or '—'}",
                    f"Município/UF: {record.municipio_uf or '—'}",
                    f"Situação: {record.situacao or '—'}",
                    f"Origem: {record.origem or '—'}",
                    f"Status: {record.status}",
                ]
            )
        if role == Qt.TextAlignmentRole and index.column() in (0, 6):
            return Qt.AlignCenter
        if role == Qt.ForegroundRole and index.column() == 6:
            if not record.valido:
                return QBrush(QColor(COLORS["danger"]))
            if record.carregando:
                return QBrush(QColor(COLORS["secondary"]))
            if record.erro:
                return QBrush(QColor(COLORS["warning"]))
            return QBrush(QColor(COLORS["success"]))
        if role == Qt.UserRole:
            return record
        return None

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal:
            return self.HEADERS[section]
        return str(section + 1)

    def flags(self, index):
        if not index.isValid():
            return Qt.NoItemFlags
        return Qt.ItemIsEnabled | Qt.ItemIsSelectable

    def sort(self, column, order=Qt.AscendingOrder):
        key_map = {
            0: lambda rec: rec.seq,
            1: lambda rec: rec.cnpj,
            2: lambda rec: rec.razao_social,
            3: lambda rec: rec.municipio_uf,
            4: lambda rec: rec.situacao,
            5: lambda rec: rec.origem,
            6: lambda rec: rec.status,
        }
        reverse = order == Qt.DescendingOrder
        self.layoutAboutToBeChanged.emit()
        self._records.sort(key=key_map.get(column, key_map[0]), reverse=reverse)
        self.layoutChanged.emit()

    def records(self) -> List[CNPJRecord]:
        return [replace(record) for record in self._records]

    def set_records(self, records: List[CNPJRecord]):
        self.beginResetModel()
        self._records = [replace(record) for record in records]
        self._renumerar()
        self.endResetModel()

    def add_record(self, record: CNPJRecord):
        self.beginInsertRows(QModelIndex(), len(self._records), len(self._records))
        self._records.append(replace(record))
        self.endInsertRows()
        self._renumerar()

    def remove_rows(self, rows: List[int]):
        for row in sorted(set(rows), reverse=True):
            if 0 <= row < len(self._records):
                self.beginRemoveRows(QModelIndex(), row, row)
                self._records.pop(row)
                self.endRemoveRows()
        self._renumerar()

    def clear(self):
        self.beginResetModel()
        self._records.clear()
        self.endResetModel()

    def update_record(self, cnpj: str, **changes: Any):
        for row, record in enumerate(self._records):
            if record.cnpj != cnpj:
                continue
            for key, value in changes.items():
                setattr(record, key, value)
            top_left = self.index(row, 0)
            bottom_right = self.index(row, self.columnCount() - 1)
            self.dataChanged.emit(top_left, bottom_right, [Qt.DisplayRole, Qt.ToolTipRole, Qt.ForegroundRole])
            return

    def has_cnpj(self, cnpj: str) -> bool:
        return any(record.cnpj == cnpj for record in self._records)

    def _renumerar(self):
        for index, record in enumerate(self._records, start=1):
            record.seq = index
        if self._records:
            top_left = self.index(0, 0)
            bottom_right = self.index(len(self._records) - 1, self.columnCount() - 1)
            self.dataChanged.emit(top_left, bottom_right, [Qt.DisplayRole])


class ResultsTableModel(QAbstractTableModel):
    HEADERS = ["CNPJ", "Status", "Detalhe"]

    def __init__(self):
        super().__init__()
        self._results: List[ProcessingResult] = []

    def rowCount(self, parent=QModelIndex()):  # noqa: N802
        return 0 if parent.isValid() else len(self._results)

    def columnCount(self, parent=QModelIndex()):  # noqa: N802
        return 0 if parent.isValid() else len(self.HEADERS)

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None
        result = self._results[index.row()]
        values = [result.cnpj, "Sucesso" if result.sucesso else "Falha", result.detalhe]

        if role in (Qt.DisplayRole, Qt.EditRole):
            return values[index.column()]
        if role == Qt.ToolTipRole:
            return result.detalhe if index.column() == 2 else values[index.column()]
        if role == Qt.ForegroundRole and index.column() == 1:
            color = COLORS["success"] if result.sucesso else COLORS["danger"]
            return QBrush(QColor(color))
        if role == Qt.TextAlignmentRole and index.column() == 1:
            return Qt.AlignCenter
        return None

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal:
            return self.HEADERS[section]
        return str(section + 1)

    def clear(self):
        self.beginResetModel()
        self._results.clear()
        self.endResetModel()

    def add_result(self, result: ProcessingResult):
        row = len(self._results)
        self.beginInsertRows(QModelIndex(), row, row)
        self._results.append(result)
        self.endInsertRows()

    def results(self) -> List[ProcessingResult]:
        return list(self._results)

