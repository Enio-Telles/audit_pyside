from __future__ import annotations

from PySide6.QtWidgets import QStyledItemDelegate, QDoubleSpinBox


class FloatDelegate(QStyledItemDelegate):
    def createEditor(self, parent, option, index):
        editor = QDoubleSpinBox(parent)
        editor.setDecimals(6)
        editor.setMinimum(-999999999.0)
        editor.setMaximum(999999999.0)
        return editor
