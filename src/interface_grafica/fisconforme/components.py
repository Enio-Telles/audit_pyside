from __future__ import annotations

from typing import Iterable, List, Sequence, Tuple

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QFontMetrics
from PySide6.QtWidgets import (
    QAbstractItemView,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QSizePolicy,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from .theme import ADAPTIVE_TWO_COLUMN_BREAKPOINT, COLORS, SPACING


class PageHeader(QWidget):
    """Cabeçalho fixo exibido acima da área rolável."""

    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)

        self.title_label = QLabel()
        self.title_label.setObjectName("PageTitle")
        layout.addWidget(self.title_label)

        self.subtitle_label = QLabel()
        self.subtitle_label.setObjectName("PageSubtitle")
        self.subtitle_label.setWordWrap(True)
        layout.addWidget(self.subtitle_label)

    def set_content(self, title: str, subtitle: str):
        self.title_label.setText(title)
        self.subtitle_label.setText(subtitle)


class SectionCard(QGroupBox):
    """Card de seção padronizado."""

    def __init__(self, title: str, parent: QWidget | None = None):
        super().__init__(title, parent)


class ElidedLabel(QLabel):
    """Label com elisão automática e tooltip com o texto completo."""

    def __init__(self, text: str = "", parent: QWidget | None = None):
        super().__init__(parent)
        self._full_text = text
        self.setText(text)

    def setText(self, text: str):  # type: ignore[override]
        self._full_text = text
        self._update_elided_text()

    def full_text(self) -> str:
        return self._full_text

    def resizeEvent(self, event):  # noqa: N802
        super().resizeEvent(event)
        self._update_elided_text()

    def _update_elided_text(self):
        metrics = QFontMetrics(self.font())
        available = max(32, self.contentsRect().width())
        elided = metrics.elidedText(self._full_text, Qt.ElideRight, available)
        super().setText(elided)
        self.setToolTip(self._full_text if elided != self._full_text else "")


class StatusBanner(QFrame):
    """Faixa de feedback visual padronizada."""

    LEVELS = {
        "info": ("Informação", COLORS["secondary"]),
        "success": ("Sucesso", COLORS["success"]),
        "warning": ("Atenção", COLORS["warning"]),
        "danger": ("Erro", COLORS["danger"]),
    }

    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self.setObjectName("StatusBanner")
        self.setVisible(False)

        layout = QHBoxLayout(self)
        layout.setContentsMargins(12, 10, 12, 10)
        layout.setSpacing(10)

        self.title = QLabel()
        self.title.setStyleSheet("font-weight: 700;")
        layout.addWidget(self.title)

        self.message = QLabel()
        self.message.setWordWrap(True)
        self.message.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)
        layout.addWidget(self.message, 1)

    def set_status(self, level: str, message: str):
        if not message:
            self.clear()
            return
        title, color = self.LEVELS.get(level, self.LEVELS["info"])
        self.title.setText(title)
        self.title.setProperty("statusRole", level)
        self.message.setText(message)
        self.message.setProperty("statusRole", level)
        self.setStyleSheet(
            f"QFrame#StatusBanner {{ background-color: {color}15; border: 1px solid {color}55; }}"
        )
        self.title.style().unpolish(self.title)
        self.title.style().polish(self.title)
        self.message.style().unpolish(self.message)
        self.message.style().polish(self.message)
        self.setVisible(True)

    def clear(self):
        self.setVisible(False)
        self.message.clear()
        self.title.clear()


class MetadataGrid(QFrame):
    """Grid compacto para resumo de dados-chave."""

    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self.setObjectName("MetadataGrid")
        self._layout = QGridLayout(self)
        self._layout.setContentsMargins(12, 12, 12, 12)
        self._layout.setHorizontalSpacing(18)
        self._layout.setVerticalSpacing(10)
        self._rows: List[QWidget] = []

    def set_items(self, items: Sequence[Tuple[str, str]]):
        while self._layout.count():
            item = self._layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.deleteLater()

        for index, (label_text, value_text) in enumerate(items):
            key = QLabel(label_text)
            key.setObjectName("MetadataKey")
            value = QLabel(value_text or "—")
            value.setObjectName("MetadataValue")
            value.setWordWrap(True)
            row = index // 2
            col = (index % 2) * 2
            self._layout.addWidget(key, row * 2, col)
            self._layout.addWidget(value, row * 2 + 1, col, 1, 2)


class DataTable(QTableView):
    """Tabela base para visualização de dados operacionais."""

    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self.setAlternatingRowColors(True)
        self.setSortingEnabled(True)
        self.setWordWrap(False)
        self.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.setSelectionMode(QAbstractItemView.SingleSelection)
        self.setShowGrid(False)
        self.setCornerButtonEnabled(False)
        self.verticalHeader().setVisible(False)
        self.horizontalHeader().setStretchLastSection(False)
        self.setHorizontalScrollMode(QAbstractItemView.ScrollPerPixel)
        self.setVerticalScrollMode(QAbstractItemView.ScrollPerPixel)
        self.setEditTriggers(QAbstractItemView.NoEditTriggers)


class AdaptiveFieldGrid(QWidget):
    """Grid de campos que alterna entre 1 e 2 colunas conforme largura disponível."""

    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self._items: List[QWidget] = []
        self._columns = 0
        self._layout = QGridLayout(self)
        self._layout.setContentsMargins(0, 0, 0, 0)
        self._layout.setHorizontalSpacing(SPACING["sm"])
        self._layout.setVerticalSpacing(SPACING["sm"])

    def add_field(self, label_text: str, field_widget: QWidget):
        wrapper = QWidget()
        layout = QVBoxLayout(wrapper)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        label = QLabel(label_text)
        label.setStyleSheet("font-weight: 700;")
        layout.addWidget(label)
        layout.addWidget(field_widget)
        self._items.append(wrapper)
        self._rebuild_layout()

    def resizeEvent(self, event):  # noqa: N802
        super().resizeEvent(event)
        self._rebuild_layout()

    def _rebuild_layout(self):
        columns = 2 if self.width() >= ADAPTIVE_TWO_COLUMN_BREAKPOINT else 1
        if columns == self._columns and self._layout.count() == len(self._items):
            return

        while self._layout.count():
            item = self._layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.setParent(self)

        for index, widget in enumerate(self._items):
            row = index // columns
            col = index % columns
            self._layout.addWidget(widget, row, col)

        self._columns = columns


class ActionFooter(QFrame):
    """Rodapé fixo com CTA principal do wizard."""

    previous_clicked = Signal()
    primary_clicked = Signal()

    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self.setObjectName("FooterBar")
        layout = QHBoxLayout(self)
        layout.setContentsMargins(18, 10, 18, 10)
        layout.setSpacing(10)

        self.context_label = QLabel()
        self.context_label.setStyleSheet(f"color: {COLORS['muted']}; font-size: 11px;")
        layout.addWidget(self.context_label)

        layout.addStretch()

        self.previous_button = QPushButton("Anterior")
        self.previous_button.clicked.connect(self.previous_clicked.emit)
        layout.addWidget(self.previous_button)

        self.primary_button = QPushButton("Próximo")
        self.primary_button.setObjectName("PrimaryButton")
        self.primary_button.clicked.connect(self.primary_clicked.emit)
        layout.addWidget(self.primary_button)

    def configure(
        self,
        primary_label: str,
        context_text: str,
        primary_object_name: str = "PrimaryButton",
        show_previous: bool = True,
        primary_enabled: bool = True,
    ):
        self.context_label.setText(context_text)
        self.previous_button.setVisible(show_previous)
        self.primary_button.setText(primary_label)
        self.primary_button.setEnabled(primary_enabled)
        self.primary_button.setObjectName(primary_object_name)
        self.primary_button.style().unpolish(self.primary_button)
        self.primary_button.style().polish(self.primary_button)
