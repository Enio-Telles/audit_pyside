from __future__ import annotations

import polars as pl
from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QPushButton,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from interface_grafica.models.table_model import PolarsTableModel


class DetachedTableWindow(QMainWindow):
    closed = Signal(str)

    def __init__(
        self,
        titulo: str,
        contexto: str,
        table_model: PolarsTableModel,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._contexto = contexto
        self._source_model = table_model
        self._table_model = table_model.clone_configuration(pl.DataFrame())
        self.setAttribute(Qt.WA_DeleteOnClose, True)
        self.setWindowTitle(titulo)
        self.resize(1200, 720)

        central = QWidget(self)
        layout = QVBoxLayout(central)
        layout.setContentsMargins(8, 8, 8, 8)

        self.lbl_titulo = QLabel(titulo)
        self.lbl_titulo.setStyleSheet(
            "QLabel { font-weight: bold; color: #f8fafc; background: #1f2a44; "
            "border: 1px solid #334155; border-radius: 4px; padding: 6px 10px; }"
        )
        layout.addWidget(self.lbl_titulo)

        filtros = QHBoxLayout()
        self.filter_column = QComboBox(self)
        self.filter_column.setMinimumWidth(180)
        self.filter_column.addItem("Todas")
        self.filter_text = QLineEdit(self)
        self.filter_text.setPlaceholderText("Filtrar na janela destacada...")
        self.profile_combo = QComboBox(self)
        self.profile_combo.setMinimumWidth(140)
        self.btn_apply_profile = QPushButton("Perfil", self)
        self.btn_save_profile = QPushButton("Salvar perfil", self)
        self.btn_columns = QPushButton("Colunas", self)
        self.btn_apply_filter = QPushButton("Aplicar filtros", self)
        self.btn_clear_filter = QPushButton("Limpar filtros", self)
        filtros.addWidget(self.filter_column)
        filtros.addWidget(self.filter_text, 1)
        filtros.addWidget(self.profile_combo)
        filtros.addWidget(self.btn_apply_profile)
        filtros.addWidget(self.btn_save_profile)
        filtros.addWidget(self.btn_columns)
        filtros.addWidget(self.btn_apply_filter)
        filtros.addWidget(self.btn_clear_filter)
        layout.addLayout(filtros)

        self.lbl_status = QLabel("Filtros ativos: nenhum", self)
        self.lbl_status.setStyleSheet(
            "QLabel { padding: 4px 8px; color: #dbeafe; background: #0f1b33; border: 1px solid #334155; border-radius: 4px; }"
        )
        layout.addWidget(self.lbl_status)

        self.table = QTableView(self)
        self.table.setModel(self._table_model)
        self.table.setSelectionBehavior(QAbstractItemView.SelectItems)
        self.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table.setAlternatingRowColors(True)
        self.table.setSortingEnabled(True)
        self.table.setWordWrap(True)
        self.table.verticalHeader().setDefaultSectionSize(40)
        self.table.horizontalHeader().setMinimumSectionSize(40)
        self.table.horizontalHeader().setDefaultSectionSize(150)
        self.table.horizontalHeader().setMaximumSectionSize(420)
        self.table.horizontalHeader().setSectionsMovable(True)
        self.table.horizontalHeader().setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        layout.addWidget(self.table, 1)

        self.setCentralWidget(central)

        self.btn_apply_filter.clicked.connect(self.apply_filters)
        self.btn_clear_filter.clicked.connect(self.clear_filters)
        self.filter_text.returnPressed.connect(self.apply_filters)
        self.filter_column.currentIndexChanged.connect(lambda _i: self.apply_filters())

        self._on_source_model_reset = self._refresh_from_source
        self._on_source_layout_changed = self._refresh_from_source

        def _on_source_data_changed_handler(*_args):
            self._refresh_from_source()

        self._on_source_data_changed = _on_source_data_changed_handler
        self._source_model.modelReset.connect(self._on_source_model_reset)
        self._source_model.layoutChanged.connect(self._on_source_layout_changed)
        self._source_model.dataChanged.connect(self._on_source_data_changed)

        self._refresh_from_source()

    @property
    def contexto(self) -> str:
        return self._contexto

    @property
    def table_model(self) -> PolarsTableModel:
        return self._table_model

    def _expr_texto_coluna(self, df: pl.DataFrame, coluna: str) -> pl.Expr:
        dtype = df.schema.get(coluna)
        if dtype is not None and dtype.base_type() == pl.List:
            return (
                pl.col(coluna)
                .cast(pl.List(pl.Utf8), strict=False)
                .list.join(" | ")
                .fill_null("")
                .str.to_lowercase()
            )
        return (
            pl.col(coluna).cast(pl.Utf8, strict=False).fill_null("").str.to_lowercase()
        )

    def _refresh_from_source(self) -> None:
        if not hasattr(self, "filter_column") or self.filter_column is None:
            return
        df = self._source_model.get_dataframe()
        col_atual = self.filter_column.currentText()
        self.filter_column.blockSignals(True)
        self.filter_column.clear()
        self.filter_column.addItem("Todas")
        self.filter_column.addItems(df.columns)
        idx = self.filter_column.findText(col_atual)
        if idx >= 0:
            self.filter_column.setCurrentIndex(idx)
        self.filter_column.blockSignals(False)
        self.apply_filters()

    def apply_filters(self) -> None:
        df = self._source_model.get_dataframe()
        termo = self.filter_text.text().strip().lower()
        coluna = self.filter_column.currentText().strip()

        if not df.is_empty() and termo:
            if coluna and coluna != "Todas" and coluna in df.columns:
                df = df.filter(
                    self._expr_texto_coluna(df, coluna).str.contains(
                        termo, literal=True
                    )
                )
            else:
                exprs = [
                    self._expr_texto_coluna(df, col).str.contains(termo, literal=True)
                    for col in df.columns
                ]
                if exprs:
                    df = df.filter(pl.any_horizontal(exprs))

        self._table_model.set_dataframe(df)
        self.lbl_status.setText(
            "Filtros ativos: nenhum"
            if not termo
            else f"Filtros ativos: {coluna or 'Todas'} contem '{termo}'"
        )
        self.table.resizeColumnsToContents()

    def clear_filters(self) -> None:
        self.filter_text.clear()
        self.filter_column.setCurrentIndex(0)
        self.apply_filters()

    def closeEvent(self, event) -> None:
        try:
            self._source_model.modelReset.disconnect(self._on_source_model_reset)
        except Exception:
            pass
        try:
            self._source_model.layoutChanged.disconnect(self._on_source_layout_changed)
        except Exception:
            pass
        try:
            self._source_model.dataChanged.disconnect(self._on_source_data_changed)
        except Exception:
            pass
        self.closed.emit(self._contexto)
        super().closeEvent(event)
