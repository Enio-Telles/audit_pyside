from __future__ import annotations

from pathlib import Path

import polars as pl
from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QPushButton,
    QTableView,
    QVBoxLayout,
)

from interface_grafica.models.table_model import PolarsTableModel


class ColumnSelectorDialog(QDialog):
    def __init__(self, columns: list[str], visible_columns: list[str], parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Selecionar colunas visiveis")
        self.resize(420, 520)

        self.filter_edit = QLineEdit()
        self.filter_edit.setPlaceholderText("Buscar coluna...")
        self.filter_edit.textChanged.connect(self._apply_filter)

        self.list_widget = QListWidget()
        self.list_widget.setSelectionMode(QAbstractItemView.SingleSelection)
        self.list_widget.setDragDropMode(QAbstractItemView.InternalMove)
        self.list_widget.setDefaultDropAction(Qt.MoveAction)
        visible = set(visible_columns)
        for col in columns:
            item = QListWidgetItem(col)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Checked if col in visible else Qt.Unchecked)
            self.list_widget.addItem(item)

        top_actions = QHBoxLayout()
        self.btn_select_all = QPushButton("Marcar todas")
        self.btn_clear_all = QPushButton("Limpar")
        self.btn_move_up = QPushButton("Subir")
        self.btn_move_down = QPushButton("Descer")
        self.btn_select_all.clicked.connect(self._select_all)
        self.btn_clear_all.clicked.connect(self._clear_all)
        self.btn_move_up.clicked.connect(self._move_up)
        self.btn_move_down.clicked.connect(self._move_down)
        top_actions.addWidget(self.btn_select_all)
        top_actions.addWidget(self.btn_clear_all)
        top_actions.addWidget(self.btn_move_up)
        top_actions.addWidget(self.btn_move_down)
        top_actions.addStretch()

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btn_ok = buttons.button(QDialogButtonBox.Ok)
        if btn_ok is not None:
            btn_ok.setText("Definir")
        btn_cancel = buttons.button(QDialogButtonBox.Cancel)
        if btn_cancel is not None:
            btn_cancel.setText("Cancelar")
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)

        layout = QVBoxLayout(self)
        layout.addWidget(self.filter_edit)
        layout.addLayout(top_actions)
        layout.addWidget(self.list_widget)
        layout.addWidget(buttons)

    def selected_columns(self) -> list[str]:
        selected = []
        for idx in range(self.list_widget.count()):
            item = self.list_widget.item(idx)
            if item.checkState() == Qt.Checked:
                selected.append(item.text())
        return selected

    def column_order(self) -> list[str]:
        return [self.list_widget.item(idx).text() for idx in range(self.list_widget.count())]

    def _select_all(self) -> None:
        for idx in range(self.list_widget.count()):
            self.list_widget.item(idx).setCheckState(Qt.Checked)

    def _clear_all(self) -> None:
        for idx in range(self.list_widget.count()):
            self.list_widget.item(idx).setCheckState(Qt.Unchecked)

    def _apply_filter(self, text: str) -> None:
        filtro = (text or "").strip().lower()
        for idx in range(self.list_widget.count()):
            item = self.list_widget.item(idx)
            nome = item.text().lower()
            item.setHidden(bool(filtro) and filtro not in nome)

    def _move_up(self) -> None:
        row = self.list_widget.currentRow()
        if row <= 0:
            return
        item = self.list_widget.takeItem(row)
        self.list_widget.insertItem(row - 1, item)
        self.list_widget.setCurrentRow(row - 1)

    def _move_down(self) -> None:
        row = self.list_widget.currentRow()
        if row < 0 or row >= self.list_widget.count() - 1:
            return
        item = self.list_widget.takeItem(row)
        self.list_widget.insertItem(row + 1, item)
        self.list_widget.setCurrentRow(row + 1)


class DialogoSelecaoConsultas(QDialog):
    """Dialogo para selecionar quais consultas SQL executar."""

    def __init__(self, consultas: list[str], parent=None, pre_selecionados: list[str] | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Selecionar consultas SQL para execucao")
        self.resize(520, 480)
        self._consultas = consultas

        layout = QVBoxLayout(self)

        layout.addWidget(QLabel("Marque as consultas SQL que deseja executar:"))

        todos_marcados = pre_selecionados is None or len(pre_selecionados) == len(consultas)

        self.chk_todos = QCheckBox("Selecionar todas")
        self.chk_todos.setChecked(todos_marcados)
        self.chk_todos.stateChanged.connect(self._alternar_todos)
        layout.addWidget(self.chk_todos)

        self.lista = QListWidget()
        pre_set = set(pre_selecionados) if pre_selecionados is not None else None

        for sql_id in consultas:
            path_parts = sql_id.split("/")
            rotulo = Path(sql_id).stem
            if len(path_parts) > 1:
                rotulo = f"{'/'.join(path_parts[:-1])}/{Path(sql_id).stem}"
            item = QListWidgetItem(rotulo)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            estado = Qt.Checked if (pre_set is None or sql_id in pre_set) else Qt.Unchecked
            item.setCheckState(estado)
            item.setToolTip(sql_id)
            item.setData(Qt.UserRole, sql_id)
            self.lista.addItem(item)
        layout.addWidget(self.lista, 1)

        botoes = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        botoes.accepted.connect(self.accept)
        botoes.rejected.connect(self.reject)
        layout.addWidget(botoes)

    def _alternar_todos(self, estado: int) -> None:
        marcado = Qt.Checked if estado == Qt.Checked.value else Qt.Unchecked
        for idx in range(self.lista.count()):
            self.lista.item(idx).setCheckState(marcado)

    def consultas_selecionadas(self) -> list[str]:
        selecionadas = []
        for idx in range(self.lista.count()):
            item = self.lista.item(idx)
            if item.checkState() == Qt.Checked:
                selecionadas.append(item.data(Qt.UserRole))
        return selecionadas


class DialogoSelecaoTabelas(QDialog):
    """Dialogo para selecionar quais tabelas gerar."""

    def __init__(self, tabelas: list[dict[str, str]], parent=None, pre_selecionados: list[str] | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Selecionar tabelas a gerar")
        self.resize(520, 380)
        self._tabelas = tabelas

        layout = QVBoxLayout(self)
        layout.addWidget(
            QLabel(
                "Marque as tabelas que deseja gerar.\n"
                "A ordem de execucao respeita as dependencias automaticamente."
            )
        )

        todos_marcados = pre_selecionados is None or len(pre_selecionados) == len(tabelas)
        self.chk_todos = QCheckBox("Selecionar todas")
        self.chk_todos.setChecked(todos_marcados)
        self.chk_todos.stateChanged.connect(self._alternar_todos)
        layout.addWidget(self.chk_todos)

        self.lista = QListWidget()
        pre_set = set(pre_selecionados) if pre_selecionados is not None else None

        for tabela in tabelas:
            texto = f"{tabela['nome']}\n   {tabela['descricao']}"
            item = QListWidgetItem(texto)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            estado = Qt.Checked if (pre_set is None or tabela["id"] in pre_set) else Qt.Unchecked
            item.setCheckState(estado)
            item.setData(Qt.UserRole, tabela["id"])
            self.lista.addItem(item)
        layout.addWidget(self.lista, 1)

        botoes = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        botoes.accepted.connect(self.accept)
        botoes.rejected.connect(self.reject)
        layout.addWidget(botoes)

    def _alternar_todos(self, estado: int) -> None:
        marcado = Qt.Checked if estado == Qt.Checked.value else Qt.Unchecked
        for idx in range(self.lista.count()):
            self.lista.item(idx).setCheckState(marcado)

    def tabelas_selecionadas(self) -> list[str]:
        selecionadas = []
        for idx in range(self.lista.count()):
            item = self.lista.item(idx)
            if item.checkState() == Qt.Checked:
                selecionadas.append(item.data(Qt.UserRole))
        return selecionadas


class DialogoFioDeOuro(QDialog):
    def __init__(self, df: pl.DataFrame, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Auditoria Fio de Ouro (Origens Enriquecidas)")
        self.resize(1000, 500)
        self.setWindowFlag(Qt.WindowMaximizeButtonHint)
        layout = QVBoxLayout(self)
        self.table_view = QTableView()
        self.model = PolarsTableModel()
        self.model.set_dataframe(df)
        self.table_view.setModel(self.model)
        self.table_view.setAlternatingRowColors(True)
        self.table_view.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table_view)

        btn_close = QPushButton("Fechar")
        btn_close.clicked.connect(self.accept)
        layout.addWidget(btn_close)
