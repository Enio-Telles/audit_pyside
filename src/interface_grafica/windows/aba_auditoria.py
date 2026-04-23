from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPlainTextEdit,
    QPushButton,
    QScrollArea,
    QSplitter,
    QTableView,
    QVBoxLayout,
    QWidget,
)


class AbaAuditoria:
    """Construtor da aba Consulta SQL/Auditoria."""

    def __init__(self, main_window):
        self.main = main_window
        self.root = QWidget()
        self._setup_ui()

    def _setup_ui(self) -> None:
        main = self.main
        layout = QVBoxLayout(self.root)

        top_bar = QHBoxLayout()
        top_bar.addWidget(QLabel("SQL:"))
        main.sql_combo = QComboBox()
        main.sql_combo.setMinimumWidth(300)
        top_bar.addWidget(main.sql_combo, 1)
        main.btn_sql_execute = QPushButton("Executar Consulta")
        main.btn_sql_execute.setStyleSheet(
            "QPushButton { font-weight: bold; padding: 6px 16px; }"
        )
        main.btn_sql_export = QPushButton("Exportar Excel")
        main.btn_sql_destacar = main._criar_botao_destacar()
        top_bar.addWidget(main.btn_sql_execute)
        top_bar.addWidget(main.btn_sql_export)
        top_bar.addWidget(main.btn_sql_destacar)
        layout.addLayout(top_bar)

        splitter = QSplitter(Qt.Vertical)

        upper_widget = QWidget()
        upper_layout = QHBoxLayout(upper_widget)
        upper_layout.setContentsMargins(0, 0, 0, 0)

        sql_group = QGroupBox("Texto SQL")
        sql_group_layout = QVBoxLayout(sql_group)
        main.sql_text_view = QPlainTextEdit()
        main.sql_text_view.setReadOnly(True)
        main.sql_text_view.setStyleSheet(
            "QPlainTextEdit { font-family: 'Consolas', 'Courier New', monospace; "
            "font-size: 12px; background: #1e1e2e; color: #cdd6f4; "
            "border: 1px solid #45475a; border-radius: 4px; padding: 8px; }"
        )
        main.sql_text_view.setMinimumHeight(120)
        sql_group_layout.addWidget(main.sql_text_view)
        upper_layout.addWidget(sql_group, 3)

        param_group = QGroupBox("Parametros")
        param_outer_layout = QVBoxLayout(param_group)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        main.sql_param_container = QWidget()
        main.sql_param_form = QFormLayout(main.sql_param_container)
        main.sql_param_form.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)
        scroll.setWidget(main.sql_param_container)
        param_outer_layout.addWidget(scroll)
        upper_layout.addWidget(param_group, 1)

        splitter.addWidget(upper_widget)

        result_widget = QWidget()
        result_layout = QVBoxLayout(result_widget)
        result_layout.setContentsMargins(0, 0, 0, 0)

        main.sql_status_label = QLabel("Selecione um SQL e clique em Executar.")
        main.sql_status_label.setStyleSheet(
            "QLabel { padding: 4px 8px; background: #f0f4ff; border-radius: 4px; "
            "border: 1px solid #d0d8e8; color: #334155; font-weight: bold; }"
        )
        result_layout.addWidget(main.sql_status_label)

        sql_filter_bar = QHBoxLayout()
        main.sql_result_search = QLineEdit()
        main.sql_result_search.setPlaceholderText("Buscar nos resultados...")
        sql_filter_bar.addWidget(main.sql_result_search)
        main.sql_result_page_label = QLabel("")
        main.btn_sql_prev = QPushButton("< Anterior")
        main.btn_sql_next = QPushButton("Proxima >")
        sql_filter_bar.addWidget(main.btn_sql_prev)
        sql_filter_bar.addWidget(main.sql_result_page_label)
        sql_filter_bar.addWidget(main.btn_sql_next)
        result_layout.addLayout(sql_filter_bar)

        main.sql_result_table = QTableView()
        main.sql_result_table.setModel(main.sql_result_model)
        main.sql_result_table.setSelectionBehavior(QAbstractItemView.SelectItems)
        main.sql_result_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        main.sql_result_table.setAlternatingRowColors(True)
        main.sql_result_table.setSortingEnabled(False)
        main.sql_result_table.setWordWrap(True)
        main.sql_result_table.verticalHeader().setDefaultSectionSize(60)
        main.sql_result_table.horizontalHeader().setMinimumSectionSize(40)
        main.sql_result_table.horizontalHeader().setDefaultSectionSize(200)
        main.sql_result_table.horizontalHeader().setMaximumSectionSize(400)
        main.sql_result_table.horizontalHeader().setStretchLastSection(True)
        main.sql_result_table.setStyleSheet("QTableView::item { padding: 4px 2px; }")
        result_layout.addWidget(main.sql_result_table, 1)

        splitter.addWidget(result_widget)
        splitter.setSizes([280, 500])

        layout.addWidget(splitter, 1)
