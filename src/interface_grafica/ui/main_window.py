from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Callable

import polars as pl
from PySide6.QtCore import QDate, QThread, Qt, Signal, QUrl
from PySide6.QtGui import QAction, QDesktopServices, QGuiApplication, QKeySequence, QShortcut
from PySide6.QtWidgets import (
    QMenu,
    QAbstractItemView,
    QApplication,
    QCheckBox,
    QDateEdit,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QPlainTextEdit,
    QComboBox,
    QScrollArea,
    QSplitter,
    QStatusBar,
    QTabWidget,
    QTableView,
    QTextEdit,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
    QStyledItemDelegate,
    QDoubleSpinBox,
)

from interface_grafica.config import (
    APP_NAME,
    CNPJ_ROOT,
    CONSULTAS_ROOT,
    DEFAULT_PAGE_SIZE,
)
from interface_grafica.models.table_model import PolarsTableModel
from interface_grafica.services.aggregation_service import ServicoAgregacao
from interface_grafica.services.export_service import ExportService
from interface_grafica.services.parquet_service import FilterCondition, ParquetService
from interface_grafica.services.pipeline_funcoes_service import ResultadoPipeline, ServicoPipelineCompleto
from interface_grafica.services.pipeline_service import PipelineService
from interface_grafica.services.query_worker import QueryWorker
from interface_grafica.services.registry_service import RegistryService
from interface_grafica.services.selection_persistence_service import SelectionPersistenceService
from interface_grafica.services.sql_service import SqlService, ParamInfo, WIDGET_DATE
from interface_grafica.ui.dialogs import (
    ColumnSelectorDialog,
    DialogoSelecaoConsultas,
    DialogoSelecaoTabelas,
)
from utilitarios.text import display_cell, normalize_text, remove_accents


class FloatDelegate(QStyledItemDelegate):
    def createEditor(self, parent, option, index):
        editor = QDoubleSpinBox(parent)
        editor.setDecimals(6)
        editor.setMinimum(-999999999.0)
        editor.setMaximum(999999999.0)
        return editor

class PipelineWorker(QThread):
    finished_ok = Signal(object)
    failed = Signal(str)
    progress = Signal(str)

    def __init__(
        self,
        service: ServicoPipelineCompleto,
        cnpj: str,
        consultas: list[Path],
        tabelas: list[str],
        data_limite: str | None = None,
    ) -> None:
        super().__init__()
        self.service = service
        self.cnpj = cnpj
        self.consultas = consultas
        self.tabelas = tabelas
        self.data_limite = data_limite

    def run(self) -> None:
        try:
            result = self.service.executar_completo(
                self.cnpj, 
                self.consultas, 
                self.tabelas, 
                self.data_limite,
                progresso=self.progress.emit
            )
        except Exception as exc:  # pragma: nao cover - UI
            self.failed.emit(str(exc))
            return
        
        if result.ok:
            self.finished_ok.emit(result)
        else:
            message = "\n".join(result.erros) if result.erros else "Falha nao pipeline."
            self.failed.emit(message or "Falha sem detalhes.")


@dataclass
class ViewState:
    current_cnpj: str | None = None
    current_file: Path | None = None
    current_page: int = 1
    page_size: int = DEFAULT_PAGE_SIZE
    all_columns: list[str] | None = None
    visible_columns: list[str] | None = None
    filters: list[FilterCondition] | None = None
    total_rows: int = 0


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle(APP_NAME)
        self.resize(1560, 920)

        self.registry_service = RegistryService()
        self.selection_service = SelectionPersistenceService()
        self.parquet_service = ParquetService(root=CNPJ_ROOT)
        self.pipeline_service = PipelineService(output_root=CONSULTAS_ROOT)
        self.servico_pipeline_funcoes = ServicoPipelineCompleto()
        self.export_service = ExportService()
        self.servico_agregacao = ServicoAgregacao()
        self.sql_service = SqlService()

        self.state = ViewState(filters=[])
        self.current_page_df_all = pl.DataFrame()
        self.current_page_df_visible = pl.DataFrame()
        self.table_model = PolarsTableModel()
        self.aggregation_table_model = PolarsTableModel(checkable=True)
        self.results_table_model = PolarsTableModel(checkable=True)
        self.conversion_model = PolarsTableModel()
        self.conversion_model.set_editable_columns({"fator", "unid_ref"})
        self.sql_result_model = PolarsTableModel()
        self.aggregation_basket: list[dict] = []
        self.aggregation_results: list[dict] = []
        self.pipeline_worker: PipelineWorker | None = None
        self.query_worker: QueryWorker | None = None
        self._sql_files: list = []
        self._sql_param_widgets: dict[str, QWidget] = {}
        self._sql_current_sql: str = ""
        self._sql_result_df: pl.DataFrame = pl.DataFrame()
        self._conversion_df_full: pl.DataFrame = pl.DataFrame()
        self._conversion_file_path: Path | None = None
        self._updating_conversion_model: bool = False
        self._aggregation_file_path: Path | None = None
        self._aggregation_filters: list[FilterCondition] = []
        self._aggregation_results_filters: list[FilterCondition] = []

        self._build_ui()
        self._connect_signals()
        self._setup_copy_shortcut()
        self.refresh_cnpjs()
        self.refresh_logs()
        self._populate_sql_combo()

    def _build_ui(self) -> None:
        central = QWidget()
        self.setCentralWidget(central)
        root_layout = QVBoxLayout(central)

        splitter = QSplitter(Qt.Horizontal)
        root_layout.addWidget(splitter)

        splitter.addWidget(self._build_left_panel())
        splitter.addWidget(self._build_right_panel())
        splitter.setSizes([310, 1200])

        self.status = QStatusBar()
        self.setStatusBar(self.status)
        self.status.showMessage("Pronto.")

    def _build_left_panel(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)

        cnpj_box = QGroupBox("CNPJs")
        cnpj_layout = QVBoxLayout(cnpj_box)
        input_line = QHBoxLayout()
        self.cnpj_input = QLineEdit()
        self.cnpj_input.setPlaceholderText("Digite o CNPJ com ou sem mascara")
        self.btn_run_pipeline = QPushButton("Extrair + Processar")
        input_line.addWidget(self.cnpj_input)
        input_line.addWidget(self.btn_run_pipeline)
        cnpj_layout.addLayout(input_line)

        date_line = QHBoxLayout()
        date_line.addWidget(QLabel("Data limite EFD:"))
        self.date_input = QDateEdit()
        self.date_input.setCalendarPopup(True)
        self.date_input.setDate(QDate.currentDate())
        self.date_input.setDisplayFormat("dd/MM/yyyy")
        date_line.addWidget(self.date_input)
        cnpj_layout.addLayout(date_line)

        actions_row1 = QHBoxLayout()
        self.btn_extrair_brutas = QPushButton("Extrair Tabelas Brutas")
        self.btn_processamento = QPushButton("Processamento")
        actions_row1.addWidget(self.btn_extrair_brutas)
        actions_row1.addWidget(self.btn_processamento)
        cnpj_layout.addLayout(actions_row1)

        actions_row2 = QHBoxLayout()
        self.btn_refresh_cnpjs = QPushButton("Atualizar lista")
        self.btn_open_cnpj_folder = QPushButton("Abrir pasta")
        actions_row2.addWidget(self.btn_refresh_cnpjs)
        actions_row2.addWidget(self.btn_open_cnpj_folder)
        cnpj_layout.addLayout(actions_row2)

        actions_row3 = QHBoxLayout()
        self.btn_apagar_dados = QPushButton("Apagar Dados do CNPJ")
        self.btn_apagar_dados.setStyleSheet("QPushButton { color: #e57373; }")
        self.btn_apagar_cnpj = QPushButton("Apagar CNPJ")
        self.btn_apagar_cnpj.setStyleSheet("QPushButton { color: #ef5350; font-weight: bold; }")
        actions_row3.addWidget(self.btn_apagar_dados)
        actions_row3.addWidget(self.btn_apagar_cnpj)
        cnpj_layout.addLayout(actions_row3)

        self.cnpj_list = QListWidget()
        cnpj_layout.addWidget(self.cnpj_list)
        layout.addWidget(cnpj_box)

        files_box = QGroupBox("Arquivos Parquet do CNPJ")
        files_layout = QVBoxLayout(files_box)
        self.file_tree = QTreeWidget()
        self.file_tree.setHeaderLabels(["Arquivo", "Local"])
        files_layout.addWidget(self.file_tree)
        layout.addWidget(files_box)

        notes = QLabel(
            "Fluxo recomendado: analise um CNPJ, abra a tabela desejada, filtre, selecione colunas e exporte. "
            "Para agregacao, trabalhe sobre a tabela desagregada e monte o lote na aba Agregacao."
        )
        notes.setWordWrap(True)
        layout.addWidget(notes)
        return panel

    def _build_right_panel(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)

        header = QHBoxLayout()
        self.lbl_context = QLabel("Nenhum arquivo selecionado")
        self.lbl_context.setWordWrap(True)
        header.addWidget(self.lbl_context)
        header.addStretch()
        layout.addLayout(header)

        self.tabs = QTabWidget()
        self.tabs.addTab(self._build_tab_consulta(), "Consulta")
        self.tabs.addTab(self._build_tab_sql_query(), "Consulta SQL")
        self.tabs.addTab(self._build_tab_agregacao(), "Agregacao")
        self.tab_conversao = self._build_tab_conversao()
        self.tabs.addTab(self.tab_conversao, "Conversao")
        self.tabs.addTab(self._build_tab_logs(), "Logs")
        layout.addWidget(self.tabs)
        return panel

    def _build_tab_consulta(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)

        filter_box = QGroupBox("Filtros")
        filter_layout = QVBoxLayout(filter_box)
        form = QHBoxLayout()
        self.filter_column = QComboBox()
        self.filter_operator = QComboBox()
        self.filter_operator.addItems(["contem", "igual", "comeca com", "termina com", ">", ">=", "<", "<=", "e nulo", "nao e nulo"])
        self.filter_value = QLineEdit()
        self.filter_value.setPlaceholderText("Valor do filtro")
        self.btn_add_filter = QPushButton("Adicionar filtro")
        self.btn_clear_filters = QPushButton("Limpar filtros")
        form.addWidget(QLabel("Coluna"))
        form.addWidget(self.filter_column)
        form.addWidget(QLabel("Operador"))
        form.addWidget(self.filter_operator)
        form.addWidget(QLabel("Valor"))
        form.addWidget(self.filter_value)
        form.addWidget(self.btn_add_filter)
        form.addWidget(self.btn_clear_filters)
        filter_layout.addLayout(form)

        self.filter_list = QListWidget()
        self.filter_list.setMaximumHeight(90)
        filter_layout.addWidget(self.filter_list)

        filter_actions = QHBoxLayout()
        self.btn_remove_filter = QPushButton("Remover filtro selecionado")
        self.btn_choose_columns = QPushButton("Selecionar colunas")
        self.btn_prev_page = QPushButton("Pagina anterior")
        self.btn_next_page = QPushButton("Proxima pagina")
        self.lbl_page = QLabel("Pagina 0/0")
        filter_actions.addWidget(self.btn_remove_filter)
        filter_actions.addWidget(self.btn_choose_columns)
        filter_actions.addStretch()
        filter_actions.addWidget(self.btn_prev_page)
        filter_actions.addWidget(self.lbl_page)
        filter_actions.addWidget(self.btn_next_page)
        filter_layout.addLayout(filter_actions)
        layout.addWidget(filter_box)

        export_box = QGroupBox("Exportacao")
        export_layout = QHBoxLayout(export_box)
        self.btn_export_excel_full = QPushButton("Excel - tabela completa")
        self.btn_export_excel_filtered = QPushButton("Excel - tabela filtrada")
        self.btn_export_excel_visible = QPushButton("Excel - colunas visiveis")
        self.btn_export_docx = QPushButton("Relatorio Word")
        self.btn_export_html_txt = QPushButton("TXT com HTML")
        for btn in [
            self.btn_export_excel_full,
            self.btn_export_excel_filtered,
            self.btn_export_excel_visible,
            self.btn_export_docx,
            self.btn_export_html_txt,
        ]:
            export_layout.addWidget(btn)
        layout.addWidget(export_box)

        quick_filter_layout = QHBoxLayout()
        self.qf_norm = QLineEdit()
        self.qf_norm.setPlaceholderText("Filtrar Desc. Norm")
        self.qf_desc = QLineEdit()
        self.qf_desc.setPlaceholderText("Filtrar Descricao (ex.: buch 18)")
        self.qf_ncm = QLineEdit()
        self.qf_ncm.setPlaceholderText("Filtrar NCM")
        self.qf_cest = QLineEdit()
        self.qf_cest.setPlaceholderText("Filtrar CEST")
        
        for w in [self.qf_norm, self.qf_desc, self.qf_ncm, self.qf_cest]:
            w.setMaximumWidth(200)
            quick_filter_layout.addWidget(w)
        quick_filter_layout.addStretch()
        layout.addLayout(quick_filter_layout)

        self.table_view = QTableView()
        self.table_view.setModel(self.table_model)
        self.table_view.setSelectionBehavior(QAbstractItemView.SelectItems)
        self.table_view.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table_view.setAlternatingRowColors(True)
        self.table_view.setSortingEnabled(False)
        self.table_view.setWordWrap(True)
        self.table_view.verticalHeader().setDefaultSectionSize(60)
        self.table_view.horizontalHeader().setMinimumSectionSize(40)
        self.table_view.horizontalHeader().setDefaultSectionSize(200)
        self.table_view.horizontalHeader().setMaximumSectionSize(300)
        self.table_view.setStyleSheet("QTableView::item { padding: 4px 2px; }")
        self.table_view.horizontalHeader().setStretchLastSection(True)
        self.table_view.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table_view.customContextMenuRequested.connect(self._on_table_context_menu)
        layout.addWidget(self.table_view, 1)
        return tab

    def _build_tab_agregacao(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)

        top_box = QGroupBox("Tabela Agrupada Filtravel (Selecione linhas para agregar)")
        top_layout = QVBoxLayout(top_box)
        
        toolbar = QHBoxLayout()
        self.btn_open_editable_table = QPushButton("Abrir tabela agrupada")
        self.btn_execute_aggregation = QPushButton("Agregar Descricoes (da selecao)")
        self.btn_recalc_defaults = QPushButton("Recalcular Padroes (Geral)")
        self.btn_recalc_totals = QPushButton("Recalcular Totais")
        self.btn_refazer_tabelas_agr = QPushButton("Refazer Tabelas _agr")
        
        toolbar.addWidget(self.btn_open_editable_table)
        toolbar.addWidget(self.btn_execute_aggregation)
        toolbar.addWidget(self.btn_recalc_defaults)
        toolbar.addWidget(self.btn_recalc_totals)
        toolbar.addWidget(self.btn_refazer_tabelas_agr)
        toolbar.addStretch()
        top_layout.addLayout(toolbar)

        agg_qf_layout = QHBoxLayout()
        self.aqf_norm = QLineEdit()
        self.aqf_norm.setPlaceholderText("Filtrar Desc. Norm")
        self.aqf_desc = QLineEdit()
        self.aqf_desc.setPlaceholderText("Filtrar Descricao (ex.: buch 18)")
        self.aqf_ncm = QLineEdit()
        self.aqf_ncm.setPlaceholderText("Filtrar NCM")
        self.aqf_cest = QLineEdit()
        self.aqf_cest.setPlaceholderText("Filtrar CEST")
        self.btn_clear_top_agg_filters = QPushButton("Limpar filtros")

        for w in [self.aqf_norm, self.aqf_desc, self.aqf_ncm, self.aqf_cest]:
            w.setMaximumWidth(200)
            agg_qf_layout.addWidget(w)
        agg_qf_layout.addWidget(self.btn_clear_top_agg_filters)
        agg_qf_layout.addStretch()
        top_layout.addLayout(agg_qf_layout)

        self.aggregation_table_view = QTableView()
        self.aggregation_table_view.setModel(self.aggregation_table_model)
        self.aggregation_table_view.setSelectionBehavior(QAbstractItemView.SelectItems)
        self.aggregation_table_view.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.aggregation_table_view.setAlternatingRowColors(True)
        self.aggregation_table_view.setWordWrap(True)
        self.aggregation_table_view.verticalHeader().setDefaultSectionSize(60)
        self.aggregation_table_view.horizontalHeader().setMinimumSectionSize(40)
        self.aggregation_table_view.horizontalHeader().setDefaultSectionSize(200)
        self.aggregation_table_view.horizontalHeader().setMaximumSectionSize(300)
        self.aggregation_table_view.setStyleSheet("QTableView::item { padding: 4px 2px; }")
        top_layout.addWidget(self.aggregation_table_view, 1)
        layout.addWidget(top_box, 3)

        bottom_box = QGroupBox("Linhas Agregadas (Mesma Tabela de Referencia)")
        bottom_layout = QVBoxLayout(bottom_box)
        agg_bottom_qf_layout = QHBoxLayout()
        self.bqf_norm = QLineEdit()
        self.bqf_norm.setPlaceholderText("Filtrar Desc. Norm")
        self.bqf_desc = QLineEdit()
        self.bqf_desc.setPlaceholderText("Filtrar Descricao (ex.: whisky 12)")
        self.bqf_ncm = QLineEdit()
        self.bqf_ncm.setPlaceholderText("Filtrar NCM")
        self.bqf_cest = QLineEdit()
        self.bqf_cest.setPlaceholderText("Filtrar CEST")
        self.btn_clear_bottom_agg_filters = QPushButton("Limpar filtros")

        for w in [self.bqf_norm, self.bqf_desc, self.bqf_ncm, self.bqf_cest]:
            w.setMaximumWidth(200)
            agg_bottom_qf_layout.addWidget(w)
        agg_bottom_qf_layout.addWidget(self.btn_clear_bottom_agg_filters)
        agg_bottom_qf_layout.addStretch()
        bottom_layout.addLayout(agg_bottom_qf_layout)

        self.results_table_view = QTableView()
        self.results_table_view.setModel(self.results_table_model)
        self.results_table_view.setSelectionBehavior(QAbstractItemView.SelectItems)
        self.results_table_view.setAlternatingRowColors(True)
        self.results_table_view.setWordWrap(True)
        self.results_table_view.verticalHeader().setDefaultSectionSize(60)
        self.results_table_view.horizontalHeader().setMinimumSectionSize(40)
        self.results_table_view.horizontalHeader().setDefaultSectionSize(200)
        self.results_table_view.horizontalHeader().setMaximumSectionSize(300)
        self.results_table_view.setStyleSheet("QTableView::item { padding: 4px 2px; }")
        self.results_table_view.setContextMenuPolicy(Qt.CustomContextMenu)
        self.results_table_view.customContextMenuRequested.connect(self._on_results_context_menu)
        bottom_layout.addWidget(self.results_table_view, 1)
        layout.addWidget(bottom_box, 1)

        return tab

    # ------------------------------------------------------------------
    # Aba Consulta SQL
    # ------------------------------------------------------------------
    def _build_tab_sql_query(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)

        # --- Linha superior: seletor de SQL + botAes ---
        top_bar = QHBoxLayout()
        top_bar.addWidget(QLabel("SQL:"))
        self.sql_combo = QComboBox()
        self.sql_combo.setMinimumWidth(300)
        top_bar.addWidget(self.sql_combo, 1)
        self.btn_sql_execute = QPushButton("Executar Consulta")
        self.btn_sql_execute.setStyleSheet("QPushButton { font-weight: bold; padding: 6px 16px; }")
        self.btn_sql_export = QPushButton("Exportar Excel")
        top_bar.addWidget(self.btn_sql_execute)
        top_bar.addWidget(self.btn_sql_export)
        layout.addLayout(top_bar)

        # --- Splitter: SQL + parametros (esquerda) | resultados (direita) ---
        splitter = QSplitter(Qt.Vertical)

        # Parte superior: SQL + parametros
        upper_widget = QWidget()
        upper_layout = QHBoxLayout(upper_widget)
        upper_layout.setContentsMargins(0, 0, 0, 0)

        # Visualizador SQL
        sql_group = QGroupBox("Texto SQL")
        sql_group_layout = QVBoxLayout(sql_group)
        self.sql_text_view = QPlainTextEdit()
        self.sql_text_view.setReadOnly(True)
        self.sql_text_view.setStyleSheet(
            "QPlainTextEdit { font-family: 'Consolas', 'Courier New', monospace; "
            "font-size: 12px; background: #1e1e2e; color: #cdd6f4; "
            "border: 1px solid #45475a; border-radius: 4px; padding: 8px; }"
        )
        self.sql_text_view.setMinimumHeight(120)
        sql_group_layout.addWidget(self.sql_text_view)
        upper_layout.addWidget(sql_group, 3)

        # Painel de parametros
        param_group = QGroupBox("Parametros")
        param_outer_layout = QVBoxLayout(param_group)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        self.sql_param_container = QWidget()
        self.sql_param_form = QFormLayout(self.sql_param_container)
        self.sql_param_form.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)
        scroll.setWidget(self.sql_param_container)
        param_outer_layout.addWidget(scroll)
        upper_layout.addWidget(param_group, 1)

        splitter.addWidget(upper_widget)

        # Parte inferior: resultados
        result_widget = QWidget()
        result_layout = QVBoxLayout(result_widget)
        result_layout.setContentsMargins(0, 0, 0, 0)

        # Status
        self.sql_status_label = QLabel("Selecione um SQL e clique em Executar.")
        self.sql_status_label.setStyleSheet(
            "QLabel { padding: 4px 8px; background: #f0f4ff; border-radius: 4px; "
            "border: 1px solid #d0d8e8; color: #334155; font-weight: bold; }"
        )
        result_layout.addWidget(self.sql_status_label)

        # Filtro rApido nos resultados
        sql_filter_bar = QHBoxLayout()
        self.sql_result_search = QLineEdit()
        self.sql_result_search.setPlaceholderText("Buscar nos resultados...")
        sql_filter_bar.addWidget(self.sql_result_search)
        self.sql_result_page_label = QLabel("")
        self.btn_sql_prev = QPushButton("< Anterior")
        self.btn_sql_next = QPushButton("Proxima >")
        sql_filter_bar.addWidget(self.btn_sql_prev)
        sql_filter_bar.addWidget(self.sql_result_page_label)
        sql_filter_bar.addWidget(self.btn_sql_next)
        result_layout.addLayout(sql_filter_bar)

        # Tabela de resultados
        self.sql_result_table = QTableView()
        self.sql_result_table.setModel(self.sql_result_model)
        self.sql_result_table.setSelectionBehavior(QAbstractItemView.SelectItems)
        self.sql_result_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.sql_result_table.setAlternatingRowColors(True)
        self.sql_result_table.setSortingEnabled(False)
        self.sql_result_table.setWordWrap(True)
        self.sql_result_table.verticalHeader().setDefaultSectionSize(60)
        self.sql_result_table.horizontalHeader().setMinimumSectionSize(40)
        self.sql_result_table.horizontalHeader().setDefaultSectionSize(200)
        self.sql_result_table.horizontalHeader().setMaximumSectionSize(400)
        self.sql_result_table.horizontalHeader().setStretchLastSection(True)
        self.sql_result_table.setStyleSheet("QTableView::item { padding: 4px 2px; }")
        result_layout.addWidget(self.sql_result_table, 1)

        splitter.addWidget(result_widget)
        splitter.setSizes([280, 500])

        layout.addWidget(splitter, 1)
        return tab

    def _build_tab_conversao(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)

        toolbar = QHBoxLayout()
        self.btn_refresh_conversao = QPushButton("Recarregar")
        self.btn_refresh_conversao.setIcon(QApplication.style().standardIcon(QApplication.style().StandardPixmap.SP_BrowserReload))
        self.chk_show_single_unit = QCheckBox("Mostrar itens de unidade unica")
        self.chk_show_single_unit.setChecked(False)
        self.btn_export_conversao = QPushButton("Exportar Excel")
        self.btn_import_conversao = QPushButton("Importar Excel")
        
        toolbar.addWidget(self.btn_refresh_conversao)
        toolbar.addWidget(self.chk_show_single_unit)
        toolbar.addStretch()
        toolbar.addWidget(self.btn_import_conversao)
        layout.addLayout(toolbar)

        self.panel_unid_ref = QGroupBox("Alterar Unidade de Referencia do Produto Selecionado")
        panel_layout = QHBoxLayout(self.panel_unid_ref)
        self.lbl_produto_sel = QLabel("Nenhum produto selecionado")
        self.lbl_produto_sel.setStyleSheet("font-weight: bold; color: #1e40af;")
        self.combo_unid_ref = QComboBox()
        self.btn_apply_unid_ref = QPushButton("Aplicar a todos os itens")
        self.btn_apply_unid_ref.setStyleSheet("font-weight: bold;")
        self.btn_apply_unid_ref.setEnabled(False)
        self.combo_unid_ref.setEnabled(False)
        panel_layout.addWidget(self.lbl_produto_sel)
        panel_layout.addWidget(QLabel("   -> Nova unid_ref:"))
        panel_layout.addWidget(self.combo_unid_ref)
        panel_layout.addWidget(self.btn_apply_unid_ref)
        panel_layout.addStretch()
        layout.addWidget(self.panel_unid_ref)

        self.conversion_table = QTableView()
        self.conversion_table.setModel(self.conversion_model)
        self.conversion_table.setAlternatingRowColors(True)
        self.conversion_table.setSelectionBehavior(QAbstractItemView.SelectItems)
        self.conversion_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.conversion_table.setSortingEnabled(True)
        layout.addWidget(self.conversion_table)

        return tab

    def _build_tab_logs(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        self.log_view = QPlainTextEdit()
        self.log_view.setReadOnly(True)
        layout.addWidget(self.log_view)
        return tab

    def _connect_signals(self) -> None:
        self.btn_refresh_cnpjs.clicked.connect(self.refresh_cnpjs)
        self.btn_run_pipeline.clicked.connect(self.run_pipeline_for_input)
        self.btn_extrair_brutas.clicked.connect(self.extrair_tabelas_brutas)
        self.btn_processamento.clicked.connect(self.executar_processamento)
        self.btn_apagar_dados.clicked.connect(self.apagar_dados_cnpj)
        self.btn_apagar_cnpj.clicked.connect(self.apagar_cnpj_completo)
        self.cnpj_list.itemSelectionChanged.connect(self.on_cnpj_selected)
        self.file_tree.itemClicked.connect(self.on_file_activated)
        self.file_tree.itemDoubleClicked.connect(self.on_file_activated)
        self.btn_open_cnpj_folder.clicked.connect(self.open_cnpj_folder)

        self.btn_add_filter.clicked.connect(self.add_filter_from_form)
        self.btn_clear_filters.clicked.connect(self.clear_filters)
        self.btn_remove_filter.clicked.connect(self.remove_selected_filter)
        self.btn_choose_columns.clicked.connect(self.choose_columns)
        self.btn_prev_page.clicked.connect(self.prev_page)
        self.btn_next_page.clicked.connect(self.next_page)

        self.btn_export_excel_full.clicked.connect(lambda: self.export_excel("full"))
        self.btn_export_excel_filtered.clicked.connect(lambda: self.export_excel("filtered"))
        self.btn_export_excel_visible.clicked.connect(lambda: self.export_excel("visible"))
        self.btn_export_docx.clicked.connect(self.export_docx)
        self.btn_export_html_txt.clicked.connect(self.export_txt_html)

        self.btn_open_editable_table.clicked.connect(self.open_editable_aggregation_table)
        self.btn_execute_aggregation.clicked.connect(self.execute_aggregation)
        self.btn_recalc_defaults.clicked.connect(self.recalcular_padroes_agregacao)
        self.btn_recalc_totals.clicked.connect(self.recalcular_totais_agregacao)
        self.btn_refazer_tabelas_agr.clicked.connect(self.refazer_tabelas_agr_agregacao)
        self.btn_clear_top_agg_filters.clicked.connect(self.clear_top_aggregation_filters)
        self.btn_clear_bottom_agg_filters.clicked.connect(self.clear_bottom_aggregation_filters)

        for qf in [self.qf_norm, self.qf_desc, self.qf_ncm, self.qf_cest,
                   self.aqf_norm, self.aqf_desc, self.aqf_ncm, self.aqf_cest]:
            qf.returnPressed.connect(self.apply_quick_filters)
        for qf in [self.bqf_norm, self.bqf_desc, self.bqf_ncm, self.bqf_cest]:
            qf.returnPressed.connect(self.apply_aggregation_results_filters)

        # --- Consulta SQL tab ---
        self.sql_combo.currentIndexChanged.connect(self._on_sql_selected)
        self.btn_sql_execute.clicked.connect(self._execute_sql_query)
        self.btn_sql_export.clicked.connect(self._export_sql_results)
        self.sql_result_search.returnPressed.connect(self._filter_sql_results)
        self.btn_sql_prev.clicked.connect(self._sql_prev_page)
        self.btn_sql_next.clicked.connect(self._sql_next_page)

        # --- Conversao tab ---
        self.btn_refresh_conversao.clicked.connect(self.atualizar_aba_conversao)
        self.chk_show_single_unit.stateChanged.connect(lambda _state: self.atualizar_aba_conversao())
        self.btn_export_conversao.clicked.connect(self.exportar_conversao_excel)
        self.btn_import_conversao.clicked.connect(self.importar_conversao_excel)
        self.conversion_model.dataChanged.connect(self._on_conversion_model_changed)
        
        self.conversion_table.selectionModel().selectionChanged.connect(self._on_conversion_selection_changed)
        self.btn_apply_unid_ref.clicked.connect(self._apply_unid_ref_to_all)

    def _abrir_fio_de_ouro(self, id_agrupado: str) -> None:
        if not self.state.current_cnpj:
            return
            
        pasta_analises = CNPJ_ROOT / self.state.current_cnpj / "analises" / "produtos"
        arquivos = list(pasta_analises.glob(f"*_enriquecido_{self.state.current_cnpj}.parquet"))
        dfs = []
        for arq in arquivos:
            try:
                df = pl.read_parquet(arq).filter(pl.col("id_agrupado") == id_agrupado)
                if not df.is_empty():
                    df = df.with_columns(pl.lit(arq.name.split("_enriquecido")[0].upper()).alias("origem_fio_ouro"))
                    dfs.append(df)
            except Exception:
                pass
                
        if not dfs:
            self.show_info("Fio de Ouro", f"Nenhum registro enriquecido encontrado para: {id_agrupado}.")
            return
            
        try:
            df_final = pl.concat(dfs, how="diagonal_relaxed")
            from interface_grafica.ui.dialogs import DialogoFioDeOuro
            dlg = DialogoFioDeOuro(df_final, self)
            dlg.exec()
        except Exception as e:
            self.show_error("Fio de Ouro", f"Erro ao gerar trilha de auditoria: {e}")

    def _on_table_context_menu(self, pos) -> None:
        index = self.table_view.indexAt(pos)
        if not index.isValid(): return
        
        menu = QMenu(self)
        df = self.table_model.get_dataframe()
        if "id_agrupado" in df.columns:
            id_agrupado = df["id_agrupado"][index.row()]
            acao = menu.addAction(f"Auditoria 'Fio de Ouro' ({id_agrupado})")
            acao.triggered.connect(lambda: self._abrir_fio_de_ouro(id_agrupado))
            menu.exec(self.table_view.viewport().mapToGlobal(pos))
            
    def _on_results_context_menu(self, pos) -> None:
        index = self.results_table_view.indexAt(pos)
        if not index.isValid(): return
        
        menu = QMenu(self)
        df = self.results_table_model.get_dataframe()
        if "id_agrupado" in df.columns:
            id_agrupado = df["id_agrupado"][index.row()]
            acao = menu.addAction(f"Auditoria 'Fio de Ouro' ({id_agrupado})")
            acao.triggered.connect(lambda: self._abrir_fio_de_ouro(id_agrupado))
            menu.exec(self.results_table_view.viewport().mapToGlobal(pos))

    def show_error(self, title: str, message: str) -> None:
        QMessageBox.critical(self, title, message)

    def show_info(self, title: str, message: str) -> None:
        QMessageBox.information(self, title, message)

    def _setup_copy_shortcut(self) -> None:
        self.shortcut_copy = QShortcut(QKeySequence.StandardKey.Copy, self)
        self.shortcut_copy.activated.connect(self._copy_selection_from_active_table)

    def _copy_selection_from_active_table(self) -> None:
        tables = [
            self.table_view,
            self.aggregation_table_view,
            self.results_table_view,
            self.sql_result_table,
            self.conversion_table,
        ]
        active_table = next((t for t in tables if t and t.hasFocus()), None)
        if active_table is None:
            return

        selected_indexes = active_table.selectedIndexes()
        if not selected_indexes:
            return

        selected_indexes = sorted(selected_indexes, key=lambda i: (i.row(), i.column()))
        row_min = min(i.row() for i in selected_indexes)
        row_max = max(i.row() for i in selected_indexes)
        col_min = min(i.column() for i in selected_indexes)
        col_max = max(i.column() for i in selected_indexes)
        selected_map = {(i.row(), i.column()): i for i in selected_indexes}

        lines: list[str] = []
        for r in range(row_min, row_max + 1):
            vals: list[str] = []
            for c in range(col_min, col_max + 1):
                idx = selected_map.get((r, c))
                vals.append(str(idx.data() if idx is not None else ""))
            lines.append("\t".join(vals))

        QGuiApplication.clipboard().setText("\n".join(lines))

    def refresh_cnpjs(self) -> None:
        known = {record.cnpj for record in self.registry_service.list_records()}
        known.update(self.parquet_service.list_cnpjs())
        current = self.state.current_cnpj
        self.cnpj_list.clear()
        for cnpj in sorted(known):
            self.cnpj_list.addItem(cnpj)
        if current:
            matches = self.cnpj_list.findItems(current, Qt.MatchExactly)
            if matches:
                self.cnpj_list.setCurrentItem(matches[0])

    def run_pipeline_for_input(self) -> None:
        try:
            cnpj = self.servico_pipeline_funcoes.servico_extracao.sanitizar_cnpj(self.cnpj_input.text())
        except Exception as exc:
            self.show_error("CNPJ invalido", str(exc))
            return

        # 1. Selecionar Consultas SQL
        consultas_disp = self.servico_pipeline_funcoes.servico_extracao.listar_consultas()
        if not consultas_disp:
            self.show_error("Sem consultas", "Nenhum arquivo .sql encontrado na pasta sql/")
            return
            
        pre_sql = self.selection_service.get_selections("ultimas_consultas")
        dlg_sql = DialogoSelecaoConsultas(consultas_disp, self, pre_selecionados=pre_sql)
        if not dlg_sql.exec():
            return
        sql_selecionados = dlg_sql.consultas_selecionadas()
        self.selection_service.set_selections("ultimas_consultas", [str(p) for p in sql_selecionados])

        # 2. Selecionar Tabelas
        tabelas_disp = self.servico_pipeline_funcoes.servico_tabelas.listar_tabelas()
        pre_tabs = self.selection_service.get_selections("ultimas_tabelas")
        dlg_tab = DialogoSelecaoTabelas(tabelas_disp, self, pre_selecionados=pre_tabs)
        if not dlg_tab.exec():
            return
        tabelas_selecionadas = dlg_tab.tabelas_selecionadas()
        self.selection_service.set_selections("ultimas_tabelas", tabelas_selecionadas)

        if not sql_selecionados and not tabelas_selecionadas:
            return

        self.btn_run_pipeline.setEnabled(False)
        self.status.showMessage(f"Executando pipeline para {cnpj}...")
        
        data_limite = self.date_input.date().toString("dd/MM/yyyy")
        self.pipeline_worker = PipelineWorker(
            self.servico_pipeline_funcoes, 
            cnpj, 
            sql_selecionados, 
            tabelas_selecionadas, 
            data_limite
        )
        self.pipeline_worker.finished_ok.connect(self.on_pipeline_finished)
        self.pipeline_worker.failed.connect(self.on_pipeline_failed)
        self.pipeline_worker.progress.connect(self.status.showMessage)
        self.pipeline_worker.start()

    def on_pipeline_finished(self, result: ResultadoPipeline) -> None:
        self.btn_run_pipeline.setEnabled(True)
        self.registry_service.upsert(result.cnpj, ran_now=True)
        self.status.showMessage(f"Pipeline concluido para {result.cnpj}.")
        self.refresh_cnpjs()
        matches = self.cnpj_list.findItems(result.cnpj, Qt.MatchExactly)
        if matches:
            self.cnpj_list.setCurrentItem(matches[0])
            self.refresh_file_tree(result.cnpj)
            self.atualizar_aba_conversao()
            
        msg = "\n".join(result.mensagens[-10:]) if result.mensagens else "Processado com sucesso."
        self.show_info("Pipeline concluido", f"CNPJ {result.cnpj} processado.\n\nUltimas mensagens:\n{msg}")

    def on_pipeline_failed(self, message: str) -> None:
        self.btn_run_pipeline.setEnabled(True)
        self.status.showMessage("Falha na execucao do pipeline.")
        self.show_error("Falha nao pipeline", message)

    # ------------------------------------------------------------------
    # BotAes individuais: Extrair Brutas, Processamento, Apagar
    # ------------------------------------------------------------------
    def _obter_cnpj_valido(self) -> str | None:
        """ObtAm CNPJ valido da input box ou da selecao da lista."""
        texto = self.cnpj_input.text().strip()
        if not texto:
            item = self.cnpj_list.currentItem()
            if item:
                texto = item.text()
        if not texto:
            self.show_error("CNPJ nao informado", "Digite ou selecione um CNPJ.")
            return None
        try:
            return self.servico_pipeline_funcoes.servico_extracao.sanitizar_cnpj(texto)
        except Exception as exc:
            self.show_error("CNPJ invalido", str(exc))
            return None

    def extrair_tabelas_brutas(self) -> None:
        """Executa apenas a extracao SQL (fase 1 do pipeline)."""
        cnpj = self._obter_cnpj_valido()
        if not cnpj:
            return

        consultas_disp = self.servico_pipeline_funcoes.servico_extracao.listar_consultas()
        if not consultas_disp:
            self.show_error("Sem consultas", "Nenhum arquivo .sql encontrado na pasta sql/")
            return

        pre_sql = self.selection_service.get_selections("ultimas_consultas")
        dlg_sql = DialogoSelecaoConsultas(consultas_disp, self, pre_selecionados=pre_sql)
        if not dlg_sql.exec():
            return
        sql_selecionados = dlg_sql.consultas_selecionadas()
        self.selection_service.set_selections("ultimas_consultas", [str(p) for p in sql_selecionados])

        self.btn_extrair_brutas.setEnabled(False)
        self.status.showMessage(f"Extraindo tabelas brutas para {cnpj}...")

        data_limite = self.date_input.date().toString("dd/MM/yyyy")
        self.pipeline_worker = PipelineWorker(
            self.servico_pipeline_funcoes,
            cnpj,
            sql_selecionados,
            [],  # sem tabelas a apenas extracao
            data_limite,
        )
        self.pipeline_worker.finished_ok.connect(self._on_extracao_finished)
        self.pipeline_worker.failed.connect(self._on_extracao_failed)
        self.pipeline_worker.progress.connect(self.status.showMessage)
        self.pipeline_worker.start()

    def _on_extracao_finished(self, result: ResultadoPipeline) -> None:
        self.btn_extrair_brutas.setEnabled(True)
        self.status.showMessage(f"Extracao concluida para {result.cnpj}.")
        self.refresh_cnpjs()
        matches = self.cnpj_list.findItems(result.cnpj, Qt.MatchExactly)
        if matches:
            self.cnpj_list.setCurrentItem(matches[0])
            self.refresh_file_tree(result.cnpj)
        msg = "\n".join(result.mensagens[-10:]) if result.mensagens else "Extracao concluida."
        self.show_info("Extracao concluida", f"CNPJ {result.cnpj}.\n\n{msg}")

    def _on_extracao_failed(self, message: str) -> None:
        self.btn_extrair_brutas.setEnabled(True)
        self.status.showMessage("Falha na extracao.")
        self.show_error("Falha na extracao", message)

    def executar_processamento(self) -> None:
        """Executa apenas a geracao de tabelas (fase 2 do pipeline)."""
        cnpj = self._obter_cnpj_valido()
        if not cnpj:
            return

        tabelas_disp = self.servico_pipeline_funcoes.servico_tabelas.listar_tabelas()
        pre_tabs = self.selection_service.get_selections("ultimas_tabelas")
        dlg_tab = DialogoSelecaoTabelas(tabelas_disp, self, pre_selecionados=pre_tabs)
        if not dlg_tab.exec():
            return
        tabelas_selecionadas = dlg_tab.tabelas_selecionadas()
        self.selection_service.set_selections("ultimas_tabelas", tabelas_selecionadas)

        self.btn_processamento.setEnabled(False)
        self.status.showMessage(f"Gerando tabelas para {cnpj}...")

        self.pipeline_worker = PipelineWorker(
            self.servico_pipeline_funcoes,
            cnpj,
            [],  # sem consultas SQL a apenas processamento
            tabelas_selecionadas,
            None,
        )
        self.pipeline_worker.finished_ok.connect(self._on_processamento_finished)
        self.pipeline_worker.failed.connect(self._on_processamento_failed)
        self.pipeline_worker.progress.connect(self.status.showMessage)
        self.pipeline_worker.start()

    def _on_processamento_finished(self, result: ResultadoPipeline) -> None:
        self.btn_processamento.setEnabled(True)
        self.status.showMessage(f"Processamento concluido para {result.cnpj}.")
        self.refresh_cnpjs()
        matches = self.cnpj_list.findItems(result.cnpj, Qt.MatchExactly)
        if matches:
            self.cnpj_list.setCurrentItem(matches[0])
            self.refresh_file_tree(result.cnpj)
            self.atualizar_aba_conversao()
        msg = "\n".join(result.mensagens[-10:]) if result.mensagens else "Processamento concluido."
        self.show_info("Processamento concluido", f"CNPJ {result.cnpj}.\n\n{msg}")

    def _on_processamento_failed(self, message: str) -> None:
        self.btn_processamento.setEnabled(True)
        self.status.showMessage("Falha nao processamento.")
        self.show_error("Falha nao processamento", message)

    def apagar_dados_cnpj(self) -> None:
        """Apaga analises/ e arquivos_parquet/ do CNPJ selecionado (mantem pasta raiz)."""
        cnpj = self._obter_cnpj_valido()
        if not cnpj:
            return

        resp = QMessageBox.warning(
            self,
            "Apagar dados",
            f"Deseja apagar todos os dados (parquets e analises) do CNPJ {cnpj}?\n\nEsta acao nao pode ser desfeita.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if resp != QMessageBox.Yes:
            return

        ok = self.servico_pipeline_funcoes.servico_extracao.apagar_dados_cnpj(cnpj)
        if ok:
            self.show_info("Dados apagados", f"Os dados do CNPJ {cnpj} foram removidos.")
            self.refresh_file_tree(cnpj)
        else:
            self.show_error("Erro", f"A pasta do CNPJ {cnpj} nao foi encontrada.")

    def apagar_cnpj_completo(self) -> None:
        """Apaga toda a pasta do CNPJ selecionado."""
        cnpj = self._obter_cnpj_valido()
        if not cnpj:
            return

        resp = QMessageBox.warning(
            self,
            "Apagar CNPJ",
            f"Deseja APAGAR COMPLETAMENTE a pasta do CNPJ {cnpj}?\n\nTodos os arquivos serAo perdidos permanentemente.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if resp != QMessageBox.Yes:
            return

        ok = self.servico_pipeline_funcoes.servico_extracao.apagar_cnpj(cnpj)
        if ok:
            self.show_info("CNPJ removido", f"O CNPJ {cnpj} foi removido completamente.")
            self.refresh_cnpjs()
        else:
            self.show_error("Erro", f"A pasta do CNPJ {cnpj} nao foi encontrada.")

    def on_cnpj_selected(self) -> None:
        item = self.cnpj_list.currentItem()
        if item is None:
            return
        cnpj = item.text()
        self.state.current_cnpj = cnpj
        self.registry_service.upsert(cnpj, ran_now=False)
        self.refresh_file_tree(cnpj)
        self.atualizar_aba_conversao()
        self.atualizar_tabelas_agregacao()
        self.recarregar_historico_agregacao(cnpj)

        # Automacao de Data limite EFD baseada nao reg_0000
        data_efd = self.servico_pipeline_funcoes.servico_extracao.obter_data_entrega_reg0000(cnpj)
        if data_efd:
            qdate = QDate.fromString(data_efd, "dd/MM/yyyy")
            if qdate.isValid():
                self.date_input.setDate(qdate)


    def refresh_file_tree(self, cnpj: str) -> None:
        self.file_tree.clear()
        
        root_path = self.parquet_service.cnpj_dir(cnpj)
        
        cat_brutas = QTreeWidgetItem(["Tabelas brutas (SQL)", str(root_path / "arquivos_parquet")])
        cat_analises = QTreeWidgetItem(["Analises de Produtos", str(root_path / "analises" / "produtos")])
        cat_outros = QTreeWidgetItem(["Outros Parquets", str(root_path)])
        
        self.file_tree.addTopLevelItem(cat_brutas)
        self.file_tree.addTopLevelItem(cat_analises)
        self.file_tree.addTopLevelItem(cat_outros)

        first_leaf: QTreeWidgetItem | None = None
        
        for path in self.parquet_service.list_parquet_files(cnpj):
            # Identificar categoria
            if "arquivos_parquet" in str(path.parent):
                parent = cat_brutas
            elif "analises" in str(path.parent) or "produtos" in str(path.parent):
                parent = cat_analises
            else:
                parent = cat_outros
                
            item = QTreeWidgetItem([path.name, str(path.parent)])
            item.setData(0, Qt.UserRole, str(path))
            parent.addChild(item)
            if first_leaf is None:
                first_leaf = item
                
        cat_brutas.setExpanded(True)
        cat_analises.setExpanded(True)
        
        # Limpar categorias vazias
        for cat in [cat_brutas, cat_analises, cat_outros]:
            if cat.childCount() == 0:
                self.file_tree.takeTopLevelItem(self.file_tree.indexOfTopLevelItem(cat))

        if first_leaf is not None:
            self.file_tree.setCurrentItem(first_leaf)
            self.on_file_activated(first_leaf, 0)

    def on_file_activated(self, item: QTreeWidgetItem, _column: int) -> None:
        raw_path = item.data(0, Qt.UserRole)
        if not raw_path:
            return
        self.state.current_file = Path(raw_path)
        self.state.current_page = 1
        self.state.filters = []
        self.current_page_df_all = pl.DataFrame()
        self.current_page_df_visible = pl.DataFrame()
        self.load_current_file(reset_columns=True)
        self.tabs.setCurrentIndex(0)

    def load_current_file(self, reset_columns: bool = False) -> None:
        if self.state.current_file is None:
            return
        try:
            all_columns = self.parquet_service.get_schema(self.state.current_file)
        except Exception as exc:
            self.show_error("Erro ao abrir Parquet", str(exc))
            return
        self.state.all_columns = all_columns
        if reset_columns or not self.state.visible_columns:
            self.state.visible_columns = all_columns[:]
        self.filter_column.clear()
        self.filter_column.addItems(all_columns)
        self.reload_table()

    def reload_table(self, update_main_view: bool = True) -> None:
        if self.state.current_file is None:
            return
        try:
            page_result = self.parquet_service.get_page(
                parquet_path=self.state.current_file,
                conditions=self.state.filters or [],
                visible_columns=self.state.visible_columns or [],
                page=self.state.current_page,
                page_size=self.state.page_size,
            )
            self.state.total_rows = page_result.total_rows
            self.current_page_df_all = page_result.df_all_columns
            self.current_page_df_visible = page_result.df_visible

            if update_main_view:
                self.table_model.set_dataframe(self.current_page_df_visible)
                self._update_page_label()
                self._update_context_label()
                self._refresh_filter_list_widget()
                self.table_view.resizeColumnsToContents()
        except Exception as exc:
            self.show_error("Erro ao carregar dados", str(exc))

    def _update_page_label(self) -> None:
        total_pages = max(1, ((self.state.total_rows - 1) // self.state.page_size) + 1 if self.state.total_rows else 1)
        if self.state.current_page > total_pages:
            self.state.current_page = total_pages
        self.lbl_page.setText(f"Pagina {self.state.current_page}/{total_pages} | Linhas filtradas: {self.state.total_rows}")

    def _update_context_label(self) -> None:
        if self.state.current_file is None:
            self.lbl_context.setText("Nenhum arquivo selecionado")
            return
        self.lbl_context.setText(
            f"CNPJ: {self.state.current_cnpj or '-'} | Arquivo: {self.state.current_file.name} | "
            f"Colunas visiveis: {len(self.state.visible_columns or [])}/{len(self.state.all_columns or [])}"
        )

    def add_filter_from_form(self) -> None:
        column = self.filter_column.currentText().strip()
        operator = self.filter_operator.currentText().strip()
        value = self.filter_value.text().strip()
        if not column:
            self.show_error("Filtro invalido", "Selecione uma coluna para filtrar.")
            return
        if operator not in {"e nulo", "nao e nulo"} and value == "":
            self.show_error("Filtro invalido", "Informe um valor para o filtro escolhido.")
            return
        self.state.filters = self.state.filters or []
        self.state.filters.append(FilterCondition(column=column, operator=operator, value=value))
        self.state.current_page = 1
        self.filter_value.clear()
        self.reload_table()

    def clear_filters(self) -> None:
        self.state.filters = []
        self.state.current_page = 1
        self.reload_table()

    def remove_selected_filter(self) -> None:
        row = self.filter_list.currentRow()
        if row < 0 or not self.state.filters:
            return
        self.state.filters.pop(row)
        self.state.current_page = 1
        self.reload_table()

    def _refresh_filter_list_widget(self) -> None:
        self.filter_list.clear()
        for cond in self.state.filters or []:
            text = f"{cond.column} {cond.operator} {cond.value}".strip()
            self.filter_list.addItem(text)

    def choose_columns(self) -> None:
        if not self.state.all_columns:
            return
        dialog = ColumnSelectorDialog(self.state.all_columns, self.state.visible_columns or self.state.all_columns, self)
        if dialog.exec():
            selected = dialog.selected_columns()
            if not selected:
                self.show_error("Selecao invAlida", "Pelo menos uma coluna deve permanecer visivel.")
                return
            self.state.visible_columns = selected
            self.state.current_page = 1
            self.reload_table()

    def prev_page(self) -> None:
        if self.state.current_page > 1:
            self.state.current_page -= 1
            self.reload_table()

    def next_page(self) -> None:
        total_pages = max(1, ((self.state.total_rows - 1) // self.state.page_size) + 1 if self.state.total_rows else 1)
        if self.state.current_page < total_pages:
            self.state.current_page += 1
            self.reload_table()

    def _save_dialog(self, title: str, pattern: str) -> Path | None:
        filename, _ = QFileDialog.getSaveFileName(self, title, str(CONSULTAS_ROOT), pattern)
        return Path(filename) if filename else None

    def _filters_text(self) -> str:
        return " | ".join(f"{f.column} {f.operator} {f.value}".strip() for f in self.state.filters or [])

    def _dataset_for_export(self, mode: str) -> pl.DataFrame:
        if self.state.current_file is None:
            raise ValueError("Nenhum arquivo selecionado.")
        if mode == "full":
            return self.parquet_service.load_dataset(self.state.current_file)
        if mode == "filtered":
            return self.parquet_service.load_dataset(self.state.current_file, self.state.filters or [])
        if mode == "visible":
            return self.parquet_service.load_dataset(
                self.state.current_file,
                self.state.filters or [],
                self.state.visible_columns or [],
            )
        raise ValueError(f"Modo de exportacao nao suportado: {mode}")

    def export_excel(self, mode: str) -> None:
        try:
            df = self._dataset_for_export(mode)
            target = self._save_dialog("Salvar Excel", "Excel (*.xlsx)")
            if not target:
                return
            self.export_service.export_excel(target, df, sheet_name=self.state.current_file.stem if self.state.current_file else "Dados")
            self.show_info("Exportacao concluida", f"Arquivo gerado em:\n{target}")
        except Exception as exc:
            self.show_error("Falha na exportacao para Excel", str(exc))

    def export_docx(self) -> None:
        try:
            if self.state.current_file is None:
                raise ValueError("Nenhum arquivo selecionado.")
            df = self.parquet_service.load_dataset(self.state.current_file, self.state.filters or [], self.state.visible_columns or [])
            target = self._save_dialog("Salvar relatorio Word", "Word (*.docx)")
            if not target:
                return
            self.export_service.export_docx(
                target,
                title="Relatorio Padronizado de AnAlise Fiscal",
                cnpj=self.state.current_cnpj or "",
                table_name=self.state.current_file.name,
                df=df,
                filters_text=self._filters_text(),
                visible_columns=self.state.visible_columns or [],
            )
            self.show_info("Relatorio gerado", f"Arquivo gerado em:\n{target}")
        except Exception as exc:
            self.show_error("Falha na exportacao para Word", str(exc))

    def export_txt_html(self) -> None:
        try:
            if self.state.current_file is None:
                raise ValueError("Nenhum arquivo selecionado.")
            df = self.parquet_service.load_dataset(self.state.current_file, self.state.filters or [], self.state.visible_columns or [])
            html_report = self.export_service.build_html_report(
                title="Relatorio Padronizado de AnAlise Fiscal",
                cnpj=self.state.current_cnpj or "",
                table_name=self.state.current_file.name,
                df=df,
                filters_text=self._filters_text(),
                visible_columns=self.state.visible_columns or [],
            )
            target = self._save_dialog("Salvar TXT com HTML", "TXT (*.txt)")
            if not target:
                return
            self.export_service.export_txt_with_html(target, html_report)
            self.show_info("Relatorio HTML/TXT gerado", f"Arquivo gerado em:\n{target}")
        except Exception as exc:
            self.show_error("Falha na exportacao TXT/HTML", str(exc))

    def open_editable_aggregation_table(self) -> None:
        if not self.state.current_cnpj:
            self.show_error("CNPJ nao selecionado", "Selecione um CNPJ na lista.")
            return
        try:
            target = self.servico_agregacao.carregar_tabela_editavel(self.state.current_cnpj)
            self._aggregation_file_path = target
            self._aggregation_filters = []
            self._aggregation_results_filters = []
            self._load_aggregation_table()
            self.recarregar_historico_agregacao(self.state.current_cnpj)
        except Exception as exc:
            self.show_error("Falha ao abrir tabela editAvel", str(exc))
            return

        self.tabs.setCurrentIndex(2) # Switch to Agregacao tab (0-indexed: Consulta, SQL, Agregacao, Logs)

    def _load_aggregation_table(self) -> None:
        if self._aggregation_file_path is None:
            return
        df = self.parquet_service.load_dataset(self._aggregation_file_path, self._aggregation_filters or [])
        self.aggregation_table_model.set_dataframe(df)
        self.aggregation_table_view.resizeColumnsToContents()

    def execute_aggregation(self) -> None:
        if not self.state.current_cnpj:
            self.show_error("CNPJ nao selecionado", "Selecione um CNPJ antes de agregar.")
            return

        rows_top = self.aggregation_table_model.get_checked_rows()
        rows_bottom = self.results_table_model.get_checked_rows()
        
        # Merge and de-duplicate
        combined = []
        seen = set()
        for r in (rows_top + rows_bottom):
            key = str(r.get("id_agrupado") or "").strip()
            if not key:
                key = str(r.get("chave_produto") or "").strip()
            if not key:
                key = str(r.get("chave_item") or "").strip()
            if not key:
                key = str(r.get("descr_padrao") or r.get("descricao") or "").strip().upper()
            if key not in seen:
                seen.add(key)
                combined.append(r)

        if len(combined) < 2:
            self.show_error("Selecao insuficiente", "Marque pelo menos duas linhas com 'Visto' (pode ser em ambas as tabelas) para agregar.")
            return

        try:
            # Novo: Passar lista de IDs agrupados para o servico
            ids_selecionados = [str(r.get("id_agrupado") or "") for r in combined if r.get("id_agrupado")]
            
            if len(ids_selecionados) < 2:
                 self.show_error("Selecao insuficiente", "Nao foi possivel identificar IDs unicos para os grupos selecionados.")
                 return

            self.servico_agregacao.agregar_linhas(
                cnpj=self.state.current_cnpj,
                ids_agrupados_selecionados=ids_selecionados,
            )
            # Update the tables to reflect the changes
            self.atualizar_tabelas_agregacao()
            self.recarregar_historico_agregacao(self.state.current_cnpj)
            self.refresh_logs()
            
            self.show_info(
                "Agregacao concluida",
                f"As {len(combined)} descricoes foram unificadas."
            )
        except Exception as e:
            import traceback
            traceback.print_exc()
            self.show_error("Erro na agregacao", f"Ocorreu um erro ao agregar: {e}")
            
            # Clear checks and reload top table
            self.aggregation_table_model.clear_checked()
            self.results_table_model.clear_checked()
            self.open_editable_aggregation_table()
        except Exception as exc:
            self.show_error("Falha na agregacao", str(exc))

    def apply_quick_filters(self) -> None:
        idx = self.tabs.currentIndex()
        if idx == 0: # Consulta
            fields = {
                "descricao_normalizada": self.qf_norm.text().strip(),
                "descricao": self.qf_desc.text().strip(),
                "ncm_padrao": self.qf_ncm.text().strip(),
                "cest_padrao": self.qf_cest.text().strip(),
            }
        elif idx == 2: # Agregacao (Index 2 is "Agregacao", Index 1 is "SQL")
            fields = {
                "descricao_normalizada": self.aqf_norm.text().strip(),
                "descricao": self.aqf_desc.text().strip(),
                "ncm_padrao": self.aqf_ncm.text().strip(),
                "cest_padrao": self.aqf_cest.text().strip(),
            }
        else:
            return

        def split_terms(value: str) -> list[str]:
            texto = (value or "").strip()
            if not texto:
                return []
            # Permite buscar varios trechos no mesmo campo.
            # Ex.: "buch 18", "buch;18" ou "buch, 18".
            partes = re.split(r"[;,]+|\s{2,}", texto)
            if len(partes) == 1 and " " in texto:
                partes = texto.split()
            return [p.strip() for p in partes if p and p.strip()]

        # Mapas de colunas equivalentes por tipo de filtro rapido.
        # Inclui colunas usadas na aba de Agregacao (ex.: descr_padrao).
        alternatives = {
            "ncm_padrao": ["ncm_padrao", "NCM_padrao", "lista_ncm", "ncm_final", "ncm"],
            "cest_padrao": ["cest_padrao", "CEST_padrao", "lista_cest", "cest_final", "cest"],
            "descricao_normalizada": [
                "descricao_normalizada",
                "descricao",
                "descr_norm",
                "descr_padrao",
                "descricao_final",
            ],
            "descricao": [
                "descricao",
                "lista_descricoes",
                "descr",
                "descr_padrao",
                "descricao_final",
            ],
        }

        # Remove filtros rapidos antigos (inclusive quando ficaram com nome de coluna "alias").
        quick_target_cols = set(fields.keys())
        for key in fields.keys():
            quick_target_cols.update(alternatives.get(key, []))

        # Na aba de Agregacao, o filtro rapido deve ser deterministico:
        # substitui totalmente os filtros anteriores para evitar "filtros ocultos".
        if idx == 2:
            new_filters = []
        else:
            new_filters = [f for f in (self.state.filters or []) if f.column not in quick_target_cols]
        
        available_columns = self.state.all_columns or []
        if idx == 2 and self._aggregation_file_path is not None:
            try:
                available_columns = self.parquet_service.get_schema(self._aggregation_file_path)
            except Exception:
                available_columns = list(self.aggregation_table_model.dataframe.columns)

        for col, val in fields.items():
            termos = split_terms(val)
            if termos:
                # Need to be flexible with column names as they might differ across files
                # We'll use the one present in the schema
                actual_col = col
                if available_columns:
                    # Match case-sensitive in alias map first.
                    if col in alternatives:
                        for alt in alternatives[col]:
                            if alt in available_columns:
                                actual_col = alt
                                break

                    # Fallback: match case/acento-insensitive
                    if actual_col not in available_columns:
                        target_clean = remove_accents(col).lower()
                        for c in available_columns:
                            if remove_accents(c).lower() == target_clean:
                                actual_col = c
                                break

                # Usa operador ASCII para evitar problemas de encoding no caminho UI -> servico.
                # Cada termo vira um filtro proprio; como os filtros sao encadeados,
                # a busca exige que todos os trechos estejam presentes.
                for termo in termos:
                    new_filters.append(FilterCondition(column=actual_col, operator="contem", value=termo))
        
        if idx == 2:
            self._aggregation_filters = new_filters
            self._load_aggregation_table()
        else:
            self.state.filters = new_filters
            self.state.current_page = 1
            self.reload_table(update_main_view=True)

    def refresh_logs(self) -> None:
        import json
        logs = [json.dumps(log) for log in self.servico_agregacao.ler_linhas_log()]
        self.log_view.setPlainText("\n".join(logs))

    def apply_aggregation_results_filters(self) -> None:
        if self.tabs.currentIndex() != 2:
            return

        fields = {
            "descricao_normalizada": self.bqf_norm.text().strip(),
            "descricao": self.bqf_desc.text().strip(),
            "ncm_padrao": self.bqf_ncm.text().strip(),
            "cest_padrao": self.bqf_cest.text().strip(),
        }

        def split_terms(value: str) -> list[str]:
            texto = (value or "").strip()
            if not texto:
                return []
            partes = re.split(r"[;,]+|\s{2,}", texto)
            if len(partes) == 1 and " " in texto:
                partes = texto.split()
            return [p.strip() for p in partes if p and p.strip()]

        alternatives = {
            "ncm_padrao": ["ncm_padrao", "NCM_padrao", "lista_ncm", "ncm_final", "ncm"],
            "cest_padrao": ["cest_padrao", "CEST_padrao", "lista_cest", "cest_final", "cest"],
            "descricao_normalizada": [
                "descricao_normalizada",
                "descricao",
                "descr_norm",
                "descr_padrao",
                "descricao_final",
            ],
            "descricao": [
                "descricao",
                "lista_descricoes",
                "descr",
                "descr_padrao",
                "descricao_final",
            ],
        }

        new_filters: list[FilterCondition] = []
        cnpj = self.state.current_cnpj
        if not cnpj:
            self._aggregation_results_filters = []
            self.recarregar_historico_agregacao("")
            return

        path = self.servico_agregacao.caminho_tabela_agregadas(cnpj)
        available_columns = []
        if path.exists():
            try:
                available_columns = self.parquet_service.get_schema(path)
            except Exception:
                available_columns = list(self.results_table_model.dataframe.columns)

        for col, val in fields.items():
            termos = split_terms(val)
            if not termos:
                continue

            actual_col = col
            if available_columns:
                if col in alternatives:
                    for alt in alternatives[col]:
                        if alt in available_columns:
                            actual_col = alt
                            break
                if actual_col not in available_columns:
                    target_clean = remove_accents(col).lower()
                    for c in available_columns:
                        if remove_accents(c).lower() == target_clean:
                            actual_col = c
                            break

            for termo in termos:
                new_filters.append(FilterCondition(column=actual_col, operator="contem", value=termo))

        self._aggregation_results_filters = new_filters
        self.recarregar_historico_agregacao(cnpj)

    def clear_top_aggregation_filters(self) -> None:
        for widget in [self.aqf_norm, self.aqf_desc, self.aqf_ncm, self.aqf_cest]:
            widget.clear()
        self._aggregation_filters = []
        self._load_aggregation_table()

    def clear_bottom_aggregation_filters(self) -> None:
        for widget in [self.bqf_norm, self.bqf_desc, self.bqf_ncm, self.bqf_cest]:
            widget.clear()
        self._aggregation_results_filters = []
        cnpj = self.state.current_cnpj or ""
        self.recarregar_historico_agregacao(cnpj)

    def open_cnpj_folder(self) -> None:
        if not self.state.current_cnpj:
            self.show_error("CNPJ nao selecionado", "Selecione um CNPJ para abrir a pasta.")
            return
        target = self.parquet_service.cnpj_dir(self.state.current_cnpj)
        if not target.exists():
            self.show_error("Pasta inexistente", f"A pasta {target} ainda nao foi criada.")
            return
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(target)))

    def _on_conversion_selection_changed(self, selected, deselected) -> None:
        indexes = self.conversion_table.selectionModel().selectedIndexes()
        if not indexes or self._conversion_df_full.is_empty():
            self.lbl_produto_sel.setText("Nenhum produto selecionado")
            self.combo_unid_ref.clear()
            self.combo_unid_ref.setEnabled(False)
            self.btn_apply_unid_ref.setEnabled(False)
            self._current_selected_id_produto = None
            return
            
        row = indexes[0].row()
        df = self.conversion_model.dataframe
        if row < 0 or row >= df.height:
            return
            
        id_prod = df.item(row, df.columns.index("id_produtos"))
        descr = df.item(row, df.columns.index("descr_padrao"))
        
        self.lbl_produto_sel.setText(f"{id_prod} - {descr}")
        self._current_selected_id_produto = id_prod
        
        # Obter unidades unicas originais vinculadas a este ID
        try:
            unidades_s = self._conversion_df_full.filter(pl.col("id_produtos") == id_prod).get_column("unid").drop_nulls().cast(pl.Utf8)
            unidades = unidades_s.unique().to_list() if not unidades_s.is_empty() else []
        except Exception:
            unidades = []
        
        self.combo_unid_ref.clear()
        if unidades:
            self.combo_unid_ref.addItems(sorted(unidades))
            self.combo_unid_ref.setEnabled(True)
            self.btn_apply_unid_ref.setEnabled(True)
        else:
            self.combo_unid_ref.setEnabled(False)
            self.btn_apply_unid_ref.setEnabled(False)

    def _apply_unid_ref_to_all(self) -> None:
        id_prod = getattr(self, "_current_selected_id_produto", None)
        nova_unid = self.combo_unid_ref.currentText()
        if not id_prod or not nova_unid or self._conversion_df_full.is_empty():
            return
            
        # Determinar o preco medio da nova unidade de referencia
        df_prod = self._conversion_df_full.filter(pl.col("id_produtos") == id_prod)
        row_ref = df_prod.filter(pl.col("unid") == nova_unid)
        
        novo_preco_ref = None
        if not row_ref.is_empty():
            val = row_ref.get_column("preco_medio")[0]
            if val is not None:
                try:
                    novo_preco_ref = float(val)
                except Exception:
                    pass

        # Atualizar unid_ref para as linhas do produto
        self._conversion_df_full = self._conversion_df_full.with_columns(
            pl.when(pl.col("id_produtos") == id_prod)
            .then(pl.lit(nova_unid))
            .otherwise(pl.col("unid_ref"))
            .alias("unid_ref")
        )
        
        # Recalcular fatores de conversao das unidades relativas ao novo preco alvo
        if novo_preco_ref is not None and novo_preco_ref > 0:
            self._conversion_df_full = self._conversion_df_full.with_columns(
                pl.when(pl.col("id_produtos") == id_prod)
                .then(
                    pl.when(pl.col("preco_medio").is_not_null())
                    .then(pl.col("preco_medio").cast(pl.Float64) / novo_preco_ref)
                    .otherwise(1.0)
                )
                .otherwise(pl.col("fator"))
                .alias("fator")
            )
        else:
            # Caso a nova unidade selecionada nao tenha preco medio valido, forcamos fator 1.0 para todo o produto
            self._conversion_df_full = self._conversion_df_full.with_columns(
                pl.when(pl.col("id_produtos") == id_prod)
                .then(pl.lit(1.0))
                .otherwise(pl.col("fator"))
                .alias("fator")
            )
        
        # Salvar as alteracoes matematicas
        if self._conversion_file_path:
            self._conversion_df_full.drop("__row_id__").write_parquet(self._conversion_file_path)
            
        self.status.showMessage(f"Unidade {nova_unid} e fatores recalculados aplicados para {id_prod}.")
        self.atualizar_aba_conversao()

    def atualizar_aba_conversao(self) -> None:
        """Carrega os fatores de conversao do CNPJ atual."""
        cnpj = self.state.current_cnpj
        if not cnpj:
            self._atualizar_titulo_aba_conversao()
            return

        pasta_produtos = CNPJ_ROOT / cnpj / "analises" / "produtos"
        arq_conversao = pasta_produtos / f"fatores_conversao_{cnpj}.parquet"

        if not arq_conversao.exists():
            self.conversion_model.set_dataframe(pl.DataFrame())
            self._conversion_df_full = pl.DataFrame()
            self._conversion_file_path = None
            self._atualizar_titulo_aba_conversao()
            return

        try:
            df = pl.read_parquet(arq_conversao).with_row_index("__row_id__")
            self._conversion_df_full = df
            self._conversion_file_path = arq_conversao
            total_bruto = df.height
            df_vis = df
            # Regra de visualizacao (padrao): oculta produtos com apenas uma unidade,
            # pois nesses casos o fator tende a ser 1.0 e nao agrega analise.
            mostrar_unidade_unica = getattr(self, "chk_show_single_unit", None)
            mostrar_unidade_unica = bool(mostrar_unidade_unica and mostrar_unidade_unica.isChecked())
            if (not mostrar_unidade_unica) and {"id_produtos", "unid"}.issubset(set(df.columns)):
                df_multi_unid = (
                    df_vis.group_by("id_produtos")
                    .agg(pl.col("unid").cast(pl.Utf8, strict=False).drop_nulls().n_unique().alias("qtd_unid"))
                    .filter(pl.col("qtd_unid") > 1)
                    .select("id_produtos")
                )
                if df_multi_unid.height > 0:
                    df_vis = df_vis.join(df_multi_unid, on="id_produtos", how="inner")
                else:
                    df_vis = pl.DataFrame(schema=df_vis.schema)

            self._updating_conversion_model = True
            self.conversion_model.set_dataframe(df_vis)
            self._updating_conversion_model = False
            self.conversion_table.resizeColumnsToContents()
            if "__row_id__" in self.conversion_model.dataframe.columns:
                col_idx = self.conversion_model.dataframe.columns.index("__row_id__")
                self.conversion_table.setColumnHidden(col_idx, True)
            self._atualizar_titulo_aba_conversao(df_vis.height, total_bruto)
        except Exception as e:
            self._updating_conversion_model = False
            self._atualizar_titulo_aba_conversao()
            QMessageBox.warning(self, "Erro", f"Erro ao carregar fatores de conversao: {e}")

    def _on_conversion_model_changed(self, top_left, bottom_right, _roles) -> None:
        if self._updating_conversion_model:
            return
        if self._conversion_file_path is None or self._conversion_df_full.is_empty():
            return

        df_vis = self.conversion_model.dataframe
        if df_vis.is_empty() or "__row_id__" not in df_vis.columns:
            return

        col_ini = top_left.column()
        col_fim = bottom_right.column()
        touched_cols = set(df_vis.columns[col_ini : col_fim + 1])
        if not ("fator" in touched_cols or "unid_ref" in touched_cols):
            return

        row_ini = max(0, top_left.row())
        row_fim = min(df_vis.height - 1, bottom_right.row())
        
        updates_row_id = []
        updates_fator = []
        updates_unid_ref = []
        
        for r in range(row_ini, row_fim + 1):
            row_id = df_vis.item(r, df_vis.columns.index("__row_id__"))
            
            # Fator
            try:
                fator = df_vis.item(r, df_vis.columns.index("fator"))
                fator_val = None if fator is None else float(fator)
            except Exception:
                fator_val = None
                
            # Unidade de Referencia
            try:
                unid_ref = df_vis.item(r, df_vis.columns.index("unid_ref"))
                unid_ref_val = None if unid_ref is None else str(unid_ref).strip()
            except Exception:
                unid_ref_val = None
                
            updates_row_id.append(int(row_id))
            updates_fator.append(fator_val)
            updates_unid_ref.append(unid_ref_val)

        if not updates_row_id:
            return

        df_updates = pl.DataFrame({
            "__row_id__": updates_row_id, 
            "fator_editado": updates_fator,
            "unid_ref_editado": updates_unid_ref
        })
        
        self._conversion_df_full = (
            self._conversion_df_full
            .join(df_updates, on="__row_id__", how="left")
            .with_columns([
                pl.coalesce([pl.col("fator_editado"), pl.col("fator")]).alias("fator"),
                pl.coalesce([pl.col("unid_ref_editado"), pl.col("unid_ref")]).alias("unid_ref")
            ])
            .drop(["fator_editado", "unid_ref_editado"])
        )

        self._conversion_df_full.drop("__row_id__").write_parquet(self._conversion_file_path)
        self.status.showMessage("Fator e/ou unidade de referencia atualizados e salvos.")

    def _atualizar_titulo_aba_conversao(self, visiveis: int | None = None, total: int | None = None) -> None:
        if not hasattr(self, "tabs") or not hasattr(self, "tab_conversao"):
            return
        idx = self.tabs.indexOf(self.tab_conversao)
        if idx < 0:
            return
        if visiveis is None or total is None:
            self.tabs.setTabText(idx, "Conversao")
            return
        self.tabs.setTabText(idx, f"Conversao ({visiveis}/{total})")

    def exportar_conversao_excel(self) -> None:
        """Exporta os fatores de conversao para Excel para edicao."""
        df = self.conversion_model.dataframe
        if "__row_id__" in df.columns:
            df = df.drop("__row_id__")
        if df.is_empty():
            QMessageBox.information(self, "Aviso", "Nao hA dados para exportar.")
            return

        path, _ = QFileDialog.getSaveFileName(self, "Salvar Excel", f"fator_conversao_{self.state.current_cnpj}.xlsx", "Excel (*.xlsx)")
        if not path:
            return

        try:
            df.write_excel(path)
            QMessageBox.information(self, "Sucesso", f"Arquivo salvo com sucesso:\n{path}")
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Erro ao exportar: {e}")

    def importar_conversao_excel(self) -> None:
        """Importa fatores de conversao do Excel, sobrescrevendo o Parquet."""
        cnpj = self.state.current_cnpj
        if not cnpj:
            return

        path, _ = QFileDialog.getOpenFileName(self, "Abrir Excel", "", "Excel (*.xlsx)")
        if not path:
            return

        try:
            df_excel = pl.read_excel(path)
            # Validacao conforme documentacao: id_produtos, descr_padrao, unid, unid_ref, fator
            mapping = {
                "id_produtos": "id_produtos",
                "descr_padrao": "descr_padrao",
                "unid": "unid",
                "unid_ref": "unid_ref",
                "fator": "fator"
            }
            cols_obrigatorias = list(mapping.keys())
            if not all(c in df_excel.columns for c in cols_obrigatorias):
                raise ValueError(f"O Excel deve conter as colunas: {cols_obrigatorias}")

            pasta_produtos = CNPJ_ROOT / cnpj / "analises" / "produtos"
            nome_saida = f"fatores_conversao_{cnpj}.parquet"
            
            # Renomeia para colunas internas e garante tipos
            df_imp = df_excel.select(cols_obrigatorias).rename({c: mapping[c] for c in cols_obrigatorias})
            df_imp = df_imp.with_columns([
                pl.col("fator").cast(pl.Float64)
            ])

            df_imp.write_parquet(pasta_produtos / nome_saida)
            self.atualizar_aba_conversao()
            QMessageBox.information(self, "Sucesso", "Fatores de conversao importados com sucesso.")
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Erro ao importar: {e}")

    def recalcular_padroes_agregacao(self) -> None:
        """Invoca o servico para recalcular todos os padroes do CNPJ atual."""
        cnpj = self.state.current_cnpj
        if not cnpj: return
        
        ret = QMessageBox.question(self, "Recalcular Padroes", 
                                   "Isso ira atualizar NCM, CEST, GTIN, UNID e SEFIN de TODOS os grupos baseando-se na moda dos itens originais.\nProsseguir?",
                                   QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if ret == QMessageBox.StandardButton.No: return
        
        try:
            ok = self.servico_agregacao.recalcular_todos_padroes(cnpj)
            if ok:
                self.atualizar_tabelas_agregacao()
                QMessageBox.information(self, "Sucesso", "Valores padrao recalculados com sucesso para toda a tabela.")
            else:
                QMessageBox.warning(self, "Aviso", "Nao foi possivel recalcular. Verifique se as tabelas existem.")
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Erro ao recalcular: {e}")

    def recalcular_totais_agregacao(self) -> None:
        """Invoca o servico para recalcular totais de entrada/saAda do CNPJ atual."""
        cnpj = self.state.current_cnpj
        if not cnpj: return
        
        ret = QMessageBox.question(self, "Recalcular Totais", 
                                   "Isso ira calcular os totais de Entrada (C170) e Saida (NFe) para todos os produtos (apenas operacoes mercantis).\nProsseguir?",
                                   QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if ret == QMessageBox.StandardButton.No: return
        
        self.status.showMessage("Calculando totais de valores...")
        try:
            ok = self.servico_agregacao.recalcular_valores_totais(cnpj)
            if ok:
                self.atualizar_tabelas_agregacao()
                QMessageBox.information(self, "Sucesso", "Totais de valores recalculados com sucesso.")
            else:
                QMessageBox.warning(self, "Aviso", "Nao foi possivel recalcular os totais.")
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Erro ao recalcular totais: {e}")
        finally:
            self.status.showMessage("Pronto.")

    def refazer_tabelas_agr_agregacao(self) -> None:
        """Regenera c170_agr/bloco_h_agr/nfe_agr/nfce_agr com as agregacoes atuais."""
        cnpj = self.state.current_cnpj
        if not cnpj:
            return

        ret = QMessageBox.question(
            self,
            "Refazer tabelas _agr",
            "Isso vai recalcular produtos_final, recriar c170_agr/bloco_h_agr/nfe_agr/nfce_agr e atualizar fatores_conversao, c176_xml e mov_estoque usando as agregacoes atuais.\nProsseguir?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if ret == QMessageBox.StandardButton.No:
            return

        self.status.showMessage("Refazendo tabelas _agr...")
        try:
            ok = self.servico_agregacao.recalcular_referencias_agr(cnpj)
            if ok:
                self.refresh_file_tree(cnpj)
                QMessageBox.information(self, "Sucesso", "Produtos final, tabelas _agr e tabelas derivadas refeitos com sucesso.")
            else:
                QMessageBox.warning(self, "Aviso", "Nao foi possivel refazer as tabelas de referencia.")
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Erro ao refazer tabelas de referencia: {e}")
        finally:
            self.status.showMessage("Pronto.")

    def refazer_fontes_produtos_agregacao(self) -> None:
        """Alias legado para refazer_tabelas_agr_agregacao."""
        self.refazer_tabelas_agr_agregacao()

    def recarregar_historico_agregacao(self, cnpj: str) -> None:
        """Carrega a tabela de descricoes agregadas no painel inferior."""
        try:
            path = self.servico_agregacao.caminho_tabela_agregadas(cnpj)
            if path.exists():
                df_agregadas = self.parquet_service.load_dataset(path, self._aggregation_results_filters or [])
            else:
                df_agregadas = pl.DataFrame()
            self.results_table_model.set_dataframe(df_agregadas)
            self.results_table_view.resizeColumnsToContents()
        except Exception:
            self.results_table_model.set_dataframe(pl.DataFrame())

    def atualizar_tabelas_agregacao(self) -> None:
        """Atualiza os modelos das tabelas de agregacao."""
        cnpj = self.state.current_cnpj
        if not cnpj: return
        self._aggregation_file_path = self.servico_agregacao.caminho_tabela_editavel(cnpj)
        if self._aggregation_file_path.exists():
            self._load_aggregation_table()
            
    # ==================================================================
    # Consulta SQL - metodos de suporte
    # ==================================================================
    _sql_result_page: int = 1
    _sql_result_page_size: int = DEFAULT_PAGE_SIZE

    def _populate_sql_combo(self) -> None:
        """Carrega a lista de arquivos SQL disponiveis no combo."""
        self._sql_files = self.sql_service.list_sql_files()
        self.sql_combo.blockSignals(True)
        self.sql_combo.clear()
        self.sql_combo.addItem("- Selecione uma consulta -")
        for info in self._sql_files:
            self.sql_combo.addItem(f"{info.display_name}  [{info.source_dir}]", str(info.path))
        self.sql_combo.blockSignals(False)

    def _on_sql_selected(self, index: int) -> None:
        """Ao selecionar um SQL no combo: le, exibe e gera o formulario de parametros."""
        if index <= 0:
            self.sql_text_view.setPlainText("")
            self._clear_param_form()
            self._sql_current_sql = ""
            return
        path_str = self.sql_combo.itemData(index)
        if not path_str:
            return
        try:
            sql_text = self.sql_service.read_sql(Path(path_str))
        except Exception as exc:
            self.show_error("Erro ao ler SQL", str(exc))
            return
        self._sql_current_sql = sql_text
        self.sql_text_view.setPlainText(sql_text)
        params = self.sql_service.extract_params(sql_text)
        self._rebuild_param_form(params)

    def _clear_param_form(self) -> None:
        """Remove todos os campos do formulario de parametros."""
        while self.sql_param_form.rowCount() > 0:
            self.sql_param_form.removeRow(0)
        self._sql_param_widgets.clear()

    def _rebuild_param_form(self, params: list[ParamInfo]) -> None:
        """Reconstroi o formulario de parametros conforme os parametros detectados."""
        self._clear_param_form()
        for param in params:
            label = QLabel(f":{param.name}")
            label.setStyleSheet("font-weight: bold; color: #1e40af;")
            if param.widget_type == WIDGET_DATE:
                widget = QDateEdit()
                widget.setCalendarPopup(True)
                widget.setDate(QDate.currentDate())
                widget.setDisplayFormat("dd/MM/yyyy")
            else:
                widget = QLineEdit()
                if param.placeholder:
                    widget.setPlaceholderText(param.placeholder)
                # Pre-preencher CNPJ se disponAvel
                if "cnpj" in param.name.lower() and self.state.current_cnpj:
                    widget.setText(self.state.current_cnpj)
            self.sql_param_form.addRow(label, widget)
            self._sql_param_widgets[param.name] = widget

    def _collect_param_values(self) -> dict[str, str]:
        """Coleta os valores do formulario de parametros."""
        values: dict[str, str] = {}
        for name, widget in self._sql_param_widgets.items():
            if isinstance(widget, QDateEdit):
                values[name] = widget.date().toString("dd/MM/yyyy")
            elif isinstance(widget, QLineEdit):
                values[name] = widget.text().strip()
            else:
                values[name] = ""
        return values

    def _execute_sql_query(self) -> None:
        """Executa a consulta SQL em thread separada."""
        if not self._sql_current_sql:
            self.show_error("Nenhum SQL", "Selecione um arquivo SQL antes de executar.")
            return
        if self.query_worker is not None and self.query_worker.isRunning():
            self.show_error("Aguarde", "Uma consulta ja estA em execucao.")
            return

        values = self._collect_param_values()
        binds = self.sql_service.build_binds(self._sql_current_sql, values)

        self.btn_sql_execute.setEnabled(False)
        self._set_sql_status("a3 Conectando ao Oracle...", "#fef9c3", "#92400e")

        self.query_worker = QueryWorker(self._sql_current_sql, binds)
        self.query_worker.progress.connect(lambda msg: self._set_sql_status(f"a3 {msg}", "#fef9c3", "#92400e"))
        self.query_worker.finished_ok.connect(self._on_query_finished)
        self.query_worker.failed.connect(self._on_query_failed)
        self.query_worker.start()

    def _on_query_finished(self, df: pl.DataFrame) -> None:
        """Callback quando a consulta Oracle finaliza com sucesso."""
        self.btn_sql_execute.setEnabled(True)
        self._sql_result_df = df
        self._sql_result_page = 1
        if df.height == 0:
            self._set_sql_status("a1i   Consulta retornou 0 resultados.", "#e0e7ff", "#3730a3")
            self.sql_result_model.set_dataframe(pl.DataFrame())
        else:
            self._set_sql_status(
                f"a... {df.height:,} linhas, {df.width} colunas.",
                "#dcfce7", "#166534"
            )
            self._show_sql_result_page()

    def _on_query_failed(self, message: str) -> None:
        """Callback quando a consulta Oracle falha."""
        self.btn_sql_execute.setEnabled(True)
        self._set_sql_status(f"a Erro: {message[:200]}", "#fee2e2", "#991b1b")

    def _set_sql_status(self, text: str, bg: str, fg: str) -> None:
        self.sql_status_label.setText(text)
        self.sql_status_label.setStyleSheet(
            f"QLabel {{ padding: 4px 8px; background: {bg}; border-radius: 4px; "
            f"border: 1px solid {bg}; color: {fg}; font-weight: bold; }}"
        )

    def _show_sql_result_page(self) -> None:
        """Exibe a pAgina atual dos resultados SQL."""
        df = self._sql_result_df
        if df.height == 0:
            return
        total_pages = max(1, ((df.height - 1) // self._sql_result_page_size) + 1)
        self._sql_result_page = max(1, min(self._sql_result_page, total_pages))
        offset = (self._sql_result_page - 1) * self._sql_result_page_size
        page_df = df.slice(offset, self._sql_result_page_size)
        self.sql_result_model.set_dataframe(page_df)
        self.sql_result_table.resizeColumnsToContents()
        self.sql_result_page_label.setText(
            f"Pagina {self._sql_result_page}/{total_pages} | Total: {df.height:,}"
        )

    def _sql_prev_page(self) -> None:
        if self._sql_result_page > 1:
            self._sql_result_page -= 1
            self._show_sql_result_page()

    def _sql_next_page(self) -> None:
        total_pages = max(1, ((self._sql_result_df.height - 1) // self._sql_result_page_size) + 1)
        if self._sql_result_page < total_pages:
            self._sql_result_page += 1
            self._show_sql_result_page()

    def _filter_sql_results(self) -> None:
        """Aplica filtro textual global sobre os resultados SQL."""
        search = self.sql_result_search.text().strip().lower()
        if not search or self._sql_result_df.height == 0:
            self._sql_result_page = 1
            self._show_sql_result_page()
            return
        # Filtrar em todas as colunas (cast para string)
        exprs = [
            pl.col(c).cast(pl.Utf8, strict=False).fill_null("").str.to_lowercase().str.contains(search, literal=True)
            for c in self._sql_result_df.columns
        ]
        combined = exprs[0]
        for e in exprs[1:]:
            combined = combined | e
        filtered = self._sql_result_df.filter(combined)
        if filtered.height == 0:
            self._set_sql_status(f"Busca '{search}' nao encontrou resultados.", "#e0e7ff", "#3730a3")
            self.sql_result_model.set_dataframe(pl.DataFrame())
        else:
            self._set_sql_status(
                f"a... Busca '{search}': {filtered.height:,} de {self._sql_result_df.height:,} linhas.",
                "#dcfce7", "#166534"
            )
            # Show first page of filtered results
            page_df = filtered.head(self._sql_result_page_size)
            self.sql_result_model.set_dataframe(page_df)
            self.sql_result_table.resizeColumnsToContents()
            total_pages = max(1, ((filtered.height - 1) // self._sql_result_page_size) + 1)
            self.sql_result_page_label.setText(f"Pagina 1/{total_pages} | Filtrado: {filtered.height:,}")

    def _export_sql_results(self) -> None:
        """Exporta os resultados da consulta SQL para Excel."""
        if self._sql_result_df.height == 0:
            self.show_error("Sem dados", "Execute uma consulta antes de exportar.")
            return
        target = self._save_dialog("Exportar resultados SQL para Excel", "Excel (*.xlsx)")
        if not target:
            return
        try:
            sql_name = self.sql_combo.currentText().split("[")[0].strip() or "consulta_sql"
            self.export_service.export_excel(target, self._sql_result_df, sheet_name=sql_name[:31])
            self.show_info("Exportacao concluida", f"Arquivo gerado em:\n{target}")
        except Exception as exc:
            self.show_error("Falha na exportacao", str(exc))
