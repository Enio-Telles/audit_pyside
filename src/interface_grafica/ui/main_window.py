from __future__ import annotations

import base64
import re
from collections.abc import Callable
from pathlib import Path

import polars as pl
from interface_grafica.config import (
    APP_NAME,
    CNPJ_ROOT,
    CONSULTAS_ROOT,
    DEFAULT_PAGE_SIZE,
)
from interface_grafica.controllers.importacao_controller import (
    ImportacaoControllerMixin,
)
from interface_grafica.controllers.shared_state import ViewState
from interface_grafica.controllers.workers import PipelineWorker, ServiceTaskWorker
from interface_grafica.models.table_model import PolarsTableModel
from interface_grafica.services.aggregation_service import ServicoAgregacao
from interface_grafica.services.export_service import ExportService
from interface_grafica.services.parquet_service import FilterCondition, ParquetService
from interface_grafica.services.pipeline_funcoes_service import (
    ServicoPipelineCompleto,
)
from interface_grafica.services.pipeline_service import PipelineService
from interface_grafica.services.profile_utils import (
    ordenar_colunas_perfil,
    ordenar_colunas_visiveis,
)
from interface_grafica.services.query_worker import QueryWorker
from interface_grafica.services.registry_service import RegistryService
from interface_grafica.services.selection_persistence_service import (
    SelectionPersistenceService,
)
from interface_grafica.services.sql_service import WIDGET_DATE, ParamInfo, SqlService
from interface_grafica.ui.dialogs import (
    ColumnSelectorDialog,
)
from interface_grafica.widgets.detached_table_window import DetachedTableWindow
from interface_grafica.windows.aba_importacao import ImportacaoWindowMixin
from interface_grafica.windows.aba_auditoria import AuditoriaWindowMixin
from interface_grafica.windows.aba_agregacao import AgregacaoWindowMixin
from interface_grafica.windows.aba_relatorios import RelatoriosWindowMixin
from interface_grafica.controllers.relatorios_periodos_controller import RelatoriosPeriodosControllerMixin
from interface_grafica.controllers.relatorios_produtos_controller import RelatoriosProdutosControllerMixin
from interface_grafica.controllers.relatorios_resumo_controller import RelatoriosResumoControllerMixin
from interface_grafica.controllers.relatorios_style_controller import RelatoriosStyleControllerMixin
from interface_grafica.controllers.id_agrupados_controller import IdAgrupadosControllerMixin
from interface_grafica.controllers.consulta_controller import ConsultaControllerMixin
from interface_grafica.controllers.agregacao_controller import AgregacaoControllerMixin
from interface_grafica.controllers.conversao_controller import ConversaoControllerMixin
from interface_grafica.controllers.sql_query_controller import SqlQueryControllerMixin
from interface_grafica.controllers.auditoria_controller import AuditoriaControllerMixin
from openpyxl import Workbook
from openpyxl.styles import Font as OpenPyxlFont
from PySide6.QtCore import QByteArray, QDate, Qt, QThread, QTimer, QUrl
from PySide6.QtGui import (
    QDesktopServices,
    QFont,
    QGuiApplication,
    QKeySequence,
    QShortcut,
)
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QCheckBox,
    QComboBox,
    QDateEdit,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QLineEdit,
    QListWidget,
    QMainWindow,
    QMenu,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QScrollArea,
    QSplitter,
    QStatusBar,
    QTableView,
    QTabWidget,
    QToolBar,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)
from utilitarios.text import (
    display_cell,
    is_year_column_name,
    remove_accents,
)


class MainWindow(RelatoriosStyleControllerMixin, RelatoriosResumoControllerMixin, RelatoriosProdutosControllerMixin, RelatoriosPeriodosControllerMixin, RelatoriosWindowMixin, SqlQueryControllerMixin, ConversaoControllerMixin, AgregacaoControllerMixin, ConsultaControllerMixin, IdAgrupadosControllerMixin, AgregacaoWindowMixin, AuditoriaControllerMixin, AuditoriaWindowMixin, ImportacaoControllerMixin, ImportacaoWindowMixin, QMainWindow):
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
        self.mov_estoque_model = PolarsTableModel(
            foreground_resolver=self._mov_estoque_foreground,
            background_resolver=self._mov_estoque_background,
            font_resolver=self._mov_estoque_font,
        )
        self.sql_result_model = PolarsTableModel()
        self.aggregation_basket: list[dict] = []
        self.aggregation_results: list[dict] = []
        self.pipeline_worker: PipelineWorker | None = None
        self.query_worker: QueryWorker | None = None
        self.service_worker: ServiceTaskWorker | None = None
        self._sql_files: list = []
        self._sql_param_widgets: dict[str, QWidget] = {}
        self._sql_current_sql: str = ""
        self._sql_result_df: pl.DataFrame = pl.DataFrame()
        self._conversion_df_full: pl.DataFrame = pl.DataFrame()
        self._recalculando_conversao = False
        self._conversion_recalc_pending = False
        self._mov_estoque_file_path: Path | None = None
        self._mov_estoque_df: pl.DataFrame = pl.DataFrame()
        self.aba_anual_model = PolarsTableModel(
            checkable=True,
            foreground_resolver=self._aba_anual_foreground,
            background_resolver=self._aba_anual_background,
        )
        self.aba_periodos_model = PolarsTableModel(
            df=pl.DataFrame(),
            checkable=True,
            foreground_resolver=self._aba_anual_foreground,
            background_resolver=self._aba_anual_background,
        )
        self._aba_periodos_file_path: Path | None = None
        self._aba_periodos_df: pl.DataFrame = pl.DataFrame()
        self._aba_anual_file_path: Path | None = None
        self._aba_anual_df: pl.DataFrame = pl.DataFrame()
        self.aba_mensal_model = PolarsTableModel(
            checkable=True,
            foreground_resolver=self._aba_mensal_foreground,
            background_resolver=self._aba_mensal_background,
        )
        self._aba_mensal_file_path: Path | None = None
        self._aba_mensal_df: pl.DataFrame = pl.DataFrame()
        self.nfe_entrada_model = PolarsTableModel()
        self._nfe_entrada_file_path: Path | None = None
        self._nfe_entrada_df: pl.DataFrame = pl.DataFrame()
        self.id_agrupados_model = PolarsTableModel()
        self._id_agrupados_file_path: Path | None = None
        self._id_agrupados_df: pl.DataFrame = pl.DataFrame()
        self.produtos_selecionados_model = PolarsTableModel(checkable=True)
        self._produtos_selecionados_df: pl.DataFrame = pl.DataFrame()
        self._produtos_selecionados_mov_df: pl.DataFrame = pl.DataFrame()
        self._produtos_selecionados_mensal_df: pl.DataFrame = pl.DataFrame()
        self._produtos_selecionados_anual_df: pl.DataFrame = pl.DataFrame()
        self._produtos_selecionados_periodos_df: pl.DataFrame = pl.DataFrame()
        self.resumo_global_model = PolarsTableModel()
        self._resumo_global_df: pl.DataFrame = pl.DataFrame()
        self._produtos_sel_preselecionado_cnpj: str | None = None
        self._filtro_cruzado_anuais_ids: list[str] = []
        self._aggregation_file_path: Path | None = None
        self._aggregation_filters: list[FilterCondition] = []
        self._aggregation_results_filters: list[FilterCondition] = []
        self._aggregation_relational_mode: str | None = None
        self._aggregation_results_relational_mode: str | None = None
        self._sync_resumos_estoque_cnpj: str | None = None
        self._debounce_timers: dict[str, QTimer] = {}
        self._debounce_callbacks: dict[str, Callable[[], None]] = {}
        self._auto_resized_tables: set[str] = set()
        self._detached_windows: dict[str, DetachedTableWindow] = {}
        self._closing_after_workers = False

        self._build_ui()
        self._connect_signals()
        self._setup_copy_shortcut()
        self._refresh_profile_combos()
        self.refresh_cnpjs()
        self.refresh_logs()
        self._populate_sql_combo()
        # verifica conexão Oracle automaticamente na abertura da aplicação
        # Removido: não verificar conexões automaticamente na inicialização (solicitado pelo usuário)

    def _executar_callback_debounce(self, key: str) -> None:
        callback = self._debounce_callbacks.get(key)
        if callback is None:
            return
        callback()

    def _schedule_debounced(
        self, key: str, callback: Callable[[], None], delay_ms: int = 280
    ) -> None:
        timer = self._debounce_timers.get(key)
        if timer is None:
            timer = QTimer(self)
            timer.setSingleShot(True)
            timer.timeout.connect(lambda key=key: self._executar_callback_debounce(key))
            self._debounce_timers[key] = timer
        self._debounce_callbacks[key] = callback
        timer.start(delay_ms)

    def _registrar_limpeza_worker(self, attr_name: str, worker: QThread) -> None:
        def _cleanup() -> None:
            if getattr(self, attr_name, None) is worker:
                setattr(self, attr_name, None)
            worker.deleteLater()
            self._atualizar_estado_botao_nfe_entrada()
            if self._closing_after_workers:
                self._tentar_fechar_apos_workers()

        worker.finished.connect(_cleanup)

    def _workers_ativos(self) -> list[QThread]:
        ativos: list[QThread] = []
        for worker in (self.pipeline_worker, self.query_worker, self.service_worker):
            if worker is not None and worker.isRunning():
                ativos.append(worker)
        return ativos

    def _atualizar_estado_botao_nfe_entrada(self) -> None:
        if not hasattr(self, "btn_extract_nfe_entrada"):
            return
        habilitado = bool(self.state.current_cnpj) and not self._workers_ativos()
        self.btn_extract_nfe_entrada.setEnabled(habilitado)

    def _tentar_fechar_apos_workers(self) -> None:
        if self._workers_ativos():
            return
        self._closing_after_workers = False
        self.close()

    def closeEvent(self, event) -> None:
        ativos = self._workers_ativos()
        if not ativos:
            super().closeEvent(event)
            return

        if not self._closing_after_workers:
            self._closing_after_workers = True
            self.status.showMessage(
                "Aguardando o termino das operacoes em execucao para fechar a janela..."
            )
            self.setEnabled(False)
            for worker in ativos:
                worker.finished.connect(self._tentar_fechar_apos_workers)
            QTimer.singleShot(100, self._tentar_fechar_apos_workers)
        event.ignore()

    def _resize_table_once(self, table: QTableView, key: str) -> None:
        if key in self._auto_resized_tables:
            return
        table.resizeColumnsToContents()
        self._auto_resized_tables.add(key)

    def _reset_table_resize_flag(self, key: str) -> None:
        self._auto_resized_tables.discard(key)

    def _estilo_botao_destacar(self) -> str:
        return (
            "QPushButton { background: #0e639c; color: #ffffff; border: 1px solid #1177bb; "
            "border-radius: 4px; padding: 6px 10px; font-weight: bold; }"
            "QPushButton:hover { background: #1177bb; }"
            "QPushButton:pressed { background: #0b4f7c; }"
        )

    def _criar_botao_destacar(self, texto: str = "Destacar") -> QPushButton:
        botao = QPushButton(texto)
        botao.setStyleSheet(self._estilo_botao_destacar())
        return botao

    def _build_ui(self) -> None:
        central = QWidget()
        self.setCentralWidget(central)
        root_layout = QVBoxLayout(central)

        self.main_splitter = QSplitter(Qt.Horizontal)
        root_layout.addWidget(self.main_splitter)

        self.left_panel_widget = self._build_left_panel()
        self.main_splitter.addWidget(self.left_panel_widget)
        self.main_splitter.addWidget(self._build_right_panel())
        self.main_splitter.setSizes([310, 1200])

        self.status = QStatusBar()
        self.setStatusBar(self.status)
        self.status.showMessage("Pronto.")

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
        self.btn_toggle_panel = QPushButton("<< Ocultar Painel Lateral")
        self.btn_toggle_panel.setCheckable(True)
        self.tabs.setCornerWidget(self.btn_toggle_panel, Qt.TopRightCorner)

        self.tabs.addTab(self._build_tab_configuracoes(), "Configurações")
        self.tabs.addTab(self._build_tab_consulta(), "Consulta")
        self.tabs.addTab(self._build_tab_sql_query(), "Consulta SQL")
        self.tabs.addTab(self._build_tab_agregacao(), "Agregacao")
        self.tab_conversao = self._build_tab_conversao()
        self.tabs.addTab(self.tab_conversao, "Conversao")
        self.tabs.addTab(self._build_tab_estoque(), "Estoque")
        self.tab_nfe_entrada = self._build_tab_nfe_entrada()
        self.tabs.addTab(self.tab_nfe_entrada, "NFe Entrada")
        self.tabs.addTab(self._build_tab_analise_lote_cnpj(), "Análise Lote CNPJ")
        self.tabs.addTab(self._build_tab_logs(), "Logs")
        layout.addWidget(self.tabs)
        return panel

    # ------------------------------------------------------------------
    # Aba: Configurações Oracle / Aplicativo
    # ------------------------------------------------------------------



    # ------------------------------------------------------------------
    # Aba Consulta SQL
    # ------------------------------------------------------------------
















    def _connect_signals(self) -> None:
        def schedule_mov() -> None:
            self._schedule_debounced("mov_filters", self.aplicar_filtros_mov_estoque)

        def schedule_anual() -> None:
            self._schedule_debounced("anual_filters", self.aplicar_filtros_aba_anual)

        def schedule_mensal() -> None:
            self._schedule_debounced("mensal_filters", self.aplicar_filtros_aba_mensal)

        def schedule_nfe_entrada() -> None:
            self._schedule_debounced(
                "nfe_entrada_filters", self.aplicar_filtros_nfe_entrada
            )

        def schedule_produtos_sel() -> None:
            self._schedule_debounced(
                "produtos_sel_filters", self.aplicar_filtros_produtos_selecionados
            )

        def schedule_id_agrupados() -> None:
            self._schedule_debounced(
                "id_agrupados_filters", self.aplicar_filtros_id_agrupados
            )

        def schedule_conv() -> None:
            self._schedule_debounced(
                "conversao_filters", self.aplicar_filtros_conversao
            )

        def schedule_consulta_quick() -> None:
            self._schedule_debounced("consulta_quick_filters", self.apply_quick_filters)

        def schedule_agregacao_bottom() -> None:
            self._schedule_debounced(
                "agregacao_bottom_filters", self.apply_aggregation_results_filters
            )

        def schedule_sql_search() -> None:
            self._schedule_debounced("sql_result_search", self._filter_sql_results)

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
        self.btn_toggle_panel.toggled.connect(self._toggle_left_panel)
        self.tabs.currentChanged.connect(self._on_main_tab_changed)

        # --- Estoque Tab signals ---
        self.mov_filter_id.currentTextChanged.connect(lambda _value: schedule_mov())
        self.mov_filter_desc.textChanged.connect(lambda _value: schedule_mov())
        self.mov_filter_ncm.textChanged.connect(lambda _value: schedule_mov())
        self.mov_filter_tipo.currentIndexChanged.connect(lambda _index: schedule_mov())
        self.mov_filter_texto.textChanged.connect(lambda _value: schedule_mov())
        self.mov_filter_data_col.currentIndexChanged.connect(
            lambda _index: schedule_mov()
        )
        self.mov_filter_data_ini.dateChanged.connect(lambda _date: schedule_mov())
        self.mov_filter_data_fim.dateChanged.connect(lambda _date: schedule_mov())
        self.mov_filter_num_col.currentIndexChanged.connect(
            lambda _index: schedule_mov()
        )
        self.mov_filter_num_min.textChanged.connect(lambda _value: schedule_mov())
        self.mov_filter_num_max.textChanged.connect(lambda _value: schedule_mov())
        self.btn_mov_profile.clicked.connect(
            lambda: self._aplicar_perfil_tabela(
                "mov_estoque",
                self.mov_estoque_table,
                self.mov_estoque_model,
                self.mov_profile.currentText(),
                "mov_estoque",
            )
        )
        self.btn_mov_save_profile.clicked.connect(
            lambda: self._salvar_perfil_tabela_com_dialogo(
                "mov_estoque",
                self.mov_estoque_table,
                self.mov_estoque_model,
                self.mov_profile,
                [
                    "Exportar",
                    "Padrao",
                    "Auditoria",
                    "Auditoria Fiscal",
                    "Estoque",
                    "Custos",
                ],
            )
        )
        self.btn_mov_colunas.clicked.connect(
            lambda: self._abrir_menu_colunas_tabela(
                "mov_estoque", self.mov_estoque_table
            )
        )
        self.btn_mov_destacar.clicked.connect(
            lambda: self._destacar_tabela("mov_estoque")
        )
        self.mov_estoque_table.horizontalHeader().customContextMenuRequested.connect(
            lambda pos: self._abrir_menu_colunas_tabela(
                "mov_estoque", self.mov_estoque_table, pos
            )
        )
        self.mov_estoque_table.horizontalHeader().sectionMoved.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "mov_estoque", self.mov_estoque_table, self.mov_estoque_model
            )
        )
        self.mov_estoque_table.horizontalHeader().sectionResized.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "mov_estoque", self.mov_estoque_table, self.mov_estoque_model
            )
        )
        self.mov_estoque_table.horizontalHeader().sortIndicatorChanged.connect(
            lambda _index, _order: self._salvar_preferencias_tabela(
                "mov_estoque", self.mov_estoque_table, self.mov_estoque_model
            )
        )

        self.btn_export_mov_estoque.clicked.connect(self.exportar_mov_estoque_excel)
        self.btn_refresh_aba_anual.clicked.connect(self.atualizar_aba_anual)
        self.btn_apply_aba_anual_filters.clicked.connect(self.aplicar_filtros_aba_anual)
        self.btn_clear_aba_anual_filters.clicked.connect(self.limpar_filtros_aba_anual)
        self.btn_filtrar_estoque_anual.clicked.connect(
            self.filtrar_estoque_pela_selecao_anual
        )
        self.btn_limpar_filtro_cruzado.clicked.connect(self.limpar_filtro_cruzado_anual)
        self.btn_export_aba_anual.clicked.connect(self.exportar_aba_anual_excel)

        def schedule_periodos() -> None:
            self._schedule_debounced(
                "periodos_filters", self.aplicar_filtros_aba_periodos
            )

        self.btn_refresh_aba_periodos.clicked.connect(self.atualizar_aba_periodos)
        self.btn_apply_aba_periodos_filters.clicked.connect(
            self.aplicar_filtros_aba_periodos
        )
        self.btn_clear_aba_periodos_filters.clicked.connect(
            self.limpar_filtros_aba_periodos
        )
        self.btn_export_aba_periodos.clicked.connect(self.exportar_aba_periodos_excel)
        self.periodo_filter_id.currentTextChanged.connect(
            lambda _value: schedule_periodos()
        )
        self.periodo_filter_desc.textChanged.connect(lambda _value: schedule_periodos())
        self.periodo_filter_texto.textChanged.connect(
            lambda _value: schedule_periodos()
        )
        self.btn_periodo_profile.clicked.connect(
            lambda: self._aplicar_perfil_tabela(
                "aba_periodos",
                self.aba_periodos_table,
                self.aba_periodos_model,
                self.periodo_profile.currentText(),
                "aba_periodos",
            )
        )
        self.btn_periodo_save_profile.clicked.connect(
            lambda: self._salvar_perfil_tabela_com_dialogo(
                "aba_periodos",
                self.aba_periodos_table,
                self.aba_periodos_model,
                self.periodo_profile,
                ["Exportar", "Padrao", "Auditoria", "Estoque", "Custos"],
            )
        )
        self.btn_periodo_colunas.clicked.connect(
            lambda: self._abrir_menu_colunas_tabela(
                "aba_periodos", self.aba_periodos_table
            )
        )
        self.btn_destacar_aba_periodos.clicked.connect(
            lambda: self._destacar_tabela("aba_periodos")
        )
        self.aba_periodos_table.horizontalHeader().customContextMenuRequested.connect(
            lambda pos: self._abrir_menu_colunas_tabela(
                "aba_periodos", self.aba_periodos_table, pos
            )
        )
        self.anual_filter_id.currentTextChanged.connect(lambda _value: schedule_anual())
        self.anual_filter_desc.textChanged.connect(lambda _value: schedule_anual())
        self.anual_filter_ano.currentIndexChanged.connect(
            lambda _index: schedule_anual()
        )
        self.anual_filter_texto.textChanged.connect(lambda _value: schedule_anual())
        self.anual_filter_num_col.currentIndexChanged.connect(
            lambda _index: schedule_anual()
        )
        self.anual_filter_num_min.textChanged.connect(lambda _value: schedule_anual())
        self.anual_filter_num_max.textChanged.connect(lambda _value: schedule_anual())
        self.btn_anual_profile.clicked.connect(
            lambda: self._aplicar_perfil_tabela(
                "aba_anual",
                self.aba_anual_table,
                self.aba_anual_model,
                self.anual_profile.currentText(),
                "aba_anual",
            )
        )
        self.btn_anual_save_profile.clicked.connect(
            lambda: self._salvar_perfil_tabela_com_dialogo(
                "aba_anual",
                self.aba_anual_table,
                self.aba_anual_model,
                self.anual_profile,
                ["Exportar", "Padrao", "Auditoria", "Estoque", "Custos"],
            )
        )
        self.btn_anual_colunas.clicked.connect(
            lambda: self._abrir_menu_colunas_tabela("aba_anual", self.aba_anual_table)
        )
        self.btn_destacar_aba_anual.clicked.connect(
            lambda: self._destacar_tabela("aba_anual")
        )
        self.aba_anual_table.horizontalHeader().customContextMenuRequested.connect(
            lambda pos: self._abrir_menu_colunas_tabela(
                "aba_anual", self.aba_anual_table, pos
            )
        )
        self.aba_anual_table.horizontalHeader().sectionMoved.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "aba_anual", self.aba_anual_table, self.aba_anual_model
            )
        )
        self.aba_anual_table.horizontalHeader().sectionResized.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "aba_anual", self.aba_anual_table, self.aba_anual_model
            )
        )
        self.aba_anual_table.horizontalHeader().sortIndicatorChanged.connect(
            lambda _index, _order: self._salvar_preferencias_tabela(
                "aba_anual", self.aba_anual_table, self.aba_anual_model
            )
        )

        self.btn_refresh_resumo_global.clicked.connect(self.atualizar_aba_resumo_global)
        self.btn_export_resumo_global.clicked.connect(self.exportar_resumo_global_excel)

        self.btn_refresh_aba_mensal.clicked.connect(self.atualizar_aba_mensal)
        self.btn_apply_aba_mensal_filters.clicked.connect(
            self.aplicar_filtros_aba_mensal
        )
        self.btn_clear_aba_mensal_filters.clicked.connect(
            self.limpar_filtros_aba_mensal
        )
        self.btn_export_aba_mensal.clicked.connect(self.exportar_aba_mensal_excel)
        self.mensal_filter_num_col.currentIndexChanged.connect(
            lambda _index: schedule_mensal()
        )
        self.mensal_filter_num_min.textChanged.connect(lambda _value: schedule_mensal())
        self.mensal_filter_num_max.textChanged.connect(lambda _value: schedule_mensal())
        self.mensal_filter_id.currentTextChanged.connect(
            lambda _value: schedule_mensal()
        )
        self.mensal_filter_desc.textChanged.connect(lambda _value: schedule_mensal())
        self.mensal_filter_ano.currentIndexChanged.connect(
            lambda _index: schedule_mensal()
        )
        self.mensal_filter_mes.currentIndexChanged.connect(
            lambda _index: schedule_mensal()
        )
        self.mensal_filter_texto.textChanged.connect(lambda _value: schedule_mensal())
        self.btn_mensal_profile.clicked.connect(
            lambda: self._aplicar_perfil_tabela(
                "aba_mensal",
                self.aba_mensal_table,
                self.aba_mensal_model,
                self.mensal_profile.currentText(),
                "aba_mensal",
            )
        )
        self.btn_mensal_save_profile.clicked.connect(
            lambda: self._salvar_perfil_tabela_com_dialogo(
                "aba_mensal",
                self.aba_mensal_table,
                self.aba_mensal_model,
                self.mensal_profile,
                ["Exportar", "Padrao", "Auditoria", "Estoque", "Custos"],
            )
        )
        self.btn_mensal_colunas.clicked.connect(
            lambda: self._abrir_menu_colunas_tabela("aba_mensal", self.aba_mensal_table)
        )
        self.btn_destacar_aba_mensal.clicked.connect(
            lambda: self._destacar_tabela("aba_mensal")
        )
        self.aba_mensal_table.horizontalHeader().customContextMenuRequested.connect(
            lambda pos: self._abrir_menu_colunas_tabela(
                "aba_mensal", self.aba_mensal_table, pos
            )
        )
        self.aba_mensal_table.horizontalHeader().sectionMoved.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "aba_mensal", self.aba_mensal_table, self.aba_mensal_model
            )
        )
        self.aba_mensal_table.horizontalHeader().sectionResized.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "aba_mensal", self.aba_mensal_table, self.aba_mensal_model
            )
        )
        self.aba_mensal_table.horizontalHeader().sortIndicatorChanged.connect(
            lambda _index, _order: self._salvar_preferencias_tabela(
                "aba_mensal", self.aba_mensal_table, self.aba_mensal_model
            )
        )

        self.btn_extract_nfe_entrada.clicked.connect(self.extrair_dados_nfe_entrada)
        self.btn_refresh_nfe_entrada.clicked.connect(self.atualizar_aba_nfe_entrada)
        self.btn_apply_nfe_entrada_filters.clicked.connect(
            self.aplicar_filtros_nfe_entrada
        )
        self.btn_clear_nfe_entrada_filters.clicked.connect(
            self.limpar_filtros_nfe_entrada
        )
        self.btn_nfe_entrada_profile.clicked.connect(
            lambda: self._aplicar_perfil_tabela(
                "nfe_entrada",
                self.nfe_entrada_table,
                self.nfe_entrada_model,
                self.nfe_entrada_profile.currentText(),
                "nfe_entrada",
            )
        )
        self.btn_nfe_entrada_save_profile.clicked.connect(
            lambda: self._salvar_perfil_tabela_com_dialogo(
                "nfe_entrada",
                self.nfe_entrada_table,
                self.nfe_entrada_model,
                self.nfe_entrada_profile,
                ["Padrao", "Auditoria", "Estoque", "Custos"],
            )
        )
        self.btn_nfe_entrada_colunas.clicked.connect(
            lambda: self._abrir_menu_colunas_tabela(
                "nfe_entrada", self.nfe_entrada_table
            )
        )
        self.btn_nfe_entrada_destacar.clicked.connect(
            lambda: self._destacar_tabela("nfe_entrada")
        )
        self.btn_export_nfe_entrada.clicked.connect(self.exportar_nfe_entrada_excel)
        self.nfe_entrada_filter_id.currentTextChanged.connect(
            lambda _value: schedule_nfe_entrada()
        )
        self.nfe_entrada_filter_desc.textChanged.connect(
            lambda _value: schedule_nfe_entrada()
        )
        self.nfe_entrada_filter_ncm.textChanged.connect(
            lambda _value: schedule_nfe_entrada()
        )
        self.nfe_entrada_filter_sefin.textChanged.connect(
            lambda _value: schedule_nfe_entrada()
        )
        self.nfe_entrada_filter_texto.textChanged.connect(
            lambda _value: schedule_nfe_entrada()
        )
        self.nfe_entrada_filter_data_ini.dateChanged.connect(
            lambda _date: schedule_nfe_entrada()
        )
        self.nfe_entrada_filter_data_fim.dateChanged.connect(
            lambda _date: schedule_nfe_entrada()
        )
        self.nfe_entrada_table.horizontalHeader().customContextMenuRequested.connect(
            lambda pos: self._abrir_menu_colunas_tabela(
                "nfe_entrada", self.nfe_entrada_table, pos
            )
        )
        self.nfe_entrada_table.horizontalHeader().sectionMoved.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "nfe_entrada", self.nfe_entrada_table, self.nfe_entrada_model
            )
        )
        self.nfe_entrada_table.horizontalHeader().sectionResized.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "nfe_entrada", self.nfe_entrada_table, self.nfe_entrada_model
            )
        )
        self.nfe_entrada_table.horizontalHeader().sortIndicatorChanged.connect(
            lambda _index, _order: self._salvar_preferencias_tabela(
                "nfe_entrada", self.nfe_entrada_table, self.nfe_entrada_model
            )
        )

        self.btn_refresh_id_agrupados.clicked.connect(self.atualizar_aba_id_agrupados)
        self.btn_apply_id_agrupados_filters.clicked.connect(
            self.aplicar_filtros_id_agrupados
        )
        self.btn_clear_id_agrupados_filters.clicked.connect(
            self.limpar_filtros_id_agrupados
        )
        self.btn_id_agrupados_profile.clicked.connect(
            lambda: self._aplicar_perfil_tabela(
                "id_agrupados",
                self.id_agrupados_table,
                self.id_agrupados_model,
                self.id_agrupados_profile.currentText(),
                "id_agrupados",
            )
        )
        self.btn_id_agrupados_save_profile.clicked.connect(
            lambda: self._salvar_perfil_tabela_com_dialogo(
                "id_agrupados",
                self.id_agrupados_table,
                self.id_agrupados_model,
                self.id_agrupados_profile,
                ["Padrao", "Auditoria", "Estoque", "Custos"],
            )
        )
        self.btn_id_agrupados_colunas.clicked.connect(
            lambda: self._abrir_menu_colunas_tabela(
                "id_agrupados", self.id_agrupados_table
            )
        )
        self.btn_destacar_id_agrupados.clicked.connect(
            lambda: self._destacar_tabela("id_agrupados")
        )
        self.btn_export_id_agrupados.clicked.connect(self.exportar_id_agrupados_excel)
        self.id_agrupados_filter_id.currentTextChanged.connect(
            lambda _value: schedule_id_agrupados()
        )
        self.id_agrupados_filter_texto.textChanged.connect(
            lambda _value: schedule_id_agrupados()
        )
        self.id_agrupados_table.horizontalHeader().customContextMenuRequested.connect(
            lambda pos: self._abrir_menu_colunas_tabela(
                "id_agrupados", self.id_agrupados_table, pos
            )
        )
        self.id_agrupados_table.horizontalHeader().sectionMoved.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "id_agrupados", self.id_agrupados_table, self.id_agrupados_model
            )
        )
        self.id_agrupados_table.horizontalHeader().sectionResized.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "id_agrupados", self.id_agrupados_table, self.id_agrupados_model
            )
        )
        self.id_agrupados_table.horizontalHeader().sortIndicatorChanged.connect(
            lambda _index, _order: self._salvar_preferencias_tabela(
                "id_agrupados", self.id_agrupados_table, self.id_agrupados_model
            )
        )

        self.btn_refresh_produtos_sel.clicked.connect(
            self.atualizar_aba_produtos_selecionados
        )
        self.btn_apply_produtos_sel_filters.clicked.connect(
            self.aplicar_filtros_produtos_selecionados
        )
        self.btn_clear_produtos_sel_filters.clicked.connect(
            self.limpar_filtros_produtos_selecionados
        )
        self.btn_produtos_sel_profile.clicked.connect(
            lambda: self._aplicar_perfil_tabela(
                "produtos_selecionados",
                self.produtos_sel_table,
                self.produtos_selecionados_model,
                self.produtos_sel_profile.currentText(),
                "produtos_selecionados",
            )
        )
        self.btn_produtos_sel_save_profile.clicked.connect(
            lambda: self._salvar_perfil_tabela_com_dialogo(
                "produtos_selecionados",
                self.produtos_sel_table,
                self.produtos_selecionados_model,
                self.produtos_sel_profile,
                ["Padrao", "Auditoria", "Estoque", "Custos"],
            )
        )
        self.btn_colunas_produtos_sel.clicked.connect(
            lambda: self._abrir_menu_colunas_tabela(
                "produtos_selecionados", self.produtos_sel_table
            )
        )
        self.btn_destacar_produtos_sel.clicked.connect(
            lambda: self._destacar_tabela("produtos_selecionados")
        )
        self.btn_export_produtos_sel.clicked.connect(
            self.exportar_produtos_selecionados_excel
        )
        self.produtos_sel_filter_id.currentTextChanged.connect(
            lambda _value: schedule_produtos_sel()
        )
        self.produtos_sel_filter_desc.textChanged.connect(
            lambda _value: schedule_produtos_sel()
        )
        self.produtos_sel_filter_ano_ini.currentIndexChanged.connect(
            lambda _index: schedule_produtos_sel()
        )
        self.produtos_sel_filter_ano_fim.currentIndexChanged.connect(
            lambda _index: schedule_produtos_sel()
        )
        self.produtos_sel_filter_data_ini.dateChanged.connect(
            lambda _date: schedule_produtos_sel()
        )
        self.produtos_sel_filter_data_fim.dateChanged.connect(
            lambda _date: schedule_produtos_sel()
        )
        self.produtos_sel_filter_texto.textChanged.connect(
            lambda _value: schedule_produtos_sel()
        )
        self.produtos_sel_table.horizontalHeader().customContextMenuRequested.connect(
            lambda pos: self._abrir_menu_colunas_tabela(
                "produtos_selecionados", self.produtos_sel_table, pos
            )
        )
        self.produtos_sel_table.horizontalHeader().sectionMoved.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "produtos_selecionados",
                self.produtos_sel_table,
                self.produtos_selecionados_model,
            )
        )
        self.produtos_sel_table.horizontalHeader().sectionResized.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "produtos_selecionados",
                self.produtos_sel_table,
                self.produtos_selecionados_model,
            )
        )
        self.produtos_sel_table.horizontalHeader().sortIndicatorChanged.connect(
            lambda _index, _order: self._salvar_preferencias_tabela(
                "produtos_selecionados",
                self.produtos_sel_table,
                self.produtos_selecionados_model,
            )
        )

        self.btn_add_filter.clicked.connect(self.add_filter_from_form)
        self.btn_clear_filters.clicked.connect(self.clear_filters)
        self.btn_remove_filter.clicked.connect(self.remove_selected_filter)
        self.btn_choose_columns.clicked.connect(self.choose_columns)
        self.btn_apply_consulta_profile.clicked.connect(self._aplicar_perfil_consulta)
        self.btn_save_consulta_profile.clicked.connect(
            lambda: self._salvar_perfil_tabela_com_dialogo(
                "consulta",
                self.table_view,
                self.table_model,
                self.consulta_profile,
                ["Padrao", "Auditoria", "Estoque", "Custos"],
                self._consulta_scope(),
            )
        )
        self.btn_consulta_destacar.clicked.connect(
            lambda: self._destacar_tabela("consulta")
        )
        self.table_view.horizontalHeader().customContextMenuRequested.connect(
            lambda pos: self._abrir_menu_colunas_tabela(
                "consulta", self.table_view, pos, scope=self._consulta_scope()
            )
        )
        self.table_view.horizontalHeader().sectionMoved.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "consulta",
                self.table_view,
                self.table_model,
                scope=self._consulta_scope(),
            )
        )
        self.table_view.horizontalHeader().sectionResized.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "consulta",
                self.table_view,
                self.table_model,
                scope=self._consulta_scope(),
            )
        )
        self.btn_prev_page.clicked.connect(self.prev_page)
        self.btn_next_page.clicked.connect(self.next_page)

        self.btn_export_excel_full.clicked.connect(lambda: self.export_excel("full"))
        self.btn_export_excel_filtered.clicked.connect(
            lambda: self.export_excel("filtered")
        )
        self.btn_export_excel_visible.clicked.connect(
            lambda: self.export_excel("visible")
        )
        self.btn_export_docx.clicked.connect(self.export_docx)
        self.btn_export_html_txt.clicked.connect(self.export_txt_html)

        self.btn_open_editable_table.clicked.connect(
            self.open_editable_aggregation_table
        )
        self.btn_execute_aggregation.clicked.connect(self.execute_aggregation)
        self.btn_reprocessar_agregacao.clicked.connect(self.reprocessar_agregacao)
        self.btn_clear_top_agg_filters.clicked.connect(
            self.clear_top_aggregation_filters
        )
        self.btn_clear_bottom_agg_filters.clicked.connect(
            self.clear_bottom_aggregation_filters
        )
        self.btn_top_match_ncm_cest.clicked.connect(
            lambda: self._aplicar_filtro_relacional_agregacao("top", include_gtin=False)
        )
        self.btn_top_match_ncm_cest_gtin.clicked.connect(
            lambda: self._aplicar_filtro_relacional_agregacao("top", include_gtin=True)
        )
        self.btn_apply_top_profile.clicked.connect(
            lambda: self._aplicar_perfil_agregacao(
                "agregacao_top",
                self.aggregation_table,
                self.aggregation_table_model,
                self.top_profile.currentText(),
            )
        )
        self.btn_save_top_profile.clicked.connect(
            lambda: self._salvar_perfil_tabela_com_dialogo(
                "agregacao_top",
                self.aggregation_table,
                self.aggregation_table_model,
                self.top_profile,
                ["Padrao", "Auditoria", "Estoque", "Custos"],
            )
        )
        self.btn_top_colunas.clicked.connect(
            lambda: self._abrir_menu_colunas_tabela(
                "agregacao_top", self.aggregation_table
            )
        )
        self.btn_top_destacar.clicked.connect(
            lambda: self._destacar_tabela("agregacao_top")
        )
        self.aggregation_table.horizontalHeader().customContextMenuRequested.connect(
            lambda pos: self._abrir_menu_colunas_tabela(
                "agregacao_top", self.aggregation_table, pos
            )
        )
        self.aggregation_table.horizontalHeader().sectionMoved.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "agregacao_top", self.aggregation_table, self.aggregation_table_model
            )
        )
        self.aggregation_table.horizontalHeader().sectionResized.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "agregacao_top", self.aggregation_table, self.aggregation_table_model
            )
        )
        self.aggregation_table.horizontalHeader().sortIndicatorChanged.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "agregacao_top", self.aggregation_table, self.aggregation_table_model
            )
        )
        self.btn_apply_bottom_profile.clicked.connect(
            lambda: self._aplicar_perfil_agregacao(
                "agregacao_bottom",
                self.results_table,
                self.results_table_model,
                self.bottom_profile.currentText(),
            )
        )
        self.btn_save_bottom_profile.clicked.connect(
            lambda: self._salvar_perfil_tabela_com_dialogo(
                "agregacao_bottom",
                self.results_table,
                self.results_table_model,
                self.bottom_profile,
                ["Padrao", "Auditoria", "Estoque", "Custos"],
            )
        )
        self.btn_bottom_colunas.clicked.connect(
            lambda: self._abrir_menu_colunas_tabela(
                "agregacao_bottom", self.results_table
            )
        )
        self.results_table.horizontalHeader().customContextMenuRequested.connect(
            lambda pos: self._abrir_menu_colunas_tabela(
                "agregacao_bottom", self.results_table, pos
            )
        )
        self.results_table.horizontalHeader().sectionMoved.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "agregacao_bottom", self.results_table, self.results_table_model
            )
        )
        self.results_table.horizontalHeader().sectionResized.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "agregacao_bottom", self.results_table, self.results_table_model
            )
        )
        self.results_table.horizontalHeader().sortIndicatorChanged.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "agregacao_bottom", self.results_table, self.results_table_model
            )
        )
        self.btn_bottom_match_ncm_cest.clicked.connect(
            lambda: self._aplicar_filtro_relacional_agregacao(
                "bottom", include_gtin=False
            )
        )
        self.btn_bottom_match_ncm_cest_gtin.clicked.connect(
            lambda: self._aplicar_filtro_relacional_agregacao(
                "bottom", include_gtin=True
            )
        )
        self.btn_bottom_destacar.clicked.connect(
            lambda: self._destacar_tabela("agregacao_bottom")
        )
        for tabela, contexto in [
            (self.aggregation_table, "agregacao_top"),
            (self.results_table, "agregacao_bottom"),
            (self.sql_result_table, "sql_result"),
            (self.conversion_table, "conversao"),
            (self.aba_mensal_table, "aba_mensal"),
            (self.aba_anual_table, "aba_anual"),
            (self.nfe_entrada_table, "nfe_entrada"),
            (self.produtos_sel_table, "produtos_selecionados"),
            (self.id_agrupados_table, "id_agrupados"),
        ]:
            tabela.setContextMenuPolicy(Qt.CustomContextMenu)
            tabela.customContextMenuRequested.connect(
                lambda pos, t=tabela, ctx=contexto: self._abrir_menu_contexto_celula(
                    ctx, t, pos
                )
            )

        for qf in [
            self.qf_norm,
            self.qf_desc,
            self.qf_ncm,
            self.qf_cest,
            self.aqf_norm,
            self.aqf_desc,
            self.aqf_ncm,
            self.aqf_cest,
        ]:
            qf.returnPressed.connect(self.apply_quick_filters)
            qf.textChanged.connect(lambda _value: schedule_consulta_quick())
        for qf in [self.bqf_norm, self.bqf_desc, self.bqf_ncm, self.bqf_cest]:
            qf.returnPressed.connect(self.apply_aggregation_results_filters)
            qf.textChanged.connect(lambda _value: schedule_agregacao_bottom())

        # --- Consulta SQL tab ---
        self.sql_combo.currentIndexChanged.connect(self._on_sql_selected)
        self.btn_sql_execute.clicked.connect(self._execute_sql_query)
        self.btn_sql_export.clicked.connect(self._export_sql_results)
        self.btn_sql_destacar.clicked.connect(
            lambda: self._destacar_tabela("sql_result")
        )
        self.sql_result_search.returnPressed.connect(self._filter_sql_results)
        self.sql_result_search.textChanged.connect(lambda _value: schedule_sql_search())
        self.btn_sql_prev.clicked.connect(self._sql_prev_page)
        self.btn_sql_next.clicked.connect(self._sql_next_page)

        # --- Conversao tab ---
        self.btn_refresh_conversao.clicked.connect(self.atualizar_aba_conversao)
        self.chk_show_single_unit.stateChanged.connect(
            lambda _state: self.atualizar_aba_conversao()
        )
        self.btn_export_conversao.clicked.connect(self.exportar_conversao_excel)
        self.btn_import_conversao.clicked.connect(self.importar_conversao_excel)
        self.btn_conversao_destacar.clicked.connect(
            lambda: self._destacar_tabela("conversao")
        )
        self.btn_recalcular_fatores.clicked.connect(
            lambda: self.recalcular_derivados_conversao()
        )
        self.btn_apply_conversao_profile.clicked.connect(
            lambda: self._aplicar_perfil_tabela(
                "conversao",
                self.conversion_table,
                self.conversion_model,
                self.conversao_profile.currentText(),
                "conversao",
            )
        )
        self.btn_save_conversao_profile.clicked.connect(
            lambda: self._salvar_perfil_tabela_com_dialogo(
                "conversao",
                self.conversion_table,
                self.conversion_model,
                self.conversao_profile,
                ["Padrao", "Auditoria", "Estoque", "Custos"],
            )
        )
        self.btn_conversao_colunas.clicked.connect(
            lambda: self._abrir_menu_colunas_tabela("conversao", self.conversion_table)
        )
        self.conversion_table.horizontalHeader().customContextMenuRequested.connect(
            lambda pos: self._abrir_menu_colunas_tabela(
                "conversao", self.conversion_table, pos
            )
        )
        self.conversion_table.horizontalHeader().sectionMoved.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "conversao", self.conversion_table, self.conversion_model
            )
        )
        self.conversion_table.horizontalHeader().sectionResized.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "conversao", self.conversion_table, self.conversion_model
            )
        )
        self.conversion_table.horizontalHeader().sortIndicatorChanged.connect(
            lambda _index, _order: self._salvar_preferencias_tabela(
                "conversao", self.conversion_table, self.conversion_model
            )
        )
        self.conv_filter_id.currentTextChanged.connect(lambda _value: schedule_conv())
        self.conv_filter_desc.textChanged.connect(lambda _value: schedule_conv())
        self.conversion_model.dataChanged.connect(self._on_conversion_model_changed)

        self.conversion_table.selectionModel().selectionChanged.connect(
            self._on_conversion_selection_changed
        )
        self.btn_apply_unid_ref.clicked.connect(self._apply_unid_ref_to_all)

    def _abrir_fio_de_ouro(self, id_agrupado: str) -> None:
        if not self.state.current_cnpj:
            return

        pasta_analises = CNPJ_ROOT / self.state.current_cnpj / "analises" / "produtos"
        arquivos = list(
            pasta_analises.glob(f"*_enriquecido_{self.state.current_cnpj}.parquet")
        )
        dfs = []
        filtro_id = [
            FilterCondition(column="id_agrupado", operator="igual", value=id_agrupado)
        ]
        for arq in arquivos:
            try:
                schema = self.parquet_service.get_schema(arq)
                if "id_agrupado" not in schema:
                    continue
                df = self.parquet_service.load_dataset(arq, filtro_id)
                if not df.is_empty():
                    df = df.with_columns(
                        pl.lit(arq.name.split("_enriquecido")[0].upper()).alias(
                            "origem_fio_ouro"
                        )
                    )
                    dfs.append(df)
            except Exception:
                pass

        if not dfs:
            self.show_info(
                "Fio de Ouro",
                f"Nenhum registro enriquecido encontrado para: {id_agrupado}.",
            )
            return

        try:
            df_final = pl.concat(dfs, how="diagonal_relaxed")
            from interface_grafica.ui.dialogs import DialogoFioDeOuro

            dlg = DialogoFioDeOuro(df_final, self)
            dlg.exec()
        except Exception as e:
            self.show_error("Fio de Ouro", f"Erro ao gerar trilha de auditoria: {e}")

    def _copiar_valor_celula(self, table: QTableView, index) -> None:
        if not index or not index.isValid():
            return
        valor = index.data(Qt.DisplayRole)
        QGuiApplication.clipboard().setText("" if valor is None else str(valor))

    def _abrir_menu_contexto_celula(
        self, contexto: str, table: QTableView, pos
    ) -> None:
        index = table.indexAt(pos)
        if not index.isValid():
            return

        menu = QMenu(self)
        acao_copiar = menu.addAction("Copiar valor")
        acao_copiar.triggered.connect(lambda: self._copiar_valor_celula(table, index))

        model = table.model()
        if (
            contexto == "mov_estoque"
            and isinstance(model, PolarsTableModel)
            and not model.get_dataframe().is_empty()
            and "id_agrupado" in model.get_dataframe().columns
        ):
            try:
                id_agrupado = model.get_dataframe()["id_agrupado"][index.row()]
            except Exception:
                id_agrupado = None
            if id_agrupado:
                menu.addSeparator()
                acao = menu.addAction(f"Auditoria 'Fio de Ouro' ({id_agrupado})")
                acao.triggered.connect(lambda: self._abrir_fio_de_ouro(id_agrupado))

        menu.exec(table.viewport().mapToGlobal(pos))

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
            self.mov_estoque_table,
            self.aba_mensal_table,
            self.aba_anual_table,
            self.nfe_entrada_table,
            self.id_agrupados_table,
            self.produtos_sel_table,
        ]
        tables.extend(
            janela.table
            for janela in self._detached_windows.values()
            if janela is not None
        )
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

    def _detached_title(self, contexto: str) -> str:
        cnpj = self.state.current_cnpj or "sem CNPJ"
        mapa = {
            "consulta": f"Consulta - {cnpj}",
            "sql_result": f"Consulta SQL - {cnpj}",
            "agregacao_top": f"Agregacao - Tabela Superior - {cnpj}",
            "agregacao_bottom": f"Agregacao - Tabela Inferior - {cnpj}",
            "conversao": f"Conversao - {cnpj}",
            "mov_estoque": f"Movimentacao de Estoque - {cnpj}",
            "aba_mensal": f"Tabela Mensal - {cnpj}",
            "aba_anual": f"Tabela Anual - {cnpj}",
            "nfe_entrada": f"NFe Entrada - {cnpj}",
            "id_agrupados": f"id_agrupados - {cnpj}",
            "produtos_selecionados": f"Produtos Selecionados - {cnpj}",
        }
        return mapa.get(contexto, f"Tabela Destacada - {cnpj}")

    def _detached_assets(
        self, contexto: str
    ) -> tuple[QTableView | None, PolarsTableModel | None]:
        mapa = {
            "consulta": (self.table_view, self.table_model),
            "sql_result": (self.sql_result_table, self.sql_result_model),
            "agregacao_top": (self.aggregation_table, self.aggregation_table_model),
            "agregacao_bottom": (self.results_table, self.results_table_model),
            "conversao": (self.conversion_table, self.conversion_model),
            "mov_estoque": (self.mov_estoque_table, self.mov_estoque_model),
            "aba_mensal": (self.aba_mensal_table, self.aba_mensal_model),
            "aba_anual": (self.aba_anual_table, self.aba_anual_model),
            "nfe_entrada": (self.nfe_entrada_table, self.nfe_entrada_model),
            "id_agrupados": (self.id_agrupados_table, self.id_agrupados_model),
            "produtos_selecionados": (
                self.produtos_sel_table,
                self.produtos_selecionados_model,
            ),
        }
        return mapa.get(contexto, (None, None))

    def _detached_scope(self, contexto: str) -> str | None:
        if contexto == "consulta":
            return self._consulta_scope()
        return None

    def _marcar_recalculo_conversao_pendente(self, motivo: str | None = None) -> None:
        self._conversion_recalc_pending = True
        if hasattr(self, "btn_recalcular_fatores"):
            self.btn_recalcular_fatores.setEnabled(True)
        mensagem = "Alteracoes em fatores salvas. Recalculo pendente."
        if motivo:
            mensagem += f" {motivo}"
        self.status.showMessage(mensagem)

    def _limpar_recalculo_conversao_pendente(self) -> None:
        self._conversion_recalc_pending = False
        if hasattr(self, "btn_recalcular_fatores"):
            self.btn_recalcular_fatores.setEnabled(False)

    def _on_main_tab_changed(self, current_index: int) -> None:
        if not hasattr(self, "tab_conversao"):
            return

        # Lazy Loading: Ao trocar de aba, carrega os dados referentes ao CNPJ atual
        self._carregar_aba_atual()

        idx_conversao = self.tabs.indexOf(self.tab_conversao)
        if idx_conversao < 0:
            return
        if current_index != idx_conversao and self._conversion_recalc_pending:
            self.recalcular_derivados_conversao(show_popup=False)

    def _on_detached_window_closed(self, contexto: str) -> None:
        self._detached_windows.pop(contexto, None)

    def _destacar_tabela(self, contexto: str) -> None:
        table, source_model = self._detached_assets(contexto)
        if table is None or source_model is None:
            self.show_error(
                "Tabela indisponivel",
                "Nao foi possivel localizar a tabela para destacar.",
            )
            return
        if source_model.dataframe.is_empty():
            self.show_error(
                "Tabela vazia", "Nao ha dados carregados nessa tabela para destacar."
            )
            return

        janela_existente = self._detached_windows.get(contexto)
        if janela_existente is not None:
            janela_existente.show()
            janela_existente.raise_()
            janela_existente.activateWindow()
            return

        janela = DetachedTableWindow(
            self._detached_title(contexto), contexto, source_model, self
        )
        self._atualizar_combo_perfis_tabela(
            janela.profile_combo,
            contexto,
            ["Padrao", "Auditoria", "Estoque", "Custos"],
            scope=self._detached_scope(contexto),
        )
        janela.btn_apply_profile.clicked.connect(
            lambda _checked=False,
            ctx=contexto,
            t=janela.table,
            m=janela.table_model,
            combo=janela.profile_combo: self._aplicar_perfil_tabela(
                ctx, t, m, combo.currentText(), ctx, scope=self._detached_scope(ctx)
            )
        )
        janela.btn_save_profile.clicked.connect(
            lambda _checked=False,
            ctx=contexto,
            t=janela.table,
            m=janela.table_model,
            combo=janela.profile_combo: self._salvar_perfil_tabela_com_dialogo(
                ctx,
                t,
                m,
                combo,
                ["Padrao", "Auditoria", "Estoque", "Custos"],
                scope=self._detached_scope(ctx),
            )
        )
        janela.btn_columns.clicked.connect(
            lambda _checked=False,
            ctx=contexto,
            t=janela.table: self._abrir_menu_colunas_tabela(
                ctx, t, scope=self._detached_scope(ctx)
            )
        )
        janela.closed.connect(self._on_detached_window_closed)
        janela.table.customContextMenuRequested.connect(
            lambda pos, t=janela.table, ctx=contexto: self._abrir_menu_contexto_celula(
                ctx, t, pos
            )
        )
        janela.table.horizontalHeader().customContextMenuRequested.connect(
            lambda pos,
            t=janela.table,
            m=janela.table_model,
            ctx=contexto: self._abrir_menu_colunas_tabela(
                ctx, t, pos, scope=self._detached_scope(ctx)
            )
        )
        janela.table.horizontalHeader().sectionMoved.connect(
            lambda *_args,
            t=janela.table,
            m=janela.table_model,
            ctx=contexto: self._salvar_preferencias_tabela(
                ctx, t, m, scope=self._detached_scope(ctx)
            )
        )
        janela.table.horizontalHeader().sectionResized.connect(
            lambda *_args,
            t=janela.table,
            m=janela.table_model,
            ctx=contexto: self._salvar_preferencias_tabela(
                ctx, t, m, scope=self._detached_scope(ctx)
            )
        )
        janela.table.horizontalHeader().sortIndicatorChanged.connect(
            lambda _index,
            _order,
            t=janela.table,
            m=janela.table_model,
            ctx=contexto: self._salvar_preferencias_tabela(
                ctx, t, m, scope=self._detached_scope(ctx)
            )
        )
        self._aplicar_preferencias_tabela(
            contexto,
            janela.table,
            janela.table_model,
            scope=self._detached_scope(contexto),
        )
        janela.show()
        self._detached_windows[contexto] = janela

    def _destacar_tabela_estoque(self, contexto: str) -> None:
        self._destacar_tabela(contexto)

    # ------------------------------------------------------------------
    # BotAes individuais: Extrair Brutas, Processamento, Apagar
    # ------------------------------------------------------------------
    def _obter_cnpj_valido(self) -> str | None:
        """Obtem CPF/CNPJ valido da input box ou da selecao da lista."""
        texto = self.cnpj_input.text().strip()
        if not texto:
            item = self.cnpj_list.currentItem()
            if item:
                texto = item.text()
        if not texto:
            self.show_error(
                "CPF/CNPJ nao informado", "Digite ou selecione um CPF/CNPJ."
            )
            return None
        try:
            return self.servico_pipeline_funcoes.servico_extracao.sanitizar_cnpj(texto)
        except Exception as exc:
            self.show_error("CPF/CNPJ invalido", str(exc))
            return None

    def _toggle_left_panel(self, checked: bool) -> None:
        if checked:
            self.left_panel_widget.hide()
            self.btn_toggle_panel.setText(">> Mostrar Painel Lateral")
        else:
            self.left_panel_widget.show()
            self.btn_toggle_panel.setText("<< Ocultar Painel Lateral")



    def _atualizar_titulo_aba_mov_estoque(
        self, visiveis: int | None = None, total: int | None = None
    ) -> None:
        if not hasattr(self, "estoque_tabs") or not hasattr(self, "tab_mov_estoque"):
            return
        idx = self.estoque_tabs.indexOf(self.tab_mov_estoque)
        if idx < 0:
            return
        if visiveis is None:
            self.estoque_tabs.setTabText(idx, "Tabela mov_estoque")
            return
        self.estoque_tabs.setTabText(idx, f"Tabela mov_estoque ({visiveis})")

    def _atualizar_titulo_aba_anual(
        self, visiveis: int | None = None, total: int | None = None
    ) -> None:
        if not hasattr(self, "estoque_tabs") or not hasattr(self, "tab_aba_anual"):
            return
        idx = self.estoque_tabs.indexOf(self.tab_aba_anual)
        if idx < 0:
            return
        if visiveis is None:
            self.estoque_tabs.setTabText(idx, "Tabela anual")
            return
        if total is None:
            self.estoque_tabs.setTabText(idx, f"Tabela anual ({visiveis})")
            return
        self.estoque_tabs.setTabText(idx, f"Tabela anual ({visiveis}/{total})")

    def _atualizar_titulo_aba_mensal(
        self, visiveis: int | None = None, total: int | None = None
    ) -> None:
        if not hasattr(self, "estoque_tabs") or not hasattr(self, "tab_aba_mensal"):
            return
        idx = self.estoque_tabs.indexOf(self.tab_aba_mensal)
        if idx < 0:
            return
        if visiveis is None:
            self.estoque_tabs.setTabText(idx, "Tabela mensal")
            return
        if total is None:
            self.estoque_tabs.setTabText(idx, f"Tabela mensal ({visiveis})")
            return
        self.estoque_tabs.setTabText(idx, f"Tabela mensal ({visiveis}/{total})")

    def _atualizar_titulo_aba_produtos_selecionados(
        self, visiveis: int | None = None, total: int | None = None
    ) -> None:
        if not hasattr(self, "estoque_tabs") or not hasattr(
            self, "tab_produtos_selecionados"
        ):
            return
        idx = self.estoque_tabs.indexOf(self.tab_produtos_selecionados)
        if idx < 0:
            return
        if visiveis is None:
            self.estoque_tabs.setTabText(idx, "Produtos selecionados")
            return
        if total is None:
            self.estoque_tabs.setTabText(idx, f"Produtos selecionados ({visiveis})")
            return
        self.estoque_tabs.setTabText(idx, f"Produtos selecionados ({visiveis}/{total})")

    def _atualizar_titulo_aba_id_agrupados(
        self, visiveis: int | None = None, total: int | None = None
    ) -> None:
        if not hasattr(self, "estoque_tabs") or not hasattr(self, "tab_id_agrupados"):
            return
        idx = self.estoque_tabs.indexOf(self.tab_id_agrupados)
        if idx < 0:
            return
        if visiveis is None:
            self.estoque_tabs.setTabText(idx, "id_agrupados")
            return
        if total is None:
            self.estoque_tabs.setTabText(idx, f"id_agrupados ({visiveis})")
            return
        self.estoque_tabs.setTabText(idx, f"id_agrupados ({visiveis}/{total})")

    def _atualizar_titulo_aba_mensal(
        self, visiveis: int | None = None, total: int | None = None
    ) -> None:
        if not hasattr(self, "estoque_tabs") or not hasattr(self, "tab_aba_mensal"):
            return
        idx = self.estoque_tabs.indexOf(self.tab_aba_mensal)
        if idx < 0:
            return
        if visiveis is None or total is None:
            self.estoque_tabs.setTabText(idx, "Tabela mensal")
            return
        self.estoque_tabs.setTabText(idx, f"Tabela mensal ({visiveis}/{total})")

    def _atualizar_titulo_aba_anual(
        self, visiveis: int | None = None, total: int | None = None
    ) -> None:
        if not hasattr(self, "estoque_tabs") or not hasattr(self, "tab_aba_anual"):
            return
        idx = self.estoque_tabs.indexOf(self.tab_aba_anual)
        if idx < 0:
            return
        if visiveis is None or total is None:
            self.estoque_tabs.setTabText(idx, "Tabela anual")
            return
        self.estoque_tabs.setTabText(idx, f"Tabela anual ({visiveis}/{total})")

    def _atualizar_titulo_aba_nfe_entrada(
        self, visiveis: int | None = None, total: int | None = None
    ) -> None:
        if not hasattr(self, "tabs") or not hasattr(self, "tab_nfe_entrada"):
            return
        idx = self.tabs.indexOf(self.tab_nfe_entrada)
        if idx < 0:
            return
        if visiveis is None:
            self.tabs.setTabText(idx, "NFe Entrada")
            return
        if total is None:
            self.tabs.setTabText(idx, f"NFe Entrada ({visiveis})")
            return
        self.tabs.setTabText(idx, f"NFe Entrada ({visiveis}/{total})")

    def _atualizar_titulo_aba_periodos(
        self, visiveis: int | None = None, total: int | None = None
    ) -> None:
        if not hasattr(self, "estoque_tabs") or not hasattr(self, "tab_aba_periodos"):
            return
        idx = self.estoque_tabs.indexOf(self.tab_aba_periodos)
        if idx < 0:
            return
        if visiveis is None:
            self.estoque_tabs.setTabText(idx, "Tabela períodos")
            return
        if total is None:
            self.estoque_tabs.setTabText(idx, f"Tabela períodos ({visiveis})")
            return
        self.estoque_tabs.setTabText(idx, f"Tabela períodos ({visiveis}/{total})")

    def _popular_combo_texto(
        self,
        combo: QComboBox,
        valores: list[str],
        valor_atual: str = "",
        primeiro_item: str = "",
    ) -> None:
        combo.blockSignals(True)
        combo.clear()
        if primeiro_item is not None:
            combo.addItem(primeiro_item)
        combo.addItems([str(v) for v in valores])
        if valor_atual:
            combo.setCurrentText(valor_atual)
        combo.blockSignals(False)

    def _filtrar_texto_em_colunas(self, df: pl.DataFrame, texto: str) -> pl.DataFrame:
        texto = (texto or "").strip().lower()
        if not texto or df.is_empty():
            return df

        colunas_busca = [
            c for c in df.columns if df.schema[c] in [pl.Utf8, pl.Categorical]
        ]
        if not colunas_busca:
            return df

        expr = None
        for col in colunas_busca:
            atual = (
                pl.col(col)
                .cast(pl.Utf8, strict=False)
                .fill_null("")
                .str.to_lowercase()
                .str.contains(texto, literal=True)
            )
            expr = atual if expr is None else (expr | atual)
        return df.filter(expr) if expr is not None else df

    def _valor_qdate_ativo(self, value: QDate) -> QDate | None:
        return None if not value.isValid() or value == QDate(1900, 1, 1) else value

    def _parse_numero_filtro(self, valor: str) -> float | None:
        bruto = (valor or "").strip()
        if not bruto:
            return None
        try:
            return float(bruto.replace(",", "."))
        except Exception:
            return None

    def _filtrar_intervalo_numerico(
        self,
        df: pl.DataFrame,
        coluna: str | None,
        valor_min: str,
        valor_max: str,
    ) -> pl.DataFrame:
        if not coluna or coluna not in df.columns:
            return df

        minimo = self._parse_numero_filtro(valor_min)
        maximo = self._parse_numero_filtro(valor_max)
        if minimo is None and maximo is None:
            return df

        expr_col = pl.col(coluna).cast(pl.Float64, strict=False)
        if minimo is not None:
            df = df.filter(expr_col >= minimo)
        if maximo is not None:
            df = df.filter(expr_col <= maximo)
        return df

    def _filtrar_intervalo_data(
        self,
        df: pl.DataFrame,
        coluna: str | None,
        data_ini: QDate | None,
        data_fim: QDate | None,
    ) -> pl.DataFrame:
        if (
            not coluna
            or coluna not in df.columns
            or (data_ini is None and data_fim is None)
        ):
            return df

        col_data = (
            pl.col(coluna)
            .cast(pl.Utf8, strict=False)
            .fill_null("")
            .str.replace_all(r"[^0-9]", "")
            .str.slice(0, 8)
            .str.strptime(pl.Date, format="%Y%m%d", strict=False)
        )
        if data_ini is not None:
            df = df.filter(col_data >= pl.lit(data_ini.toPython()))
        if data_fim is not None:
            df = df.filter(col_data <= pl.lit(data_fim.toPython()))
        return df

    def _preferencia_tabela_key(self, aba: str, scope: str | None = None) -> str:
        escopo = scope or (self.state.current_cnpj or "__global__")
        return f"preferencias_tabela::{aba}::{escopo}"

    def _consulta_scope(self) -> str:
        arquivo = (
            self.state.current_file.name
            if self.state.current_file
            else "__sem_arquivo__"
        )
        cnpj = self.state.current_cnpj or "__global__"
        return f"{cnpj}::{arquivo}"

    def _carregar_preferencias_tabela(self, aba: str, scope: str | None = None) -> dict:
        prefs = self.selection_service.get_value(
            self._preferencia_tabela_key(aba, scope), {}
        )
        return prefs if isinstance(prefs, dict) else {}

    def _capturar_estado_tabela(
        self, table: QTableView, model: PolarsTableModel
    ) -> dict:
        offset = 1 if getattr(model, "_checkable", False) else 0
        colunas = model.dataframe.columns
        header = table.horizontalHeader()
        visiveis = [
            nome
            for _visual, nome in sorted(
                (
                    (header.visualIndex(idx + offset), nome)
                    for idx, nome in enumerate(colunas)
                    if not table.isColumnHidden(idx + offset)
                ),
                key=lambda item: item[0],
            )
        ]
        estado = {
            "visible_columns": visiveis,
            "column_order": visiveis,
            "header_state": self._serializar_estado_header(table),
        }
        if getattr(model, "_last_sort_column", None):
            estado["sort_column"] = model._last_sort_column
            estado["sort_order"] = (
                "desc" if model._last_sort_order == Qt.DescendingOrder else "asc"
            )
        return estado

    def _aplicar_estado_tabela(
        self, table: QTableView, model: PolarsTableModel, prefs: dict
    ) -> bool:
        if not prefs or model.dataframe.is_empty():
            return False

        aplicado = False
        visiveis = prefs.get("visible_columns")
        if isinstance(visiveis, list) and visiveis:
            self._aplicar_preset_colunas(
                table, model.dataframe.columns, [str(v) for v in visiveis]
            )
            aplicado = True

        sort_column = prefs.get("sort_column")
        sort_order = (
            Qt.DescendingOrder
            if prefs.get("sort_order") == "desc"
            else Qt.AscendingOrder
        )
        if isinstance(sort_column, str) and sort_column in model.dataframe.columns:
            idx = model.dataframe.columns.index(sort_column) + (
                1 if getattr(model, "_checkable", False) else 0
            )
            model.sort(idx, sort_order)
            table.sortByColumn(idx, sort_order)
            aplicado = True

        header_state = prefs.get("header_state")
        if isinstance(header_state, str) and header_state:
            aplicado = self._restaurar_estado_header(table, header_state) or aplicado
        return aplicado

    def _colunas_estado_perfil(
        self, prefs: dict, model: PolarsTableModel
    ) -> list[str] | None:
        if not isinstance(prefs, dict) or model.dataframe.is_empty():
            return None

        raw = prefs.get("visible_columns")
        if not isinstance(raw, list) or not raw:
            return None

        visiveis = ordenar_colunas_perfil(
            list(model.dataframe.columns),
            raw,
            (
                prefs.get("column_order")
                if isinstance(prefs.get("column_order"), list)
                else None
            ),
        )
        if not visiveis:
            return None

        header_state = prefs.get("header_state")
        if not isinstance(header_state, str) or not header_state:
            return visiveis

        probe = QTableView()
        try:
            probe.setModel(model)
            if not self._restaurar_estado_header(probe, header_state):
                return visiveis

            offset = 1 if getattr(model, "_checkable", False) else 0
            ordem = [
                nome
                for _visual, nome in sorted(
                    (
                        (probe.horizontalHeader().visualIndex(idx + offset), nome)
                        for idx, nome in enumerate(model.dataframe.columns)
                        if nome in visiveis
                    ),
                    key=lambda item: item[0],
                )
            ]
            return ordenar_colunas_perfil(
                list(model.dataframe.columns), visiveis, ordem
            )
        finally:
            probe.setModel(None)

    def _nomes_perfis_nomeados_tabela(
        self, aba: str, scope: str | None = None
    ) -> list[str]:
        prefs = self._carregar_preferencias_tabela(aba, scope)
        perfis = prefs.get("named_profiles", {})
        if not isinstance(perfis, dict):
            return []
        return sorted(
            [str(nome) for nome in perfis.keys() if str(nome).strip()],
            key=lambda v: v.lower(),
        )

    def _obter_estado_perfil_nomeado(
        self, aba: str, perfil: str, scope: str | None = None
    ) -> dict | None:
        prefs = self._carregar_preferencias_tabela(aba, scope)
        perfis = prefs.get("named_profiles", {})
        if not isinstance(perfis, dict):
            return None
        estado = perfis.get(perfil)
        return estado if isinstance(estado, dict) else None

    def _atualizar_combo_perfis_tabela(
        self,
        combo: QComboBox,
        aba: str,
        presets: list[str],
        scope: str | None = None,
    ) -> None:
        atual = combo.currentText().strip()
        nomes = presets + [
            n
            for n in self._nomes_perfis_nomeados_tabela(aba, scope)
            if n not in presets
        ]
        combo.blockSignals(True)
        combo.clear()
        combo.addItems(nomes)
        if atual and atual in nomes:
            combo.setCurrentText(atual)
        elif nomes:
            combo.setCurrentIndex(0)
        combo.blockSignals(False)

    def _salvar_perfil_nomeado_tabela(
        self,
        aba: str,
        table: QTableView,
        model: PolarsTableModel,
        nome: str,
        scope: str | None = None,
    ) -> None:
        if model.dataframe.is_empty():
            return
        nome_limpo = nome.strip()
        if not nome_limpo:
            return
        prefs = self._carregar_preferencias_tabela(aba, scope)
        perfis = prefs.get("named_profiles", {})
        if not isinstance(perfis, dict):
            perfis = {}
        perfis[nome_limpo] = self._capturar_estado_tabela(table, model)
        prefs["named_profiles"] = perfis
        self.selection_service.set_value(
            self._preferencia_tabela_key(aba, scope), prefs
        )

    def _serializar_estado_header(self, table: QTableView) -> str:
        estado = bytes(table.horizontalHeader().saveState())
        return base64.b64encode(estado).decode("ascii")

    def _restaurar_estado_header(self, table: QTableView, valor: str) -> bool:
        try:
            bruto = base64.b64decode(valor.encode("ascii"))
            return bool(table.horizontalHeader().restoreState(QByteArray(bruto)))
        except Exception:
            return False

    def _salvar_preferencias_tabela(
        self,
        aba: str,
        table: QTableView,
        model: PolarsTableModel,
        scope: str | None = None,
    ) -> None:
        if model.dataframe.is_empty():
            return
        prefs = self._carregar_preferencias_tabela(aba, scope)
        prefs.update(self._capturar_estado_tabela(table, model))
        self.selection_service.set_value(
            self._preferencia_tabela_key(aba, scope), prefs
        )

    def _aplicar_preferencias_tabela(
        self,
        aba: str,
        table: QTableView,
        model: PolarsTableModel,
        scope: str | None = None,
    ) -> bool:
        prefs = self._carregar_preferencias_tabela(aba, scope)
        return self._aplicar_estado_tabela(table, model, prefs)

    def _obter_colunas_preset_perfil(
        self, perfil: str, colunas: list[str], contexto: str
    ) -> list[str]:
        nome = (perfil or "").strip().lower()
        if contexto == "mov_estoque":
            mapa = {
                "padrao": [
                    "ordem_operacoes",
                    "Tipo_operacao",
                    "fonte",
                    "id_agrupado",
                    "descr_padrao",
                    "Descr_item",
                    "Descr_compl",
                    "Cod_item",
                    "Cod_barra",
                    "Ncm",
                    "Cest",
                    "Tipo_item",
                    "Chv_nfe",
                    "mod",
                    "Ser",
                    "num_nfe",
                    "Num_item",
                    "Dt_doc",
                    "Dt_e_s",
                    "nsu",
                    "finnfe",
                    "infprot_cstat",
                    "co_uf_emit",
                    "co_uf_dest",
                    "Cfop",
                    "Cst",
                    "Aliq_icms",
                    "Vl_bc_icms",
                    "Vl_icms",
                    "vl_bc_icms_st",
                    "vl_icms_st",
                    "aliq_st",
                    "Qtd",
                    "q_conv",
                    "Unid",
                    "unid_ref",
                    "fator",
                    "Vl_item",
                    "preco_item",
                    "preco_unit",
                    "custo_medio_anual",
                    "saldo_estoque_anual",
                    "entr_desac_anual",
                    "mov_rep",
                    "excluir_estoque",
                    "dev_simples",
                    "dev_venda",
                    "dev_compra",
                    "dev_ent_simples",
                    "ncm_padrao",
                    "cest_padrao",
                    "co_sefin_agr",
                    "it_pc_interna",
                    "it_in_st",
                    "it_pc_mva",
                    "it_in_mva_ajustado",
                    "it_in_isento_icms",
                    "it_in_reducao",
                    "it_pc_reducao",
                    "it_in_combustivel",
                    "it_in_pmpf",
                    "it_in_reducao_credito",
                ],
                "exportar": [
                    "ordem_operacoes",
                    "Tipo_operacao",
                    "fonte",
                    "id_agrupado",
                    "descr_padrao",
                    "Descr_item",
                    "Dt_doc",
                    "Dt_e_s",
                    "Cfop",
                    "Qtd",
                    "q_conv",
                    "saldo_estoque_anual",
                    "entr_desac_anual",
                    "custo_medio_anual",
                    "preco_item",
                    "preco_unit",
                    "unid_ref",
                    "fator",
                    "mov_rep",
                    "dev_simples",
                    "excluir_estoque",
                ],
                "contribuinte": [
                    "ordem_operacoes",
                    "Tipo_operacao",
                    "Dt_doc",
                    "id_agrupado",
                    "descr_padrao",
                    "Qtd",
                    "q_conv",
                    "unid_ref",
                    "preco_item",
                    "preco_unit",
                    "saldo_estoque_anual",
                    "entr_desac_anual",
                ],
                "auditoria": [
                    "ordem_operacoes",
                    "Tipo_operacao",
                    "fonte",
                    "id_agrupado",
                    "descr_padrao",
                    "Dt_doc",
                    "Dt_e_s",
                    "Cfop",
                    "q_conv",
                    "saldo_estoque_anual",
                    "entr_desac_anual",
                    "mov_rep",
                    "dev_simples",
                    "excluir_estoque",
                ],
                "auditoria fiscal": [
                    "ordem_operacoes",
                    "Tipo_operacao",
                    "fonte",
                    "id_agrupado",
                    "descr_padrao",
                    "Descr_item",
                    "Descr_compl",
                    "Cod_item",
                    "Cod_barra",
                    "Ncm",
                    "Cest",
                    "Tipo_item",
                    "Chv_nfe",
                    "mod",
                    "Ser",
                    "num_nfe",
                    "Num_item",
                    "Dt_doc",
                    "Dt_e_s",
                    "nsu",
                    "finnfe",
                    "infprot_cstat",
                    "co_uf_emit",
                    "co_uf_dest",
                    "Cfop",
                    "Cst",
                    "Aliq_icms",
                    "Vl_bc_icms",
                    "Vl_icms",
                    "vl_bc_icms_st",
                    "vl_icms_st",
                    "aliq_st",
                    "Qtd",
                    "q_conv",
                    "Unid",
                    "unid_ref",
                    "fator",
                    "Vl_item",
                    "preco_item",
                    "preco_unit",
                    "custo_medio_anual",
                    "saldo_estoque_anual",
                    "entr_desac_anual",
                    "mov_rep",
                    "excluir_estoque",
                    "dev_simples",
                    "dev_venda",
                    "dev_compra",
                    "dev_ent_simples",
                    "co_sefin_agr",
                    "it_pc_interna",
                    "it_in_st",
                    "it_pc_mva",
                    "it_in_mva_ajustado",
                    "it_in_isento_icms",
                    "it_in_reducao",
                    "it_pc_reducao",
                    "it_in_combustivel",
                    "it_in_pmpf",
                    "it_in_reducao_credito",
                ],
                "estoque": [
                    "ordem_operacoes",
                    "Tipo_operacao",
                    "id_agrupado",
                    "descr_padrao",
                    "Dt_doc",
                    "q_conv",
                    "saldo_estoque_anual",
                    "unid_ref",
                    "fator",
                ],
                "custos": [
                    "ordem_operacoes",
                    "Tipo_operacao",
                    "id_agrupado",
                    "descr_padrao",
                    "Dt_doc",
                    "q_conv",
                    "preco_item",
                    "preco_unit",
                    "custo_medio_anual",
                    "saldo_estoque_anual",
                ],
            }
        elif contexto in {"agregacao_top", "agregacao_bottom"}:
            mapa = {
                "padrao": [
                    "id_agrupado",
                    "descr_padrao",
                    "ids_origem_agrupamento",
                    "preco_medio_compra",
                    "preco_medio_venda",
                    "total_entradas",
                    "total_saidas",
                    "total_movimentacao",
                    "total_compras",
                    "qtd_compras_total",
                    "total_vendas",
                    "qtd_vendas_total",
                    "ncm_padrao",
                    "cest_padrao",
                    "gtin_padrao",
                    "lista_itens_agrupados",
                    "lista_ncm",
                    "lista_cest",
                    "lista_gtin",
                    "lista_descricoes",
                    "lista_desc_compl",
                    "co_sefin_padrao",
                    "co_sefin_agr",
                    "lista_unidades",
                    "fontes",
                ],
                "auditoria": [
                    "id_agrupado",
                    "descr_padrao",
                    "ids_origem_agrupamento",
                    "lista_itens_agrupados",
                    "ncm_padrao",
                    "cest_padrao",
                    "gtin_padrao",
                    "lista_ncm",
                    "lista_cest",
                    "lista_gtin",
                    "lista_descricoes",
                    "lista_desc_compl",
                    "co_sefin_padrao",
                    "co_sefin_agr",
                    "lista_co_sefin",
                    "co_sefin_divergentes",
                    "lista_unidades",
                    "fontes",
                    "total_entradas",
                    "total_saidas",
                    "total_movimentacao",
                    "total_compras",
                    "qtd_compras_total",
                    "preco_medio_compra",
                    "total_vendas",
                    "qtd_vendas_total",
                    "preco_medio_venda",
                    "lista_chave_produto",
                ],
                "estoque": [
                    "id_agrupado",
                    "descr_padrao",
                    "ids_origem_agrupamento",
                    "total_entradas",
                    "total_saidas",
                    "total_movimentacao",
                    "total_compras",
                    "qtd_compras_total",
                    "total_vendas",
                    "qtd_vendas_total",
                    "lista_unidades",
                    "lista_descricoes",
                    "lista_desc_compl",
                    "lista_itens_agrupados",
                    "fontes",
                    "ncm_padrao",
                    "cest_padrao",
                ],
                "custos": [
                    "id_agrupado",
                    "descr_padrao",
                    "ids_origem_agrupamento",
                    "preco_medio_compra",
                    "preco_medio_venda",
                    "total_entradas",
                    "total_saidas",
                    "total_movimentacao",
                    "total_compras",
                    "qtd_compras_total",
                    "total_vendas",
                    "qtd_vendas_total",
                    "lista_ncm",
                    "lista_cest",
                    "lista_gtin",
                    "lista_descricoes",
                    "lista_desc_compl",
                    "lista_itens_agrupados",
                    "lista_unidades",
                    "fontes",
                ],
            }
        elif nome in {"", "padrao"}:
            return colunas
        elif contexto == "conversao":
            mapa = {
                "auditoria": [
                    "id_agrupado",
                    "id_produtos",
                    "descr_padrao",
                    "lista_descricoes_produto",
                    "unid",
                    "unid_ref",
                    "fator",
                    "fator_calculado",
                    "preco_medio",
                    "preco_medio_ref",
                    "origem_preco",
                ],
                "estoque": [
                    "id_agrupado",
                    "descr_padrao",
                    "unid",
                    "unid_ref",
                    "fator",
                    "fator_calculado",
                ],
                "custos": [
                    "id_agrupado",
                    "descr_padrao",
                    "unid",
                    "unid_ref",
                    "preco_medio",
                    "preco_medio_ref",
                    "fator_calculado",
                    "fator",
                    "origem_preco",
                ],
            }
        elif contexto == "aba_mensal":
            mapa = {
                "exportar": [
                    "ano",
                    "mes",
                    "id_agregado",
                    "descr_padrao",
                    "ST",
                    "it_in_st",
                    "valor_entradas",
                    "qtd_entradas",
                    "pme_mes",
                    "valor_saidas",
                    "qtd_saidas",
                    "pms_mes",
                    "MVA",
                    "MVA_ajustado",
                    "entradas_desacob",
                    "ICMS_entr_desacob",
                    "saldo_mes",
                    "custo_medio_mes",
                    "valor_estoque",
                ],
                "auditoria": [
                    "ano",
                    "mes",
                    "id_agregado",
                    "descr_padrao",
                    "ST",
                    "it_in_st",
                    "valor_entradas",
                    "qtd_entradas",
                    "valor_saidas",
                    "qtd_saidas",
                    "entradas_desacob",
                    "ICMS_entr_desacob",
                    "saldo_mes",
                    "custo_medio_mes",
                    "valor_estoque",
                ],
                "estoque": [
                    "ano",
                    "mes",
                    "id_agregado",
                    "descr_padrao",
                    "unids_ref_mes",
                    "qtd_entradas",
                    "qtd_saidas",
                    "saldo_mes",
                    "custo_medio_mes",
                    "valor_estoque",
                ],
                "custos": [
                    "ano",
                    "mes",
                    "id_agregado",
                    "descr_padrao",
                    "valor_entradas",
                    "pme_mes",
                    "valor_saidas",
                    "pms_mes",
                    "custo_medio_mes",
                    "valor_estoque",
                ],
            }
        elif contexto == "aba_anual":
            mapa = {
                "exportar": [
                    "ano",
                    "id_agregado",
                    "descr_padrao",
                    "unid_ref",
                    "estoque_inicial",
                    "entradas",
                    "saidas",
                    "estoque_final",
                    "saidas_calculadas",
                    "saldo_final",
                    "entradas_desacob",
                    "saidas_desacob",
                    "estoque_final_desacob",
                    "pme",
                    "pms",
                    "ICMS_saidas_desac",
                    "ICMS_estoque_desac",
                ],
                "auditoria": [
                    "ano",
                    "id_agregado",
                    "descr_padrao",
                    "unid_ref",
                    "estoque_inicial",
                    "entradas",
                    "saidas",
                    "estoque_final",
                    "saldo_final",
                    "entradas_desacob",
                    "saidas_desacob",
                    "estoque_final_desacob",
                    "ICMS_saidas_desac",
                ],
                "estoque": [
                    "ano",
                    "id_agregado",
                    "descr_padrao",
                    "unid_ref",
                    "estoque_inicial",
                    "entradas",
                    "saidas",
                    "estoque_final",
                    "saldo_final",
                ],
                "custos": [
                    "ano",
                    "id_agregado",
                    "descr_padrao",
                    "unid_ref",
                    "entradas",
                    "saidas",
                    "pme",
                    "pms",
                    "ICMS_saidas_desac",
                ],
            }
        elif contexto == "nfe_entrada":
            mapa = {
                "auditoria": [
                    "data_classificacao",
                    "fonte_documento",
                    "tipo_operacao",
                    "nnf",
                    "prod_nitem",
                    "id_agrupado",
                    "descr_padrao",
                    "prod_xprod",
                    "prod_ncm",
                    "prod_cest",
                    "co_sefin_inferido",
                    "co_sefin_agr",
                    "it_pc_interna",
                    "it_in_st",
                    "it_in_isento_icms",
                    "it_in_reducao",
                    "it_pc_reducao",
                    "it_pc_mva",
                    "it_in_mva_ajustado",
                    "xnome_emit",
                    "xnome_dest",
                    "chave_acesso",
                ],
                "estoque": [
                    "data_classificacao",
                    "fonte_documento",
                    "id_agrupado",
                    "descr_padrao",
                    "prod_xprod",
                    "prod_ncm",
                    "prod_cest",
                    "co_sefin_agr",
                    "it_pc_interna",
                    "it_in_st",
                    "it_in_isento_icms",
                    "it_in_reducao",
                    "prod_ucom",
                    "prod_qcom",
                    "prod_vprod",
                ],
                "custos": [
                    "data_classificacao",
                    "fonte_documento",
                    "id_agrupado",
                    "descr_padrao",
                    "prod_xprod",
                    "co_sefin_agr",
                    "it_pc_interna",
                    "it_in_st",
                    "it_in_isento_icms",
                    "it_in_reducao",
                    "it_pc_reducao",
                    "it_pc_mva",
                    "it_in_mva_ajustado",
                    "prod_vuncom",
                    "prod_vprod",
                ],
            }
        elif contexto == "produtos_selecionados":
            mapa = {
                "auditoria": [
                    "id_agregado",
                    "descr_padrao",
                    "total_ICMS_entr_desacob",
                    "total_ICMS_saidas_desac",
                    "total_ICMS_estoque_desac",
                    "total_ICMS_total",
                ],
                "estoque": [
                    "id_agregado",
                    "descr_padrao",
                    "total_ICMS_entr_desacob",
                    "total_ICMS_estoque_desac",
                    "total_ICMS_total",
                ],
                "custos": [
                    "id_agregado",
                    "descr_padrao",
                    "total_ICMS_entr_desacob",
                    "total_ICMS_saidas_desac",
                    "total_ICMS_estoque_desac",
                    "total_ICMS_total",
                ],
            }
        elif contexto == "id_agrupados":
            mapa = {
                "auditoria": [
                    "id_agrupado",
                    "descr_padrao",
                    "lista_descricoes",
                    "lista_desc_compl",
                    "lista_codigos",
                    "lista_unidades",
                ],
                "estoque": [
                    "id_agrupado",
                    "descr_padrao",
                    "lista_unidades",
                    "lista_descricoes",
                    "lista_desc_compl",
                ],
                "custos": [
                    "id_agrupado",
                    "descr_padrao",
                    "lista_codigos",
                    "lista_unidades",
                ],
            }
        else:
            mapa = {
                "auditoria": [
                    "cnpj",
                    "periodo",
                    "id_agrupado",
                    "id_agregado",
                    "descr_padrao",
                    "descricao",
                    "descr",
                    "Descr_item",
                    "Ncm",
                    "ncm_padrao",
                    "cest_padrao",
                    "Cfop",
                    "valor_item",
                    "preco_item",
                    "q_conv",
                    "Qtd",
                    "total_compras",
                    "qtd_compras_total",
                    "preco_medio_compra",
                    "total_vendas",
                    "qtd_vendas_total",
                    "preco_medio_venda",
                    "saldo_final",
                    "entradas_desacob",
                ],
                "estoque": [
                    "id_agrupado",
                    "id_agregado",
                    "descr_padrao",
                    "descricao",
                    "Descr_item",
                    "ncm_padrao",
                    "Ncm",
                    "unid_ref",
                    "q_conv",
                    "Qtd",
                    "total_entradas",
                    "total_saidas",
                    "total_movimentacao",
                    "total_compras",
                    "total_vendas",
                    "saldo_final",
                    "estoque_final",
                ],
                "custos": [
                    "id_agrupado",
                    "id_agregado",
                    "descr_padrao",
                    "descricao",
                    "Descr_item",
                    "total_entradas",
                    "total_saidas",
                    "total_movimentacao",
                    "total_compras",
                    "qtd_compras_total",
                    "preco_medio_compra",
                    "total_vendas",
                    "qtd_vendas_total",
                    "preco_medio_venda",
                    "preco_item",
                    "preco_unit",
                    "valor_item",
                    "q_conv",
                    "Qtd",
                    "pme",
                    "pms",
                    "custo_medio_anual",
                ],
            }

        desejadas = mapa.get(nome, colunas)
        selecionadas = [c for c in desejadas if c in colunas]
        return selecionadas or colunas

    def _aplicar_layout_padrao_agregacao(
        self,
        contexto: str,
        table: QTableView,
        model: PolarsTableModel,
        perfil: str,
    ) -> None:
        if (
            contexto not in {"agregacao_top", "agregacao_bottom"}
            or model.dataframe.is_empty()
        ):
            return
        ordem = self._obter_colunas_preset_perfil(
            perfil, model.dataframe.columns, contexto
        )
        self._aplicar_ordem_colunas(table, ordem)
        colunas = model.dataframe.columns
        offset = 1 if getattr(model, "_checkable", False) else 0
        larguras = {
            "id_agrupado": 150,
            "descr_padrao": 320,
            "ids_origem_agrupamento": 180,
            "preco_medio_compra": 150,
            "preco_medio_venda": 150,
            "total_entradas": 145,
            "total_saidas": 145,
            "total_movimentacao": 155,
            "total_compras": 140,
            "qtd_compras_total": 140,
            "total_vendas": 140,
            "qtd_vendas_total": 140,
            "lista_ncm": 180,
            "lista_cest": 180,
            "lista_gtin": 180,
            "lista_descricoes": 340,
            "lista_desc_compl": 320,
            "lista_itens_agrupados": 340,
        }
        for nome, largura in larguras.items():
            if nome in colunas:
                table.setColumnWidth(colunas.index(nome) + offset, largura)

    def _abrir_menu_colunas_tabela(
        self, aba: str, table: QTableView, pos=None, scope: str | None = None
    ) -> None:
        model = table.model()
        if not isinstance(model, PolarsTableModel) or model.dataframe.is_empty():
            return
        offset = 1 if getattr(model, "_checkable", False) else 0
        header = table.horizontalHeader()
        colunas = [
            nome
            for _visual, nome in sorted(
                (
                    (header.visualIndex(idx + offset), nome)
                    for idx, nome in enumerate(model.dataframe.columns)
                ),
                key=lambda item: item[0],
            )
        ]
        visiveis = [
            nome
            for nome in colunas
            if nome in model.dataframe.columns
            and not table.isColumnHidden(model.dataframe.columns.index(nome) + offset)
        ]
        dialog = ColumnSelectorDialog(colunas, visiveis, self)
        if not dialog.exec():
            return
        selecionadas = dialog.selected_columns()
        if not selecionadas:
            self.show_error(
                "Selecao invalida", "Pelo menos uma coluna deve permanecer visivel."
            )
            return
        self._aplicar_ordem_colunas(table, dialog.column_order())
        self._aplicar_preset_colunas(table, colunas, selecionadas)
        self._salvar_preferencias_tabela(aba, table, model, scope)

    def _aplicar_perfil_tabela(
        self,
        aba: str,
        table: QTableView,
        model: PolarsTableModel,
        perfil: str,
        contexto: str,
        scope: str | None = None,
    ) -> None:
        if model.dataframe.is_empty():
            return
        perfil_salvo = self._obter_estado_perfil_nomeado(aba, perfil, scope)
        if perfil_salvo is not None:
            self._aplicar_estado_tabela(table, model, perfil_salvo)
            self._salvar_preferencias_tabela(aba, table, model, scope)
            return
        visiveis = self._obter_colunas_preset_perfil(
            perfil, model.dataframe.columns, contexto
        )
        self._aplicar_preset_colunas(table, model.dataframe.columns, visiveis)
        self._aplicar_layout_padrao_agregacao(contexto, table, model, perfil)
        self._salvar_preferencias_tabela(aba, table, model, scope)

    def _salvar_perfil_tabela_com_dialogo(
        self,
        aba: str,
        table: QTableView,
        model: PolarsTableModel,
        combo: QComboBox,
        presets: list[str],
        scope: str | None = None,
    ) -> None:
        if model.dataframe.is_empty():
            self.show_info(
                "Salvar perfil", "Nao ha dados carregados para salvar um perfil."
            )
            return
        nome, ok = QInputDialog.getText(self, "Salvar perfil", "Nome do perfil:")
        nome = (nome or "").strip()
        if not ok or not nome:
            return
        if nome.lower() in {p.lower() for p in presets} and nome.lower() != "exportar":
            self.show_error(
                "Nome invalido", "Escolha um nome diferente dos perfis padrao."
            )
            return
        self._salvar_perfil_nomeado_tabela(aba, table, model, nome, scope)
        self._atualizar_combo_perfis_tabela(combo, aba, presets, scope)
        combo.setCurrentText(nome)

    def _aplicar_ordenacao_padrao(
        self,
        table: QTableView,
        model: PolarsTableModel,
        colunas_prioritarias: list[str],
        order: Qt.SortOrder = Qt.AscendingOrder,
    ) -> None:
        if model.dataframe.is_empty():
            return

        colunas = model.dataframe.columns
        deslocamento = 1 if getattr(model, "_checkable", False) else 0
        for nome in colunas_prioritarias:
            if nome not in colunas:
                continue
            idx = colunas.index(nome) + deslocamento
            model.sort(idx, order)
            table.sortByColumn(idx, order)
            return

    def _aplicar_preset_colunas(
        self, table: QTableView, colunas: list[str], visiveis: list[str]
    ) -> None:
        visiveis_set = set(visiveis)
        model = table.model()
        if not isinstance(model, PolarsTableModel):
            return
        offset = 1 if getattr(model, "_checkable", False) else 0
        colunas_modelo = list(model.dataframe.columns)
        for idx, nome in enumerate(colunas_modelo):
            table.setColumnHidden(idx + offset, nome not in visiveis_set)

    def _aplicar_ordem_colunas(
        self, table: QTableView, ordem_colunas: list[str]
    ) -> None:
        model = table.model()
        if not isinstance(model, PolarsTableModel) or model.dataframe.is_empty():
            return
        header = table.horizontalHeader()
        offset = 1 if getattr(model, "_checkable", False) else 0
        for idx, nome in enumerate(ordem_colunas):
            if nome not in model.dataframe.columns:
                continue
            logical_index = model.dataframe.columns.index(nome) + offset
            visual_atual = header.visualIndex(logical_index)
            visual_destino = idx + offset
            if visual_atual != visual_destino:
                header.moveSection(visual_atual, visual_destino)

    def _dataframe_colunas_visiveis(
        self, table: QTableView, model: PolarsTableModel, df: pl.DataFrame | None = None
    ) -> pl.DataFrame:
        base_df = df if df is not None else model.dataframe
        if base_df.is_empty():
            return base_df
        offset = 1 if getattr(model, "_checkable", False) else 0
        colunas_modelo = list(model.dataframe.columns)
        header = table.horizontalHeader()
        visiveis = [
            nome
            for idx, nome in enumerate(colunas_modelo)
            if not table.isColumnHidden(idx + offset)
        ]
        ordem_visual = [
            nome
            for _visual, nome in sorted(
                (
                    (header.visualIndex(idx + offset), nome)
                    for idx, nome in enumerate(colunas_modelo)
                ),
                key=lambda item: item[0],
            )
        ]
        visiveis = ordenar_colunas_visiveis(
            list(base_df.columns), visiveis, ordem_visual
        )
        return base_df.select(visiveis) if visiveis else base_df

    def _dataframe_colunas_perfil(
        self,
        aba: str,
        contexto: str,
        model: PolarsTableModel,
        df: pl.DataFrame | None = None,
        perfil: str = "Exportar",
        scope: str | None = None,
    ) -> pl.DataFrame:
        base_df = df if df is not None else model.dataframe
        if base_df.is_empty():
            return base_df

        estado_perfil = self._obter_estado_perfil_nomeado(aba, perfil, scope)
        visiveis = self._colunas_estado_perfil(estado_perfil, model)

        if not visiveis:
            visiveis = self._obter_colunas_preset_perfil(
                perfil, list(base_df.columns), contexto
            )
            visiveis = [col for col in visiveis if col in base_df.columns]

        return base_df.select(visiveis) if visiveis else base_df

    def _refresh_profile_combos(self) -> None:
        combos = [
            (
                self.consulta_profile,
                "consulta",
                ["Padrao", "Auditoria", "Estoque", "Custos"],
                self._consulta_scope(),
            ),
            (
                self.top_profile,
                "agregacao_top",
                ["Padrao", "Auditoria", "Estoque", "Custos"],
                None,
            ),
            (
                self.bottom_profile,
                "agregacao_bottom",
                ["Padrao", "Auditoria", "Estoque", "Custos"],
                None,
            ),
            (
                self.conversao_profile,
                "conversao",
                ["Padrao", "Auditoria", "Estoque", "Custos"],
                None,
            ),
            (
                self.mov_profile,
                "mov_estoque",
                [
                    "Exportar",
                    "Padrao",
                    "Contribuinte",
                    "Auditoria",
                    "Auditoria Fiscal",
                    "Estoque",
                    "Custos",
                ],
                None,
            ),
            (
                self.mensal_profile,
                "aba_mensal",
                ["Exportar", "Padrao", "Auditoria", "Estoque", "Custos"],
                None,
            ),
            (
                self.anual_profile,
                "aba_anual",
                ["Exportar", "Padrao", "Auditoria", "Estoque", "Custos"],
                None,
            ),
            (
                self.produtos_sel_profile,
                "produtos_selecionados",
                ["Padrao", "Auditoria", "Estoque", "Custos"],
                None,
            ),
            (
                self.nfe_entrada_profile,
                "nfe_entrada",
                ["Padrao", "Auditoria", "Estoque", "Custos"],
                None,
            ),
            (
                self.id_agrupados_profile,
                "id_agrupados",
                ["Padrao", "Auditoria", "Estoque", "Custos"],
                None,
            ),
        ]
        for combo, aba, presets, scope in combos:
            if combo is not None:
                self._atualizar_combo_perfis_tabela(combo, aba, presets, scope)

    def _aplicar_preset_mov_estoque(self) -> None:
        if self.mov_estoque_model.dataframe.is_empty():
            return
        visiveis = self._obter_colunas_preset_perfil(
            "exportar", self.mov_estoque_model.dataframe.columns, "mov_estoque"
        )
        self._aplicar_preset_colunas(
            self.mov_estoque_table, self.mov_estoque_model.dataframe.columns, visiveis
        )
        colunas = self.mov_estoque_model.dataframe.columns
        for nome, largura in {
            "descr_padrao": 320,
            "Descr_item": 320,
            "Tipo_operacao": 170,
            "id_agrupado": 140,
        }.items():
            if nome in colunas:
                self.mov_estoque_table.setColumnWidth(colunas.index(nome), largura)

    def _aplicar_preset_aba_anual(self) -> None:
        if self.aba_anual_model.dataframe.is_empty():
            return
        visiveis = self._obter_colunas_preset_perfil(
            "exportar", self.aba_anual_model.dataframe.columns, "aba_anual"
        )
        self._aplicar_preset_colunas(
            self.aba_anual_table, self.aba_anual_model.dataframe.columns, visiveis
        )
        colunas = self.aba_anual_model.dataframe.columns
        offset = 1 if getattr(self.aba_anual_model, "_checkable", False) else 0
        for nome, largura in {"descr_padrao": 320, "id_agregado": 140}.items():
            if nome in colunas:
                self.aba_anual_table.setColumnWidth(
                    colunas.index(nome) + offset, largura
                )

    def _aplicar_preset_aba_mensal(self) -> None:
        if self.aba_mensal_model.dataframe.is_empty():
            return
        visiveis = self._obter_colunas_preset_perfil(
            "exportar", self.aba_mensal_model.dataframe.columns, "aba_mensal"
        )
        self._aplicar_preset_colunas(
            self.aba_mensal_table, self.aba_mensal_model.dataframe.columns, visiveis
        )
        colunas = self.aba_mensal_model.dataframe.columns
        offset = 1 if getattr(self.aba_mensal_model, "_checkable", False) else 0
        for nome, largura in {
            "descr_padrao": 320,
            "id_agregado": 150,
            "unids_mes": 180,
            "unids_ref_mes": 180,
        }.items():
            if nome in colunas:
                self.aba_mensal_table.setColumnWidth(
                    colunas.index(nome) + offset, largura
                )

    def _aplicar_perfil_consulta(self) -> None:
        if self.table_model.dataframe.is_empty():
            return
        self._aplicar_perfil_tabela(
            "consulta",
            self.table_view,
            self.table_model,
            self.consulta_profile.currentText(),
            "consulta",
            self._consulta_scope(),
        )

    def _aplicar_perfil_agregacao(
        self,
        aba: str,
        table: QTableView,
        model: PolarsTableModel,
        perfil: str,
    ) -> None:
        if model.dataframe.is_empty():
            return
        self._aplicar_perfil_tabela(aba, table, model, perfil, aba)

    def _carregar_dataset_ui(
        self,
        path: Path,
        conditions: list[FilterCondition] | None = None,
        columns: list[str] | None = None,
    ) -> pl.DataFrame:
        colunas_solicitadas = columns
        if columns is not None:
            schema = set(self.parquet_service.get_schema(path))
            colunas_solicitadas = [coluna for coluna in columns if coluna in schema]
            if not colunas_solicitadas:
                return pl.DataFrame()
        return self.parquet_service.load_dataset(
            path, conditions or [], colunas_solicitadas
        )

    def _carregar_dados_parquet_async(
        self,
        path: Path,
        callback: Callable,
        status_msg: str = "",
        unique_cols: list[str] | None = None,
    ) -> None:
        """
        Carrega um arquivo Parquet em background.
        Se unique_cols for fornecido, extrai valores únicos dessas colunas no background.
        O callback será chamado como callback(df) ou callback(df, uniques_dict).
        """
        if status_msg:
            self.status.showMessage(f"⏳ {status_msg}...")

        def _worker_load():
            if not path.exists():
                return None
            df = pl.read_parquet(path)
            if not unique_cols:
                return df

            uniques = {}
            for col in unique_cols:
                if col in df.columns:
                    # Extração pesada feita no worker (background thread)
                    uniques[col] = (
                        df.get_column(col)
                        .cast(pl.Utf8, strict=False)
                        .drop_nulls()
                        .unique()
                        .sort()
                        .to_list()
                    )
            return {"df": df, "uniques": uniques}

        worker = ServiceTaskWorker(_worker_load)

        def _on_success(result):
            if status_msg:
                self.status.showMessage(
                    f"✔ {status_msg.replace('Carregando', 'Feito')}", 3000
                )

            if isinstance(result, dict) and "df" in result:
                callback(result["df"], result.get("uniques"))
            else:
                callback(result)

        def _on_failed(err: str):
            self.show_error(
                "Erro de Carregamento", f"Falha ao carregar {path.name}: {err}"
            )

        worker.finished_ok.connect(_on_success)
        worker.failed.connect(_on_failed)

        if not hasattr(self, "_active_load_workers") or not isinstance(
            self._active_load_workers, set
        ):
            self._active_load_workers = set()

        self._active_load_workers.add(worker)
        worker.finished.connect(lambda: self._active_load_workers.discard(worker))
        worker.finished.connect(worker.deleteLater)

        worker.start()

    def _limpar_aba_resumo_estoque(self, contexto: str, mensagem: str) -> None:
        if contexto == "aba_mensal":
            self.aba_mensal_model.set_dataframe(pl.DataFrame())
            self._aba_mensal_df = pl.DataFrame()
            self.lbl_aba_mensal_status.setText(mensagem)
            self.lbl_aba_mensal_filtros.setText("Filtros ativos: nenhum")
            self._atualizar_titulo_aba_mensal()
            return
        if contexto == "aba_anual":
            self.aba_anual_model.set_dataframe(pl.DataFrame())
            self._aba_anual_df = pl.DataFrame()
            self.lbl_aba_anual_status.setText(mensagem)
            self.lbl_aba_anual_filtros.setText("Filtros ativos: nenhum")
            self._atualizar_titulo_aba_anual()

    def _garantir_resumos_estoque_atualizados(self, cnpj: str) -> bool:
        artefatos_defasados = self.servico_agregacao.artefatos_estoque_defasados(cnpj)
        if not artefatos_defasados:
            return True

        if self._sync_resumos_estoque_cnpj == cnpj:
            return False

        if self.service_worker is not None and self.service_worker.isRunning():
            self.status.showMessage(
                "Aguardando o processamento atual para sincronizar as tabelas mensal/anual."
            )
            return False

        self._sync_resumos_estoque_cnpj = cnpj
        nomes = {
            "calculos_mensais": "mensal",
            "calculos_anuais": "anual",
        }
        descricoes = ", ".join(nomes.get(item, item) for item in artefatos_defasados)

        def _on_success(ok) -> None:
            self._sync_resumos_estoque_cnpj = None
            if ok:
                self.refresh_file_tree(cnpj)
                self.atualizar_aba_mensal()
                self.atualizar_aba_anual()
                self.atualizar_aba_produtos_selecionados()
                self.atualizar_aba_resumo_global()
                self.status.showMessage(
                    f"Tabelas {descricoes} sincronizadas com a mov_estoque."
                )
            else:
                self.status.showMessage(
                    "Falha ao sincronizar as tabelas mensal/anual com a mov_estoque."
                )
                self.show_error(
                    "Falha na sincronizacao",
                    "Nao foi possivel atualizar as tabelas mensal/anual.",
                )

        def _on_failure(mensagem: str) -> None:
            self._sync_resumos_estoque_cnpj = None
            self.status.showMessage("Erro ao sincronizar as tabelas mensal/anual.")
            self.show_error("Falha na sincronizacao", mensagem)

        iniciado = self._executar_em_worker(
            self.servico_agregacao.recalcular_resumos_estoque,
            cnpj,
            mensagem_inicial=f"Sincronizando tabelas {descricoes} com a mov_estoque...",
            on_success=_on_success,
            on_failure=_on_failure,
        )
        if not iniciado:
            self._sync_resumos_estoque_cnpj = None
            return False
        return False















































    def _obter_cnpj_valido(self) -> str | None:
        if not self.state.current_cnpj:
            self.show_error(
                "CNPJ nao selecionado", "Selecione um CNPJ na lista a esquerda."
            )
            return None
        return self.state.current_cnpj

    def _executar_em_worker(
        self,
        func: Callable,
        *args,
        mensagem_inicial: str,
        on_success: Callable[[object], None],
        on_failure: Callable[[str], None] | None = None,
        **kwargs,
    ) -> bool:
        if self.service_worker is not None and self.service_worker.isRunning():
            self.show_error("Aguarde", "Ja existe um processamento pesado em execucao.")
            return False

        self.status.showMessage(mensagem_inicial)
        worker = ServiceTaskWorker(func, *args, **kwargs)
        self.service_worker = worker
        worker.progress.connect(self.status.showMessage)

        def _finalizar_ok(resultado) -> None:
            self.service_worker = None
            on_success(resultado)

        def _finalizar_erro(mensagem: str) -> None:
            self.service_worker = None
            if on_failure is not None:
                on_failure(mensagem)
            else:
                self.show_error("Erro", mensagem)

        worker.finished_ok.connect(_finalizar_ok)
        worker.failed.connect(_finalizar_erro)
        self._registrar_limpeza_worker("service_worker", worker)
        worker.start()
        return True

    def on_cnpj_selected(self) -> None:
        item = self.cnpj_list.currentItem()
        if not item:
            return
        cnpj = item.text()
        self.state.current_cnpj = cnpj
        self._produtos_sel_preselecionado_cnpj = None
        self._atualizar_estado_botao_nfe_entrada()
        self._reset_table_resize_flag("conversao")
        self._reset_table_resize_flag("mov_estoque")
        self._reset_table_resize_flag("aba_mensal")
        self._reset_table_resize_flag("aba_anual")
        self._reset_table_resize_flag("nfe_entrada")
        self._reset_table_resize_flag("produtos_selecionados")
        self._reset_table_resize_flag("agregacao_top")
        self._reset_table_resize_flag("agregacao_bottom")
        self.status.showMessage(f"CNPJ selecionado: {cnpj}")
        self._refresh_profile_combos()
        self.refresh_file_tree(cnpj)

        # Lazy Loading: Carregar apenas a aba que o usuário está vendo no momento
        self._carregar_aba_atual()
        self.recarregar_historico_agregacao(cnpj)

        # Automacao de Data limite EFD baseada nao reg_0000
        data_efd = (
            self.servico_pipeline_funcoes.servico_extracao.obter_data_entrega_reg0000(
                cnpj
            )
        )
        if data_efd:
            qdate = QDate.fromString(data_efd, "dd/MM/yyyy")
            if qdate.isValid():
                self.date_input.setDate(qdate)

    def _carregar_aba_atual(self) -> None:
        """Carrega os dados da aba que está visível no momento para o CNPJ atual."""
        if not self.state.current_cnpj:
            return

        aba_idx = self.tabs.currentIndex()
        texto_aba = self.tabs.tabText(aba_idx).strip().lower()

        # Mapeamento de abas para funções de atualização
        if texto_aba == "agregacao":
            self.atualizar_tabelas_agregacao()
        elif texto_aba == "conversao":
            self.atualizar_aba_conversao()
        elif texto_aba == "estoque":
            self.atualizar_aba_mov_estoque()
        elif texto_aba == "nfe entrada":
            self.atualizar_aba_nfe_entrada()
        elif texto_aba == "logs":
            self.refresh_logs()
        elif "mensal" in texto_aba:
            self.atualizar_aba_mensal()
        elif "anual" in texto_aba:
            self.atualizar_aba_anual()
        elif "periodos" in texto_aba:
            self.atualizar_aba_periodos()
        elif "id" in texto_aba and "agrupado" in texto_aba:
            self.atualizar_aba_id_agrupados()

    def refresh_file_tree(self, cnpj: str) -> None:
        self.file_tree.clear()

        root_path = self.parquet_service.cnpj_dir(cnpj)

        cat_brutas = QTreeWidgetItem(
            ["Tabelas brutas (SQL)", str(root_path / "arquivos_parquet")]
        )
        cat_analises = QTreeWidgetItem(
            ["Analises de Produtos", str(root_path / "analises" / "produtos")]
        )
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
        self._reset_table_resize_flag("consulta")
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
        prefs = self._carregar_preferencias_tabela("consulta", self._consulta_scope())
        self._refresh_profile_combos()
        pref_visiveis = (
            prefs.get("visible_columns") if isinstance(prefs, dict) else None
        )
        if reset_columns or not self.state.visible_columns:
            self.state.visible_columns = (
                pref_visiveis
                if isinstance(pref_visiveis, list) and pref_visiveis
                else all_columns[:]
            )
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
                self._resize_table_once(self.table_view, "consulta")
                self._aplicar_preferencias_tabela(
                    "consulta",
                    self.table_view,
                    self.table_model,
                    self._consulta_scope(),
                )
        except Exception as exc:
            self.show_error("Erro ao carregar dados", str(exc))

    def _update_page_label(self) -> None:
        total_pages = max(
            1,
            (
                ((self.state.total_rows - 1) // self.state.page_size) + 1
                if self.state.total_rows
                else 1
            ),
        )
        if self.state.current_page > total_pages:
            self.state.current_page = total_pages
        self.lbl_page.setText(
            f"Pagina {self.state.current_page}/{total_pages} | Linhas filtradas: {self.state.total_rows}"
        )

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
            self.show_error(
                "Filtro invalido", "Informe um valor para o filtro escolhido."
            )
            return
        self.state.filters = self.state.filters or []
        self.state.filters.append(
            FilterCondition(column=column, operator=operator, value=value)
        )
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




















    def refresh_logs(self) -> None:
        import json

        logs = [json.dumps(log) for log in self.servico_agregacao.ler_linhas_log()]
        self.log_view.setPlainText("\n".join(logs))








    def open_cnpj_folder(self) -> None:
        if not self.state.current_cnpj:
            self.show_error(
                "CNPJ nao selecionado", "Selecione um CNPJ para abrir a pasta."
            )
            return
        target = self.parquet_service.cnpj_dir(self.state.current_cnpj)
        if not target.exists():
            self.show_error(
                "Pasta inexistente", f"A pasta {target} ainda nao foi criada."
            )
            return
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(target)))
























    # ==================================================================
    # Consulta SQL - metodos de suporte
    # ==================================================================
    _sql_result_page: int = 1
    _sql_result_page_size: int = DEFAULT_PAGE_SIZE














