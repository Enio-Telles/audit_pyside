from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import polars as pl
from interface_grafica.config import APP_NAME, CNPJ_ROOT, CONSULTAS_ROOT, DEFAULT_PAGE_SIZE
from interface_grafica.controllers.agregacao_controller import AgregacaoControllerMixin
from interface_grafica.controllers.auditoria_controller import AuditoriaControllerMixin
from interface_grafica.controllers.consulta_controller import ConsultaControllerMixin
from interface_grafica.controllers.conversao_controller import ConversaoControllerMixin
from interface_grafica.controllers.id_agrupados_controller import IdAgrupadosControllerMixin
from interface_grafica.controllers.importacao_controller import ImportacaoControllerMixin
from interface_grafica.controllers.relatorios_periodos_controller import RelatoriosPeriodosControllerMixin
from interface_grafica.controllers.relatorios_produtos_controller import RelatoriosProdutosControllerMixin
from interface_grafica.controllers.relatorios_resumo_controller import RelatoriosResumoControllerMixin
from interface_grafica.controllers.relatorios_style_controller import RelatoriosStyleControllerMixin
from interface_grafica.controllers.shared_state import ViewState
from interface_grafica.controllers.sql_query_controller import SqlQueryControllerMixin
from interface_grafica.controllers.workers import PipelineWorker, ServiceTaskWorker
from interface_grafica.models.table_model import PolarsTableModel
from interface_grafica.services.aggregation_service import ServicoAgregacao
from interface_grafica.services.export_service import ExportService
from interface_grafica.services.parquet_service import FilterCondition, ParquetService
from interface_grafica.services.pipeline_funcoes_service import ServicoPipelineCompleto
from interface_grafica.services.pipeline_service import PipelineService
from interface_grafica.services.query_worker import QueryWorker
from interface_grafica.services.registry_service import RegistryService
from interface_grafica.services.selection_persistence_service import SelectionPersistenceService
from interface_grafica.services.sql_service import SqlService
from interface_grafica.widgets.detached_table_window import DetachedTableWindow
from interface_grafica.windows.aba_agregacao import AgregacaoWindowMixin
from interface_grafica.windows.aba_auditoria import AuditoriaWindowMixin
from interface_grafica.windows.aba_importacao import ImportacaoWindowMixin
from interface_grafica.windows.aba_relatorios import RelatoriosWindowMixin
from interface_grafica.windows.main_window_filters import MainWindowFiltersMixin
from interface_grafica.windows.main_window_loading import MainWindowLoadingMixin
from interface_grafica.windows.main_window_navigation import MainWindowNavigationMixin
from interface_grafica.windows.main_window_preferences import MainWindowPreferencesMixin
from interface_grafica.windows.main_window_profile_presets import MainWindowProfilePresetsMixin
from interface_grafica.windows.main_window_signal_wiring_core import MainWindowSignalWiringCoreMixin
from interface_grafica.windows.main_window_signal_wiring_relatorios import MainWindowSignalWiringRelatoriosMixin
from interface_grafica.windows.main_window_support import MainWindowSupportMixin
from PySide6.QtCore import Qt, QTimer
from PySide6.QtWidgets import QHBoxLayout, QLabel, QMainWindow, QPushButton, QSplitter, QStatusBar, QTabWidget, QVBoxLayout, QWidget


class MainWindow(
    MainWindowSignalWiringCoreMixin,
    MainWindowSignalWiringRelatoriosMixin,
    MainWindowNavigationMixin,
    MainWindowLoadingMixin,
    MainWindowProfilePresetsMixin,
    MainWindowPreferencesMixin,
    MainWindowFiltersMixin,
    MainWindowSupportMixin,
    RelatoriosStyleControllerMixin,
    RelatoriosResumoControllerMixin,
    RelatoriosProdutosControllerMixin,
    RelatoriosPeriodosControllerMixin,
    RelatoriosWindowMixin,
    SqlQueryControllerMixin,
    ConversaoControllerMixin,
    AgregacaoControllerMixin,
    ConsultaControllerMixin,
    IdAgrupadosControllerMixin,
    AgregacaoWindowMixin,
    AuditoriaControllerMixin,
    AuditoriaWindowMixin,
    ImportacaoControllerMixin,
    ImportacaoWindowMixin,
    QMainWindow,
):
    _sql_result_page: int = 1
    _sql_result_page_size: int = DEFAULT_PAGE_SIZE

    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle(APP_NAME)
        self.resize(1560, 920)

        self._load_noir_theme()

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
        self.table_page_worker: ServiceTaskWorker | None = None
        self._table_page_request_id = 0
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

    def _load_noir_theme(self) -> None:
        theme_path = Path(__file__).resolve().parents[1] / "themes" / "noir.qss"
        if not theme_path.exists():
            return
        try:
            self.setStyleSheet(theme_path.read_text(encoding="utf-8"))
        except Exception:
            return
