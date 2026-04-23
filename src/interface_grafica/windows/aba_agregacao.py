from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QComboBox,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QSplitter,
    QTableView,
    QToolBar,
    QVBoxLayout,
    QWidget,
)


class AbaAgregacao:
    """Construtor da aba Agregacao, extraido do MainWindow monolitico."""

    def __init__(self, main_window):
        self.main = main_window
        self.root = QWidget()
        self._setup_ui()

    def _setup_ui(self) -> None:
        main = self.main
        layout = QVBoxLayout(self.root)
        layout.setContentsMargins(0, 0, 0, 0)

        splitter = QSplitter(Qt.Vertical)

        top_box = QGroupBox("Tabela Agrupada Filtravel (Selecione linhas para agregar)")
        top_layout = QVBoxLayout(top_box)
        top_layout.setContentsMargins(4, 12, 4, 4)

        toolbar = QHBoxLayout()
        main.btn_abrir_grup_sql = QPushButton("Abrir tabela agrupada")
        main.btn_abrir_grup_sql.clicked.connect(main._abrir_tabela_agrupada)
        toolbar.addWidget(main.btn_abrir_grup_sql)

        main.btn_agregar_descricoes = QPushButton("Agregar Descricoes (da selecao)")
        toolbar.addWidget(main.btn_agregar_descricoes)

        main.btn_reprocessar_agregacao = main._criar_botao_destacar("Reprocessar")
        toolbar.addWidget(main.btn_reprocessar_agregacao)

        toolbar.addStretch()
        top_layout.addLayout(toolbar)

        filtros = QHBoxLayout()
        main.top_filter_desc = QLineEdit()
        main.top_filter_desc.setPlaceholderText("Filtrar Descricao (ex.: buch 18)")
        main.top_filter_ncm = QLineEdit()
        main.top_filter_ncm.setPlaceholderText("Filtrar NCM")
        main.top_filter_cest = QLineEdit()
        main.top_filter_cest.setPlaceholderText("Filtrar CEST")
        main.top_filter_texto = QLineEdit()
        main.top_filter_texto.setPlaceholderText("Busca global...")
        main.btn_top_match_ncm_cest = QPushButton("NCM+CEST iguais")
        main.btn_top_match_ncm_cest_gtin = QPushButton("NCM+CEST+GTIN iguais")
        main.btn_clear_top_agg_filters = QPushButton("Limpar filtros")
        main.top_profile = QComboBox()
        main.top_profile.addItems(["Padrao", "Auditoria", "Estoque", "Custos"])
        main.btn_apply_top_profile = QPushButton("Perfil")
        main.btn_save_top_profile = QPushButton("Salvar perfil")
        main.btn_top_colunas = QPushButton("Colunas")
        main.btn_top_destacar = main._criar_botao_destacar()
        for widget in [
            main.top_filter_desc,
            main.top_filter_ncm,
            main.top_filter_cest,
            main.top_filter_texto,
            main.btn_top_match_ncm_cest,
            main.btn_top_match_ncm_cest_gtin,
            main.top_profile,
            main.btn_apply_top_profile,
            main.btn_save_top_profile,
            main.btn_top_colunas,
            main.btn_top_destacar,
            main.btn_clear_top_agg_filters,
        ]:
            filtros.addWidget(widget)
        top_layout.addLayout(filtros)

        main.lbl_top_table_status = QLabel("Nenhum dado.")
        top_layout.addWidget(main.lbl_top_table_status)

        main.aggregation_table = QTableView()
        main.aggregation_table.setModel(main.aggregation_table_model)
        main.aggregation_table.setSelectionBehavior(QAbstractItemView.SelectItems)
        main.aggregation_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        main.aggregation_table.setAlternatingRowColors(True)
        main.aggregation_table.setSortingEnabled(True)
        main.aggregation_table.setWordWrap(True)
        main.aggregation_table.verticalHeader().setDefaultSectionSize(40)
        main.aggregation_table.horizontalHeader().setMinimumSectionSize(40)
        main.aggregation_table.horizontalHeader().setDefaultSectionSize(150)
        main.aggregation_table.horizontalHeader().setMaximumSectionSize(400)
        main.aggregation_table.horizontalHeader().setSectionsMovable(True)
        main.aggregation_table.horizontalHeader().setContextMenuPolicy(Qt.CustomContextMenu)
        top_layout.addWidget(main.aggregation_table)

        splitter.addWidget(top_box)

        bottom_box = QGroupBox("Linhas Agregadas (Mesma Tabela de Referencia)")
        bottom_layout = QVBoxLayout(bottom_box)
        bottom_layout.setContentsMargins(4, 12, 4, 4)

        bottom_filtros = QHBoxLayout()
        main.bot_filter_desc_norm = QLineEdit()
        main.bot_filter_desc_norm.setPlaceholderText("Filtrar Desc. Norm")
        main.bot_filter_desc_orig = QLineEdit()
        main.bot_filter_desc_orig.setPlaceholderText("Filtrar Descricao (ex.: whisky 12)")
        main.bot_filter_ncm = QLineEdit()
        main.bot_filter_ncm.setPlaceholderText("Filtrar NCM")
        main.bot_filter_cest = QLineEdit()
        main.bot_filter_cest.setPlaceholderText("Filtrar CEST")
        main.btn_bottom_match_ncm_cest = QPushButton("NCM+CEST iguais")
        main.btn_bottom_match_ncm_cest_gtin = QPushButton("NCM+CEST+GTIN iguais")
        main.btn_clear_bottom_agg_filters = QPushButton("Limpar filtros")
        main.bottom_profile = QComboBox()
        main.bottom_profile.addItems(["Padrao", "Auditoria", "Estoque", "Custos"])
        main.btn_apply_bottom_profile = QPushButton("Perfil")
        main.btn_save_bottom_profile = QPushButton("Salvar perfil")
        main.btn_bottom_colunas = QPushButton("Colunas")
        main.btn_bottom_destacar = main._criar_botao_destacar()
        for widget in [
            main.bot_filter_desc_norm,
            main.bot_filter_desc_orig,
            main.bot_filter_ncm,
            main.bot_filter_cest,
            main.btn_bottom_match_ncm_cest,
            main.btn_bottom_match_ncm_cest_gtin,
            main.bottom_profile,
            main.btn_apply_bottom_profile,
            main.btn_save_bottom_profile,
            main.btn_bottom_colunas,
            main.btn_bottom_destacar,
            main.btn_clear_bottom_agg_filters,
        ]:
            bottom_filtros.addWidget(widget)
        bottom_layout.addLayout(bottom_filtros)

        main.lbl_bot_table_status = QLabel("Nenhuma linha agrupada.")
        bottom_layout.addWidget(main.lbl_bot_table_status)

        main.results_table = QTableView()
        main.results_table.setModel(main.results_table_model)
        main.results_table.setSelectionBehavior(QAbstractItemView.SelectItems)
        main.results_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        main.results_table.setAlternatingRowColors(True)
        main.results_table.setSortingEnabled(True)
        main.results_table.setWordWrap(True)
        main.results_table.verticalHeader().setDefaultSectionSize(40)
        main.results_table.horizontalHeader().setMinimumSectionSize(40)
        main.results_table.horizontalHeader().setDefaultSectionSize(150)
        main.results_table.horizontalHeader().setMaximumSectionSize(400)
        main.results_table.horizontalHeader().setSectionsMovable(True)
        main.results_table.horizontalHeader().setContextMenuPolicy(Qt.CustomContextMenu)
        bottom_layout.addWidget(main.results_table)

        tb_acoes = QToolBar()
        main.btn_reverter_agregacao = QPushButton(
            QApplication.style().standardIcon(
                QApplication.style().StandardPixmap.SP_BrowserReload
            ),
            "Reverter agrupamento",
        )
        main.btn_desfazer_agregacao = QPushButton(
            QApplication.style().standardIcon(QApplication.style().StandardPixmap.SP_ArrowLeft),
            "Desfazer selecao",
        )
        main.btn_reverter_agregacao.clicked.connect(main.reverter_agregacao)
        tb_acoes.addWidget(main.btn_reverter_agregacao)
        main.btn_desfazer_agregacao.clicked.connect(main._desfazer_agregacao)
        tb_acoes.addWidget(main.btn_desfazer_agregacao)
        main.btn_reverter_mapa_manual = QPushButton("Reverter Mapa Manual")
        tb_acoes.addWidget(main.btn_reverter_mapa_manual)
        main.btn_reverter_mapa_manual.clicked.connect(main.reverter_mapa_manual_ui)
        bottom_layout.addWidget(tb_acoes)

        splitter.addWidget(bottom_box)
        splitter.setSizes([500, 300])

        main.btn_open_editable_table = main.btn_abrir_grup_sql
        main.btn_execute_aggregation = main.btn_agregar_descricoes
        main.btn_recalc_defaults = main.btn_reprocessar_agregacao
        main.btn_recalc_totals = main.btn_reprocessar_agregacao
        main.aggregation_table_view = main.aggregation_table
        main.results_table_view = main.results_table
        main.aqf_norm = main.top_filter_texto
        main.aqf_desc = main.top_filter_desc
        main.aqf_ncm = main.top_filter_ncm
        main.aqf_cest = main.top_filter_cest
        main.bqf_norm = main.bot_filter_desc_norm
        main.bqf_desc = main.bot_filter_desc_orig
        main.bqf_ncm = main.bot_filter_ncm
        main.bqf_cest = main.bot_filter_cest

        layout.addWidget(splitter)
