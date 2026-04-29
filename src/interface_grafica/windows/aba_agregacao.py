from __future__ import annotations

from PySide6.QtCore import QDate, Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QCheckBox,
    QComboBox,
    QDateEdit,
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
    QToolBar,
    QVBoxLayout,
    QWidget,
)


class AgregacaoWindowMixin:
    def _build_tab_agregacao(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(0, 0, 0, 0)

        splitter = QSplitter(Qt.Vertical)

        # Top box
        top_box = QGroupBox("Tabela Agrupada Filtravel (Selecione linhas para agregar)")
        top_layout = QVBoxLayout(top_box)
        top_layout.setContentsMargins(4, 12, 4, 4)

        toolbar = QHBoxLayout()
        self.btn_abrir_grup_sql = QPushButton("Abrir tabela agrupada")
        self.btn_abrir_grup_sql.clicked.connect(self._abrir_tabela_agrupada)
        toolbar.addWidget(self.btn_abrir_grup_sql)

        self.btn_agregar_descricoes = QPushButton("Agregar Descricoes (da selecao)")
        toolbar.addWidget(self.btn_agregar_descricoes)

        self.btn_reprocessar_agregacao = self._criar_botao_destacar("Reprocessar")
        toolbar.addWidget(self.btn_reprocessar_agregacao)

        toolbar.addStretch()
        top_layout.addLayout(toolbar)

        filtros = QHBoxLayout()
        self.top_filter_desc = QLineEdit()
        self.top_filter_desc.setPlaceholderText("Filtrar Descricao (ex.: buch 18)")
        self.top_filter_ncm = QLineEdit()
        self.top_filter_ncm.setPlaceholderText("Filtrar NCM")
        self.top_filter_cest = QLineEdit()
        self.top_filter_cest.setPlaceholderText("Filtrar CEST")
        self.top_filter_texto = QLineEdit()
        self.top_filter_texto.setPlaceholderText("Busca global...")
        self.btn_top_match_ncm_cest = QPushButton("NCM+CEST iguais")
        self.btn_top_match_ncm_cest_gtin = QPushButton("NCM+CEST+GTIN iguais")
        self.btn_clear_top_agg_filters = QPushButton("Limpar filtros")
        self.top_profile = QComboBox()
        self.top_profile.addItems(["Padrao", "Auditoria", "Estoque", "Custos"])
        self.btn_apply_top_profile = QPushButton("Perfil")
        self.btn_save_top_profile = QPushButton("Salvar perfil")
        self.btn_top_colunas = QPushButton("Colunas")
        self.btn_top_destacar = self._criar_botao_destacar()
        filtros.addWidget(self.top_filter_desc)
        filtros.addWidget(self.top_filter_ncm)
        filtros.addWidget(self.top_filter_cest)
        filtros.addWidget(self.top_filter_texto)
        filtros.addWidget(self.btn_top_match_ncm_cest)
        filtros.addWidget(self.btn_top_match_ncm_cest_gtin)
        filtros.addWidget(self.top_profile)
        filtros.addWidget(self.btn_apply_top_profile)
        filtros.addWidget(self.btn_save_top_profile)
        filtros.addWidget(self.btn_top_colunas)
        filtros.addWidget(self.btn_top_destacar)
        filtros.addWidget(self.btn_clear_top_agg_filters)
        top_layout.addLayout(filtros)

        self.lbl_top_table_status = QLabel("Nenhum dado.")
        top_layout.addWidget(self.lbl_top_table_status)

        self.aggregation_table = QTableView()
        self.aggregation_table.setModel(self.aggregation_table_model)
        self.aggregation_table.setSelectionBehavior(QAbstractItemView.SelectItems)
        self.aggregation_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.aggregation_table.setAlternatingRowColors(True)
        self.aggregation_table.setSortingEnabled(True)
        self.aggregation_table.setWordWrap(True)
        self.aggregation_table.verticalHeader().setDefaultSectionSize(40)
        self.aggregation_table.horizontalHeader().setMinimumSectionSize(40)
        self.aggregation_table.horizontalHeader().setDefaultSectionSize(150)
        self.aggregation_table.horizontalHeader().setMaximumSectionSize(400)
        self.aggregation_table.horizontalHeader().setSectionsMovable(True)
        self.aggregation_table.horizontalHeader().setContextMenuPolicy(
            Qt.CustomContextMenu
        )
        top_layout.addWidget(self.aggregation_table)

        splitter.addWidget(top_box)

        # Bottom box
        bottom_box = QGroupBox("Linhas Agregadas (Mesma Tabela de Referencia)")
        bottom_layout = QVBoxLayout(bottom_box)
        bottom_layout.setContentsMargins(4, 12, 4, 4)

        bottom_filtros = QHBoxLayout()
        self.bot_filter_desc_norm = QLineEdit()
        self.bot_filter_desc_norm.setPlaceholderText("Filtrar Desc. Norm")
        self.bot_filter_desc_orig = QLineEdit()
        self.bot_filter_desc_orig.setPlaceholderText(
            "Filtrar Descricao (ex.: whisky 12)"
        )
        self.bot_filter_ncm = QLineEdit()
        self.bot_filter_ncm.setPlaceholderText("Filtrar NCM")
        self.bot_filter_cest = QLineEdit()
        self.bot_filter_cest.setPlaceholderText("Filtrar CEST")
        self.btn_bottom_match_ncm_cest = QPushButton("NCM+CEST iguais")
        self.btn_bottom_match_ncm_cest_gtin = QPushButton("NCM+CEST+GTIN iguais")
        self.btn_clear_bottom_agg_filters = QPushButton("Limpar filtros")
        self.bottom_profile = QComboBox()
        self.bottom_profile.addItems(["Padrao", "Auditoria", "Estoque", "Custos"])
        self.btn_apply_bottom_profile = QPushButton("Perfil")
        self.btn_save_bottom_profile = QPushButton("Salvar perfil")
        self.btn_bottom_colunas = QPushButton("Colunas")
        self.btn_bottom_destacar = self._criar_botao_destacar()
        bottom_filtros.addWidget(self.bot_filter_desc_norm)
        bottom_filtros.addWidget(self.bot_filter_desc_orig)
        bottom_filtros.addWidget(self.bot_filter_ncm)
        bottom_filtros.addWidget(self.bot_filter_cest)
        bottom_filtros.addWidget(self.btn_bottom_match_ncm_cest)
        bottom_filtros.addWidget(self.btn_bottom_match_ncm_cest_gtin)
        bottom_filtros.addWidget(self.bottom_profile)
        bottom_filtros.addWidget(self.btn_apply_bottom_profile)
        bottom_filtros.addWidget(self.btn_save_bottom_profile)
        bottom_filtros.addWidget(self.btn_bottom_colunas)
        bottom_filtros.addWidget(self.btn_bottom_destacar)
        bottom_filtros.addWidget(self.btn_clear_bottom_agg_filters)
        bottom_layout.addLayout(bottom_filtros)

        self.lbl_bot_table_status = QLabel("Nenhuma linha agrupada.")
        bottom_layout.addWidget(self.lbl_bot_table_status)

        self.results_table = QTableView()
        self.results_table.setModel(self.results_table_model)
        self.results_table.setSelectionBehavior(QAbstractItemView.SelectItems)
        self.results_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.results_table.setAlternatingRowColors(True)
        self.results_table.setSortingEnabled(True)
        self.results_table.setWordWrap(True)
        self.results_table.verticalHeader().setDefaultSectionSize(40)
        self.results_table.horizontalHeader().setMinimumSectionSize(40)
        self.results_table.horizontalHeader().setDefaultSectionSize(150)
        self.results_table.horizontalHeader().setMaximumSectionSize(400)
        self.results_table.horizontalHeader().setSectionsMovable(True)
        self.results_table.horizontalHeader().setContextMenuPolicy(Qt.CustomContextMenu)
        bottom_layout.addWidget(self.results_table)

        tb_acoes = QToolBar()
        self.btn_reverter_agregacao = QPushButton(
            QApplication.style().standardIcon(
                QApplication.style().StandardPixmap.SP_BrowserReload
            ),
            "Reverter agrupamento",
        )
        self.btn_desfazer_agregacao = QPushButton(
            QApplication.style().standardIcon(
                QApplication.style().StandardPixmap.SP_ArrowLeft
            ),
            "Desfazer selecao",
        )
        self.btn_reverter_agregacao.clicked.connect(self.reverter_agregacao)
        tb_acoes.addWidget(self.btn_reverter_agregacao)
        self.btn_desfazer_agregacao.clicked.connect(self._desfazer_agregacao)
        tb_acoes.addWidget(self.btn_desfazer_agregacao)
        # Botao para restaurar snapshot do mapa manual
        self.btn_reverter_mapa_manual = QPushButton("Reverter Mapa Manual")
        tb_acoes.addWidget(self.btn_reverter_mapa_manual)
        self.btn_reverter_mapa_manual.clicked.connect(self.reverter_mapa_manual_ui)
        bottom_layout.addWidget(tb_acoes)

        splitter.addWidget(bottom_box)
        splitter.setSizes([500, 300])

        # Aliases legados usados em outros trechos da tela.
        self.btn_open_editable_table = self.btn_abrir_grup_sql
        self.btn_execute_aggregation = self.btn_agregar_descricoes
        self.btn_recalc_defaults = self.btn_reprocessar_agregacao
        self.btn_recalc_totals = self.btn_reprocessar_agregacao
        self.aggregation_table_view = self.aggregation_table
        self.results_table_view = self.results_table
        self.aqf_norm = self.top_filter_texto
        self.aqf_desc = self.top_filter_desc
        self.aqf_ncm = self.top_filter_ncm
        self.aqf_cest = self.top_filter_cest
        self.bqf_norm = self.bot_filter_desc_norm
        self.bqf_desc = self.bot_filter_desc_orig
        self.bqf_ncm = self.bot_filter_ncm
        self.bqf_cest = self.bot_filter_cest

        layout.addWidget(splitter)
        return tab
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
        self.btn_sql_execute.setStyleSheet(
            "QPushButton { font-weight: bold; padding: 6px 16px; }"
        )
        self.btn_sql_export = QPushButton("Exportar Excel")
        self.btn_sql_destacar = self._criar_botao_destacar()
        top_bar.addWidget(self.btn_sql_execute)
        top_bar.addWidget(self.btn_sql_export)
        top_bar.addWidget(self.btn_sql_destacar)
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
        self.btn_refresh_conversao.setIcon(
            QApplication.style().standardIcon(
                QApplication.style().StandardPixmap.SP_BrowserReload
            )
        )
        self.chk_show_single_unit = QCheckBox("Mostrar itens de unidade unica")
        self.chk_show_single_unit.setChecked(False)
        self.btn_export_conversao = QPushButton("Exportar Excel")
        self.btn_import_conversao = QPushButton("Importar Excel")
        self.btn_conversao_destacar = self._criar_botao_destacar()
        self.btn_recalcular_fatores = self._criar_botao_destacar("Recalcular fatores")
        self.btn_recalcular_fatores.setEnabled(False)
        self.conversao_profile = QComboBox()
        self.conversao_profile.addItems(["Padrao", "Auditoria", "Estoque", "Custos"])
        self.btn_apply_conversao_profile = QPushButton("Perfil")
        self.btn_save_conversao_profile = QPushButton("Salvar perfil")
        self.btn_conversao_colunas = QPushButton("Colunas")

        toolbar.addWidget(self.btn_refresh_conversao)
        toolbar.addWidget(self.chk_show_single_unit)
        toolbar.addStretch()
        toolbar.addWidget(self.btn_recalcular_fatores)
        toolbar.addWidget(self.conversao_profile)
        toolbar.addWidget(self.btn_apply_conversao_profile)
        toolbar.addWidget(self.btn_save_conversao_profile)
        toolbar.addWidget(self.btn_conversao_colunas)
        toolbar.addWidget(self.btn_conversao_destacar)
        toolbar.addWidget(self.btn_import_conversao)
        layout.addLayout(toolbar)

        filtros = QHBoxLayout()
        self.conv_filter_id = QComboBox()
        self.conv_filter_id.setEditable(True)
        self.conv_filter_id.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
        self.conv_filter_id.setMinimumWidth(220)
        self.conv_filter_id.lineEdit().setPlaceholderText("Filtrar id_agrupado")
        self.conv_filter_desc = QLineEdit()
        self.conv_filter_desc.setPlaceholderText("Filtrar descr_padrao")
        filtros.addWidget(self.conv_filter_id)
        filtros.addWidget(self.conv_filter_desc)
        layout.addLayout(filtros)

        self.panel_unid_ref = QGroupBox(
            "Alterar Unidade de Referencia do Produto Selecionado"
        )
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
        self.conversion_table.horizontalHeader().setSectionsMovable(True)
        self.conversion_table.horizontalHeader().setContextMenuPolicy(
            Qt.CustomContextMenu
        )
        layout.addWidget(self.conversion_table)

        return tab
    def _build_tab_id_agrupados(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)

        self.lbl_id_agrupados_titulo = QLabel("Tabela: id_agrupados")
        self.lbl_id_agrupados_titulo.setStyleSheet(
            "QLabel { font-weight: bold; color: #f8fafc; background: #1f2a44; border: 1px solid #334155; border-radius: 4px; padding: 6px 10px; }"
        )
        layout.addWidget(self.lbl_id_agrupados_titulo)

        toolbar = QHBoxLayout()
        self.btn_refresh_id_agrupados = QPushButton("Recarregar")
        self.btn_apply_id_agrupados_filters = QPushButton("Aplicar filtros")
        self.btn_clear_id_agrupados_filters = QPushButton("Limpar filtros")
        self.id_agrupados_profile = QComboBox()
        self.id_agrupados_profile.addItems(["Padrao", "Auditoria", "Estoque", "Custos"])
        self.btn_id_agrupados_profile = QPushButton("Perfil")
        self.btn_id_agrupados_save_profile = QPushButton("Salvar perfil")
        self.btn_id_agrupados_colunas = QPushButton("Colunas")
        self.btn_destacar_id_agrupados = self._criar_botao_destacar()
        self.btn_export_id_agrupados = QPushButton("Exportar Excel")
        toolbar.addWidget(self.btn_refresh_id_agrupados)
        toolbar.addWidget(self.btn_apply_id_agrupados_filters)
        toolbar.addWidget(self.btn_clear_id_agrupados_filters)
        toolbar.addStretch()
        toolbar.addWidget(self.id_agrupados_profile)
        toolbar.addWidget(self.btn_id_agrupados_profile)
        toolbar.addWidget(self.btn_id_agrupados_save_profile)
        toolbar.addWidget(self.btn_id_agrupados_colunas)
        toolbar.addWidget(self.btn_destacar_id_agrupados)
        toolbar.addWidget(self.btn_export_id_agrupados)
        layout.addLayout(toolbar)

        filtros = QHBoxLayout()
        self.id_agrupados_filter_id = QComboBox()
        self.id_agrupados_filter_id.setEditable(True)
        self.id_agrupados_filter_id.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
        self.id_agrupados_filter_id.setMinimumWidth(240)
        self.id_agrupados_filter_id.lineEdit().setPlaceholderText("Filtrar id_agrupado")
        self.id_agrupados_filter_texto = QLineEdit()
        self.id_agrupados_filter_texto.setPlaceholderText("Busca ampla...")
        filtros.addWidget(self.id_agrupados_filter_id)
        filtros.addWidget(self.id_agrupados_filter_texto)
        layout.addLayout(filtros)

        self.lbl_id_agrupados_status = QLabel(
            "Selecione um CPF/CNPJ para carregar os id_agrupados."
        )
        self.lbl_id_agrupados_status.setStyleSheet(
            "QLabel { padding: 4px 8px; background: #101827; border: 1px solid #374151; border-radius: 4px; color: #e5e7eb; }"
        )
        layout.addWidget(self.lbl_id_agrupados_status)

        self.lbl_id_agrupados_filtros = QLabel("Filtros ativos: nenhum")
        self.lbl_id_agrupados_filtros.setStyleSheet(
            "QLabel { padding: 4px 8px; color: #dbeafe; background: #0f1b33; border: 1px solid #334155; border-radius: 4px; }"
        )
        layout.addWidget(self.lbl_id_agrupados_filtros)

        self.id_agrupados_table = QTableView()
        self.id_agrupados_table.setModel(self.id_agrupados_model)
        self.id_agrupados_table.setSelectionBehavior(QAbstractItemView.SelectItems)
        self.id_agrupados_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.id_agrupados_table.setAlternatingRowColors(True)
        self.id_agrupados_table.setSortingEnabled(True)
        self.id_agrupados_table.setWordWrap(True)
        self.id_agrupados_table.verticalHeader().setDefaultSectionSize(40)
        self.id_agrupados_table.horizontalHeader().setMinimumSectionSize(40)
        self.id_agrupados_table.horizontalHeader().setDefaultSectionSize(180)
        self.id_agrupados_table.horizontalHeader().setMaximumSectionSize(420)
        self.id_agrupados_table.horizontalHeader().setStretchLastSection(True)
        self.id_agrupados_table.horizontalHeader().setSectionsMovable(True)
        self.id_agrupados_table.horizontalHeader().setContextMenuPolicy(
            Qt.CustomContextMenu
        )
        layout.addWidget(self.id_agrupados_table, 1)
        return tab
