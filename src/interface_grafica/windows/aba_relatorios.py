from __future__ import annotations

from PySide6.QtCore import QDate, Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QCheckBox,
    QComboBox,
    QDateEdit,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QPushButton,
    QTableView,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)


class RelatoriosWindowMixin:
    def _build_tab_estoque(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        self.estoque_tabs = QTabWidget()

        self.tab_mov_estoque = self._build_tab_mov_estoque()
        self.estoque_tabs.addTab(self.tab_mov_estoque, "Tabela mov_estoque")

        self.tab_aba_mensal = self._build_tab_aba_mensal()
        self.estoque_tabs.addTab(self.tab_aba_mensal, "Tabela mensal")

        self.tab_aba_anual = self._build_tab_aba_anual()
        self.estoque_tabs.addTab(self.tab_aba_anual, "Tabela anual")
        self.tab_aba_periodos = self._build_tab_aba_periodos()
        self.estoque_tabs.addTab(self.tab_aba_periodos, "Tabela períodos")

        self.tab_resumo_global = self._build_tab_resumo_global()
        self.estoque_tabs.addTab(self.tab_resumo_global, "Resumo Global")

        self.tab_produtos_selecionados = self._build_tab_produtos_selecionados()
        self.estoque_tabs.addTab(
            self.tab_produtos_selecionados, "Produtos selecionados"
        )

        self.tab_id_agrupados = self._build_tab_id_agrupados()
        self.estoque_tabs.addTab(self.tab_id_agrupados, "id_agrupados")

        self.tab_aba_codigo_original = self._build_tab_aba_codigo_original()
        self.estoque_tabs.addTab(self.tab_aba_codigo_original, "Codigo Original")

        layout.addWidget(self.estoque_tabs)
        return tab
    def _build_tab_produtos_selecionados(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        tab.setStyleSheet(
            """
            QWidget {
                background: #252526;
                color: #f3f4f6;
            }
            QLabel {
                color: #e5e7eb;
            }
            QLineEdit, QComboBox, QDateEdit {
                background: #323438;
                color: #f9fafb;
                border: 1px solid #4b5563;
                border-radius: 4px;
                padding: 6px 8px;
                selection-background-color: #0e639c;
                selection-color: #ffffff;
            }
            QPushButton {
                background: #34373d;
                color: #f9fafb;
                border: 1px solid #4b5563;
                border-radius: 4px;
                padding: 6px 10px;
            }
            QPushButton:hover {
                background: #40444b;
            }
            QPushButton:pressed {
                background: #2f3338;
            }
            QTableView {
                background: #1f1f1f;
                alternate-background-color: #262626;
                color: #f9fafb;
                gridline-color: #3f3f46;
                border: 1px solid #3f3f46;
                selection-background-color: #0e639c;
                selection-color: #ffffff;
            }
            QHeaderView::section {
                background: #18181b;
                color: #f9fafb;
                border: 1px solid #3f3f46;
                padding: 6px 8px;
                font-weight: bold;
            }
            """
        )

        self.lbl_produtos_sel_titulo = QLabel("Tabela: produtos_selecionados")
        self.lbl_produtos_sel_titulo.setStyleSheet(
            "QLabel { font-weight: bold; color: #f8fafc; background: #1f2a44; border: 1px solid #334155; border-radius: 4px; padding: 6px 10px; }"
        )
        layout.addWidget(self.lbl_produtos_sel_titulo)

        toolbar = QHBoxLayout()
        self.btn_refresh_produtos_sel = QPushButton("Atualizar resumo")
        self.btn_apply_produtos_sel_filters = QPushButton("Aplicar filtros")
        self.btn_clear_produtos_sel_filters = QPushButton("Limpar filtros")
        self.btn_limpar_vistos_produtos_sel = QPushButton("Limpar vistos")
        self.btn_top20_icms_produtos_sel = QPushButton("20 maiores ICMS")
        self.btn_top20_icms_periodo_produtos_sel = QPushButton("20 maiores ICMS periodo")
        self.produtos_sel_profile = QComboBox()
        self.produtos_sel_profile.addItems(["Padrao", "Auditoria", "Estoque", "Custos"])
        self.btn_produtos_sel_profile = QPushButton("Perfil")
        self.btn_produtos_sel_save_profile = QPushButton("Salvar perfil")
        self.btn_colunas_produtos_sel = QPushButton("Colunas")
        self.btn_destacar_produtos_sel = self._criar_botao_destacar()
        self.btn_export_produtos_sel = QPushButton("Exportar Excel")
        toolbar.addWidget(self.btn_refresh_produtos_sel)
        toolbar.addWidget(self.btn_apply_produtos_sel_filters)
        toolbar.addWidget(self.btn_clear_produtos_sel_filters)
        toolbar.addWidget(self.btn_limpar_vistos_produtos_sel)
        toolbar.addWidget(self.btn_top20_icms_produtos_sel)
        toolbar.addWidget(self.btn_top20_icms_periodo_produtos_sel)
        toolbar.addStretch()
        toolbar.addWidget(self.produtos_sel_profile)
        toolbar.addWidget(self.btn_produtos_sel_profile)
        toolbar.addWidget(self.btn_produtos_sel_save_profile)
        toolbar.addWidget(self.btn_colunas_produtos_sel)
        toolbar.addWidget(self.btn_destacar_produtos_sel)
        toolbar.addWidget(self.btn_export_produtos_sel)
        layout.addLayout(toolbar)

        filtros = QHBoxLayout()
        self.produtos_sel_filter_id = QComboBox()
        self.produtos_sel_filter_id.setEditable(True)
        self.produtos_sel_filter_id.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
        self.produtos_sel_filter_id.setMinimumWidth(220)
        self.produtos_sel_filter_id.lineEdit().setPlaceholderText("Filtrar id_agregado")
        self.produtos_sel_filter_desc = QLineEdit()
        self.produtos_sel_filter_desc.setPlaceholderText("Filtrar descricao")
        self.produtos_sel_filter_ano_ini = QComboBox()
        self.produtos_sel_filter_ano_ini.addItem("Todos")
        self.produtos_sel_filter_ano_fim = QComboBox()
        self.produtos_sel_filter_ano_fim.addItem("Todos")
        self.produtos_sel_filter_data_ini = QDateEdit()
        self.produtos_sel_filter_data_ini.setCalendarPopup(True)
        self.produtos_sel_filter_data_ini.setDisplayFormat("dd/MM/yyyy")
        self.produtos_sel_filter_data_ini.setSpecialValueText("Data inicial")
        self.produtos_sel_filter_data_ini.setMinimumDate(QDate(1900, 1, 1))
        self.produtos_sel_filter_data_ini.setDate(
            self.produtos_sel_filter_data_ini.minimumDate()
        )
        self.produtos_sel_filter_data_fim = QDateEdit()
        self.produtos_sel_filter_data_fim.setCalendarPopup(True)
        self.produtos_sel_filter_data_fim.setDisplayFormat("dd/MM/yyyy")
        self.produtos_sel_filter_data_fim.setSpecialValueText("Data final")
        self.produtos_sel_filter_data_fim.setMinimumDate(QDate(1900, 1, 1))
        self.produtos_sel_filter_data_fim.setDate(
            self.produtos_sel_filter_data_fim.minimumDate()
        )
        self.produtos_sel_filter_texto = QLineEdit()
        self.produtos_sel_filter_texto.setPlaceholderText("Busca ampla...")
        for widget in [
            self.produtos_sel_filter_id,
            self.produtos_sel_filter_desc,
            QLabel("Ano inicial"),
            self.produtos_sel_filter_ano_ini,
            QLabel("Ano final"),
            self.produtos_sel_filter_ano_fim,
            QLabel("Data inicial"),
            self.produtos_sel_filter_data_ini,
            QLabel("Data final"),
            self.produtos_sel_filter_data_fim,
            self.produtos_sel_filter_texto,
        ]:
            filtros.addWidget(widget)
        layout.addLayout(filtros)

        self.lbl_produtos_sel_status = QLabel(
            "Selecione um CNPJ para consolidar os produtos analisados."
        )
        self.lbl_produtos_sel_status.setStyleSheet(
            "QLabel { padding: 4px 8px; background: #101827; border: 1px solid #374151; border-radius: 4px; color: #e5e7eb; }"
        )
        layout.addWidget(self.lbl_produtos_sel_status)

        self.lbl_produtos_sel_filtros = QLabel("Filtros ativos: nenhum")
        self.lbl_produtos_sel_filtros.setStyleSheet(
            "QLabel { padding: 4px 8px; color: #dbeafe; background: #0f1b33; border: 1px solid #334155; border-radius: 4px; }"
        )
        layout.addWidget(self.lbl_produtos_sel_filtros)

        self.lbl_produtos_sel_resumo = QLabel(
            "Recorte atual: mov_estoque 0 | mensal 0 | anual 0"
        )
        self.lbl_produtos_sel_resumo.setStyleSheet(
            "QLabel { padding: 4px 8px; color: #fef3c7; background: #2a1f0f; border: 1px solid #7c5a18; border-radius: 4px; }"
        )
        layout.addWidget(self.lbl_produtos_sel_resumo)

        self.produtos_sel_table = QTableView()
        self.produtos_sel_table.setModel(self.produtos_selecionados_model)
        self.produtos_sel_table.setSelectionBehavior(QAbstractItemView.SelectItems)
        self.produtos_sel_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.produtos_sel_table.setAlternatingRowColors(True)
        self.produtos_sel_table.setSortingEnabled(True)
        self.produtos_sel_table.setWordWrap(True)
        self.produtos_sel_table.verticalHeader().setDefaultSectionSize(40)
        self.produtos_sel_table.horizontalHeader().setMinimumSectionSize(40)
        self.produtos_sel_table.horizontalHeader().setDefaultSectionSize(180)
        self.produtos_sel_table.horizontalHeader().setMaximumSectionSize(420)
        self.produtos_sel_table.horizontalHeader().setStretchLastSection(True)
        self.produtos_sel_table.horizontalHeader().setSectionsMovable(True)
        self.produtos_sel_table.horizontalHeader().setContextMenuPolicy(
            Qt.CustomContextMenu
        )
        self.produtos_sel_table.setStyleSheet(
            "QTableView::item { padding: 4px 2px; }"
            "QTableCornerButton::section { background: #18181b; border: 1px solid #3f3f46; }"
        )
        layout.addWidget(self.produtos_sel_table, 1)
        return tab
    def _build_tab_aba_anual(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        tab.setStyleSheet(
            """
            QWidget {
                background: #252526;
                color: #f3f4f6;
            }
            QLabel {
                color: #e5e7eb;
            }
            QLineEdit, QComboBox, QDateEdit {
                background: #323438;
                color: #f9fafb;
                border: 1px solid #4b5563;
                border-radius: 4px;
                padding: 6px 8px;
                selection-background-color: #0e639c;
                selection-color: #ffffff;
            }
            QPushButton {
                background: #34373d;
                color: #f9fafb;
                border: 1px solid #4b5563;
                border-radius: 4px;
                padding: 6px 10px;
            }
            QPushButton:hover {
                background: #40444b;
            }
            QPushButton:pressed {
                background: #2f3338;
            }
            QTableView {
                background: #1f1f1f;
                alternate-background-color: #262626;
                color: #f9fafb;
                gridline-color: #3f3f46;
                border: 1px solid #3f3f46;
                selection-background-color: #0e639c;
                selection-color: #ffffff;
            }
            QHeaderView::section {
                background: #18181b;
                color: #f9fafb;
                border: 1px solid #3f3f46;
                padding: 6px 8px;
                font-weight: bold;
            }
            QScrollBar:vertical, QScrollBar:horizontal {
                background: #252526;
                border: none;
            }
            QScrollBar::handle:vertical, QScrollBar::handle:horizontal {
                background: #4b5563;
                border-radius: 5px;
            }
            """
        )

        self.lbl_aba_anual_titulo = QLabel("Tabela: aba_anual")
        self.lbl_aba_anual_titulo.setStyleSheet(
            "QLabel { font-weight: bold; color: #f8fafc; background: #1f2a44; border: 1px solid #334155; border-radius: 4px; padding: 6px 10px; }"
        )
        layout.addWidget(self.lbl_aba_anual_titulo)

        toolbar = QHBoxLayout()
        self.btn_refresh_aba_anual = QPushButton("Recarregar")
        self.btn_refresh_aba_anual.setIcon(
            QApplication.style().standardIcon(
                QApplication.style().StandardPixmap.SP_BrowserReload
            )
        )
        self.btn_apply_aba_anual_filters = QPushButton("Aplicar filtros")
        self.btn_clear_aba_anual_filters = QPushButton("Limpar filtros")
        self.btn_filtrar_estoque_anual = QPushButton("Filtrar Estoque (SeleCAo)")
        self.btn_limpar_filtro_cruzado = QPushButton("Limpar Filtro Cruzado")
        self.btn_export_aba_anual = QPushButton("Exportar Excel")
        self.btn_destacar_aba_anual = self._criar_botao_destacar()

        toolbar.addWidget(self.btn_refresh_aba_anual)
        toolbar.addWidget(self.btn_apply_aba_anual_filters)
        toolbar.addWidget(self.btn_clear_aba_anual_filters)
        toolbar.addStretch()
        toolbar.addWidget(self.btn_filtrar_estoque_anual)
        toolbar.addWidget(self.btn_limpar_filtro_cruzado)
        toolbar.addWidget(self.btn_destacar_aba_anual)
        toolbar.addWidget(self.btn_export_aba_anual)
        layout.addLayout(toolbar)

        filtros = QHBoxLayout()
        self.anual_filter_id = QComboBox()
        self.anual_filter_id.setEditable(True)
        self.anual_filter_id.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
        self.anual_filter_id.setMinimumWidth(220)
        self.anual_filter_id.lineEdit().setPlaceholderText("Filtrar id_agregado")
        self.anual_filter_desc = QLineEdit()
        self.anual_filter_desc.setPlaceholderText("Filtrar descriCAo")
        self.anual_filter_ano = QComboBox()
        self.anual_filter_ano.addItem("Todos")
        self.anual_filter_ano.setMinimumWidth(100)
        self.anual_filter_texto = QLineEdit()
        self.anual_filter_texto.setPlaceholderText("Busca ampla...")

        for widget in [
            self.anual_filter_id,
            self.anual_filter_desc,
            self.anual_filter_ano,
            self.anual_filter_texto,
        ]:
            filtros.addWidget(widget)
        layout.addLayout(filtros)

        filtros_avancados = QHBoxLayout()
        self.anual_filter_num_col = QComboBox()
        self.anual_filter_num_col.addItems(
            [
                "entradas_desacob",
                "saidas_desacob",
                "estoque_final_desacob",
                "saldo_final",
                "estoque_final",
            ]
        )
        self.anual_filter_num_min = QLineEdit()
        self.anual_filter_num_min.setPlaceholderText("Min numerico")
        self.anual_filter_num_max = QLineEdit()
        self.anual_filter_num_max.setPlaceholderText("Max numerico")
        self.anual_profile = QComboBox()
        self.anual_profile.addItems(["Padrao", "Auditoria", "Estoque", "Custos"])
        self.btn_anual_profile = QPushButton("Perfil")
        self.btn_anual_save_profile = QPushButton("Salvar perfil")
        self.btn_anual_colunas = QPushButton("Colunas")
        for widget in [
            QLabel("Numero"),
            self.anual_filter_num_col,
            self.anual_filter_num_min,
            self.anual_filter_num_max,
            self.anual_profile,
            self.btn_anual_profile,
            self.btn_anual_save_profile,
            self.btn_anual_colunas,
        ]:
            filtros_avancados.addWidget(widget)
        filtros_avancados.addStretch()
        layout.addLayout(filtros_avancados)

        self.lbl_aba_anual_status = QLabel(
            "Selecione um CNPJ para carregar a aba anual."
        )
        self.lbl_aba_anual_status.setStyleSheet(
            "QLabel { padding: 4px 8px; background: #101827; border: 1px solid #374151; border-radius: 4px; color: #e5e7eb; }"
        )
        layout.addWidget(self.lbl_aba_anual_status)

        self.lbl_aba_anual_filtros = QLabel("Filtros ativos: nenhum")
        self.lbl_aba_anual_filtros.setStyleSheet(
            "QLabel { padding: 4px 8px; color: #dbeafe; background: #0f1b33; border: 1px solid #334155; border-radius: 4px; }"
        )
        layout.addWidget(self.lbl_aba_anual_filtros)

        paginacao_anual = QHBoxLayout()
        self.btn_aba_anual_prev_page = QPushButton("< Anterior")
        self.btn_aba_anual_prev_page.setToolTip("Ir para a pagina anterior")
        self.btn_aba_anual_prev_page.setEnabled(False)
        self.lbl_aba_anual_page = QLabel("Pagina 1/1 | 0 linhas filtradas")
        self.lbl_aba_anual_page.setStyleSheet(
            "QLabel { padding: 4px 8px; color: #94a3b8; }"
        )
        self.btn_aba_anual_next_page = QPushButton("Proximo >")
        self.btn_aba_anual_next_page.setToolTip("Ir para a proxima pagina")
        self.btn_aba_anual_next_page.setEnabled(False)
        paginacao_anual.addWidget(self.btn_aba_anual_prev_page)
        paginacao_anual.addWidget(self.lbl_aba_anual_page)
        paginacao_anual.addStretch()
        paginacao_anual.addWidget(self.btn_aba_anual_next_page)
        layout.addLayout(paginacao_anual)

        self.aba_anual_table = QTableView()
        self.aba_anual_table.setModel(self.aba_anual_model)
        self.aba_anual_table.setSelectionBehavior(QAbstractItemView.SelectItems)
        self.aba_anual_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.aba_anual_table.setAlternatingRowColors(True)
        self.aba_anual_table.setSortingEnabled(True)
        self.aba_anual_table.setWordWrap(True)
        self.aba_anual_table.verticalHeader().setDefaultSectionSize(40)
        self.aba_anual_table.horizontalHeader().setMinimumSectionSize(40)
        self.aba_anual_table.horizontalHeader().setDefaultSectionSize(180)
        self.aba_anual_table.horizontalHeader().setMaximumSectionSize(380)
        self.aba_anual_table.horizontalHeader().setStretchLastSection(True)
        self.aba_anual_table.horizontalHeader().setSectionsMovable(True)
        self.aba_anual_table.horizontalHeader().setContextMenuPolicy(
            Qt.CustomContextMenu
        )
        self.aba_anual_table.setStyleSheet(
            "QTableView::item { padding: 4px 2px; }"
            "QTableCornerButton::section { background: #18181b; border: 1px solid #3f3f46; }"
        )
        layout.addWidget(self.aba_anual_table, 1)
        return tab
    def _build_tab_aba_periodos(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        tab.setStyleSheet(self.tab_aba_anual.styleSheet())

        self.lbl_aba_periodos_titulo = QLabel("Tabela: aba_periodos")
        self.lbl_aba_periodos_titulo.setStyleSheet(
            self.lbl_aba_anual_titulo.styleSheet()
        )
        layout.addWidget(self.lbl_aba_periodos_titulo)

        toolbar = QHBoxLayout()
        self.btn_refresh_aba_periodos = QPushButton("Recarregar")
        self.btn_refresh_aba_periodos.setIcon(
            QApplication.style().standardIcon(
                QApplication.style().StandardPixmap.SP_BrowserReload
            )
        )
        self.btn_apply_aba_periodos_filters = QPushButton("Aplicar filtros")
        self.btn_clear_aba_periodos_filters = QPushButton("Limpar filtros")
        self.btn_export_aba_periodos = QPushButton("Exportar Excel")
        self.btn_destacar_aba_periodos = self._criar_botao_destacar()

        toolbar.addWidget(self.btn_refresh_aba_periodos)
        toolbar.addWidget(self.btn_apply_aba_periodos_filters)
        toolbar.addWidget(self.btn_clear_aba_periodos_filters)
        toolbar.addStretch()
        toolbar.addWidget(self.btn_destacar_aba_periodos)
        toolbar.addWidget(self.btn_export_aba_periodos)
        layout.addLayout(toolbar)

        filtros = QHBoxLayout()
        self.periodo_filter_id = QComboBox()
        self.periodo_filter_id.setEditable(True)
        self.periodo_filter_id.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
        self.periodo_filter_id.setMinimumWidth(220)
        self.periodo_filter_id.lineEdit().setPlaceholderText("Filtrar id_agregado")
        self.periodo_filter_desc = QLineEdit()
        self.periodo_filter_desc.setPlaceholderText("Filtrar descriCAo")
        self.periodo_filter_texto = QLineEdit()
        self.periodo_filter_texto.setPlaceholderText("Busca ampla...")

        for widget in [
            self.periodo_filter_id,
            self.periodo_filter_desc,
            self.periodo_filter_texto,
        ]:
            filtros.addWidget(widget)
        layout.addLayout(filtros)

        filtros_avancados = QHBoxLayout()
        self.periodo_filter_num_col = QComboBox()
        self.periodo_filter_num_col.addItems(
            [
                "entradas_desacob",
                "saidas_desacob",
                "estoque_final_desacob",
                "saldo_final",
                "estoque_final",
            ]
        )
        self.periodo_filter_num_min = QLineEdit()
        self.periodo_filter_num_min.setPlaceholderText("Min numerico")
        self.periodo_filter_num_max = QLineEdit()
        self.periodo_filter_num_max.setPlaceholderText("Max numerico")
        self.periodo_profile = QComboBox()
        self.periodo_profile.addItems(["Padrao", "Auditoria", "Estoque", "Custos"])
        self.btn_periodo_profile = QPushButton("Perfil")
        self.btn_periodo_save_profile = QPushButton("Salvar perfil")
        self.btn_periodo_colunas = QPushButton("Colunas")
        for widget in [
            QLabel("Numero"),
            self.periodo_filter_num_col,
            self.periodo_filter_num_min,
            self.periodo_filter_num_max,
            self.periodo_profile,
            self.btn_periodo_profile,
            self.btn_periodo_save_profile,
            self.btn_periodo_colunas,
        ]:
            filtros_avancados.addWidget(widget)
        filtros_avancados.addStretch()
        layout.addLayout(filtros_avancados)

        self.lbl_aba_periodos_status = QLabel(
            "Selecione um CNPJ para carregar a aba períodos."
        )
        self.lbl_aba_periodos_status.setStyleSheet(
            self.lbl_aba_anual_status.styleSheet()
        )
        layout.addWidget(self.lbl_aba_periodos_status)

        self.lbl_aba_periodos_filtros = QLabel("Filtros ativos: nenhum")
        self.lbl_aba_periodos_filtros.setStyleSheet(
            self.lbl_aba_anual_filtros.styleSheet()
        )
        layout.addWidget(self.lbl_aba_periodos_filtros)

        paginacao_periodos = QHBoxLayout()
        self.btn_aba_periodos_prev_page = QPushButton("< Anterior")
        self.btn_aba_periodos_prev_page.setToolTip("Ir para a pagina anterior")
        self.btn_aba_periodos_prev_page.setEnabled(False)
        self.lbl_aba_periodos_page = QLabel("Pagina 1/1 | 0 linhas filtradas")
        self.lbl_aba_periodos_page.setStyleSheet("QLabel { padding: 4px 8px; color: #94a3b8; }")
        self.btn_aba_periodos_next_page = QPushButton("Proximo >")
        self.btn_aba_periodos_next_page.setToolTip("Ir para a proxima pagina")
        self.btn_aba_periodos_next_page.setEnabled(False)
        paginacao_periodos.addWidget(self.btn_aba_periodos_prev_page)
        paginacao_periodos.addWidget(self.lbl_aba_periodos_page)
        paginacao_periodos.addStretch()
        paginacao_periodos.addWidget(self.btn_aba_periodos_next_page)
        layout.addLayout(paginacao_periodos)

        self.aba_periodos_table = QTableView()
        self.aba_periodos_table.setModel(self.aba_periodos_model)
        self.aba_periodos_table.setSelectionBehavior(QAbstractItemView.SelectItems)
        self.aba_periodos_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.aba_periodos_table.setAlternatingRowColors(True)
        self.aba_periodos_table.setSortingEnabled(True)
        self.aba_periodos_table.setWordWrap(True)
        self.aba_periodos_table.setStyleSheet(self.aba_anual_table.styleSheet())
        self.aba_periodos_table.horizontalHeader().setStretchLastSection(True)
        self.aba_periodos_table.horizontalHeader().setSectionsMovable(True)
        self.aba_periodos_table.horizontalHeader().setContextMenuPolicy(
            Qt.CustomContextMenu
        )
        layout.addWidget(self.aba_periodos_table, 1)

        return tab
    def _build_tab_resumo_global(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        tab.setStyleSheet(
            """
            QWidget {
                background: #252526;
                color: #f3f4f6;
            }
            QLabel {
                color: #e5e7eb;
            }
            QPushButton {
                background: #34373d;
                color: #f9fafb;
                border: 1px solid #4b5563;
                border-radius: 4px;
                padding: 6px 10px;
            }
            QPushButton:hover {
                background: #40444b;
            }
            QPushButton:pressed {
                background: #2f3338;
            }
            QTableView {
                background: #1f1f1f;
                alternate-background-color: #262626;
                color: #f9fafb;
                gridline-color: #3f3f46;
                border: 1px solid #3f3f46;
                selection-background-color: #0e639c;
                selection-color: #ffffff;
            }
            QHeaderView::section {
                background: #18181b;
                color: #f9fafb;
                border: 1px solid #3f3f46;
                padding: 6px 8px;
                font-weight: bold;
            }
            """
        )

        self.lbl_resumo_global_titulo = QLabel("Tabela: Resumo Global (Mensal e Anual)")
        self.lbl_resumo_global_titulo.setStyleSheet(
            "QLabel { font-weight: bold; color: #f8fafc; background: #1f2a44; border: 1px solid #334155; border-radius: 4px; padding: 6px 10px; }"
        )
        layout.addWidget(self.lbl_resumo_global_titulo)

        toolbar = QHBoxLayout()
        self.btn_refresh_resumo_global = QPushButton("Atualizar Resumo Global")

        self.chk_resumo_global_so_selecionados = QCheckBox("Somente produtos selecionados")
        self.chk_resumo_global_so_selecionados.setChecked(False)
        self.chk_resumo_global_so_selecionados.setToolTip(
            "Quando marcado, consolida apenas os produtos marcados como 'Visto' "
            "na aba Produtos selecionados."
        )

        _anos = [str(a) for a in range(2015, 2031)]
        self.lbl_resumo_global_ano_ini = QLabel("Ano inicial:")
        self.cmb_resumo_global_ano_ini = QComboBox()
        self.cmb_resumo_global_ano_ini.addItems(_anos)
        self.cmb_resumo_global_ano_ini.setCurrentText("2021")
        self.lbl_resumo_global_ano_fim = QLabel("Ano final:")
        self.cmb_resumo_global_ano_fim = QComboBox()
        self.cmb_resumo_global_ano_fim.addItems(_anos)
        self.cmb_resumo_global_ano_fim.setCurrentText("2025")

        self.btn_export_resumo_global = QPushButton("Exportar Excel")

        toolbar.addWidget(self.btn_refresh_resumo_global)
        toolbar.addWidget(self.chk_resumo_global_so_selecionados)
        toolbar.addSpacing(12)
        toolbar.addWidget(self.lbl_resumo_global_ano_ini)
        toolbar.addWidget(self.cmb_resumo_global_ano_ini)
        toolbar.addWidget(self.lbl_resumo_global_ano_fim)
        toolbar.addWidget(self.cmb_resumo_global_ano_fim)
        toolbar.addStretch()
        toolbar.addWidget(self.btn_export_resumo_global)
        layout.addLayout(toolbar)

        self.lbl_resumo_global_status = QLabel(
            "Aguardando carregamento da aba mensal e anual..."
        )
        self.lbl_resumo_global_status.setStyleSheet(
            "QLabel { padding: 4px 8px; background: #101827; border: 1px solid #374151; border-radius: 4px; color: #e5e7eb; }"
        )
        layout.addWidget(self.lbl_resumo_global_status)

        self.resumo_global_table = QTableView()
        self.resumo_global_table.setModel(self.resumo_global_model)
        self.resumo_global_table.setSelectionBehavior(QAbstractItemView.SelectItems)
        self.resumo_global_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.resumo_global_table.setAlternatingRowColors(True)
        self.resumo_global_table.setSortingEnabled(True)
        self.resumo_global_table.setWordWrap(True)
        self.resumo_global_table.verticalHeader().setDefaultSectionSize(40)
        self.resumo_global_table.horizontalHeader().setMinimumSectionSize(80)
        self.resumo_global_table.horizontalHeader().setDefaultSectionSize(180)
        self.resumo_global_table.horizontalHeader().setStretchLastSection(True)
        self.resumo_global_table.setStyleSheet(
            "QTableView::item { padding: 4px 2px; }"
            "QTableCornerButton::section { background: #18181b; border: 1px solid #3f3f46; }"
        )
        layout.addWidget(self.resumo_global_table, 1)

        return tab
    def _build_tab_aba_mensal(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        tab.setStyleSheet(
            """
            QWidget {
                background: #252526;
                color: #f3f4f6;
            }
            QLabel {
                color: #e5e7eb;
            }
            QLineEdit, QComboBox, QDateEdit {
                background: #323438;
                color: #f9fafb;
                border: 1px solid #4b5563;
                border-radius: 4px;
                padding: 6px 8px;
                selection-background-color: #0e639c;
                selection-color: #ffffff;
            }
            QPushButton {
                background: #34373d;
                color: #f9fafb;
                border: 1px solid #4b5563;
                border-radius: 4px;
                padding: 6px 10px;
            }
            QPushButton:hover {
                background: #40444b;
            }
            QPushButton:pressed {
                background: #2f3338;
            }
            QTableView {
                background: #1f1f1f;
                alternate-background-color: #262626;
                color: #f9fafb;
                gridline-color: #3f3f46;
                border: 1px solid #3f3f46;
                selection-background-color: #0e639c;
                selection-color: #ffffff;
            }
            QHeaderView::section {
                background: #18181b;
                color: #f9fafb;
                border: 1px solid #3f3f46;
                padding: 6px 8px;
                font-weight: bold;
            }
            """
        )

        self.lbl_aba_mensal_titulo = QLabel("Tabela: aba_mensal")
        self.lbl_aba_mensal_titulo.setStyleSheet(
            "QLabel { font-weight: bold; color: #f8fafc; background: #1f2a44; border: 1px solid #334155; border-radius: 4px; padding: 6px 10px; }"
        )
        layout.addWidget(self.lbl_aba_mensal_titulo)

        toolbar = QHBoxLayout()
        self.btn_refresh_aba_mensal = QPushButton("Recarregar")
        self.btn_refresh_aba_mensal.setIcon(
            QApplication.style().standardIcon(
                QApplication.style().StandardPixmap.SP_BrowserReload
            )
        )
        self.btn_apply_aba_mensal_filters = QPushButton("Aplicar filtros")
        self.btn_clear_aba_mensal_filters = QPushButton("Limpar filtros")
        self.btn_export_aba_mensal = QPushButton("Exportar Excel")
        self.btn_destacar_aba_mensal = self._criar_botao_destacar()
        toolbar.addWidget(self.btn_refresh_aba_mensal)
        toolbar.addWidget(self.btn_apply_aba_mensal_filters)
        toolbar.addWidget(self.btn_clear_aba_mensal_filters)
        toolbar.addStretch()
        toolbar.addWidget(self.btn_destacar_aba_mensal)
        toolbar.addWidget(self.btn_export_aba_mensal)
        layout.addLayout(toolbar)

        filtros = QHBoxLayout()
        self.mensal_filter_id = QComboBox()
        self.mensal_filter_id.setEditable(True)
        self.mensal_filter_id.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
        self.mensal_filter_id.setMinimumWidth(220)
        self.mensal_filter_id.lineEdit().setPlaceholderText("Filtrar id_agregado")
        self.mensal_filter_desc = QLineEdit()
        self.mensal_filter_desc.setPlaceholderText("Filtrar descricao")
        self.mensal_filter_ano = QComboBox()
        self.mensal_filter_ano.addItem("Todos")
        self.mensal_filter_ano.setMinimumWidth(100)
        self.mensal_filter_ano.setToolTip("Filtrar por ano")
        self.mensal_filter_mes = QComboBox()
        self.mensal_filter_mes.addItems(["Todos"] + [str(i) for i in range(1, 13)])
        self.mensal_filter_mes.setMinimumWidth(100)
        self.mensal_filter_mes.setToolTip("Filtrar por mes")
        self.mensal_filter_texto = QLineEdit()
        self.mensal_filter_texto.setPlaceholderText("Busca ampla...")
        for widget in [
            self.mensal_filter_id,
            self.mensal_filter_desc,
            QLabel("Ano"),
            self.mensal_filter_ano,
            QLabel("Mes"),
            self.mensal_filter_mes,
            self.mensal_filter_texto,
        ]:
            filtros.addWidget(widget)
        layout.addLayout(filtros)

        filtros_avancados = QHBoxLayout()
        self.mensal_filter_num_col = QComboBox()
        self.mensal_filter_num_col.addItems(
            [
                "valor_entradas",
                "qtd_entradas",
                "pme_mes",
                "valor_saidas",
                "qtd_saidas",
                "pms_mes",
                "entradas_desacob",
                "ICMS_entr_desacob",
                "saldo_mes",
                "custo_medio_mes",
                "valor_estoque",
            ]
        )
        self.mensal_filter_num_min = QLineEdit()
        self.mensal_filter_num_min.setPlaceholderText("Min numerico")
        self.mensal_filter_num_max = QLineEdit()
        self.mensal_filter_num_max.setPlaceholderText("Max numerico")
        self.mensal_profile = QComboBox()
        self.mensal_profile.addItems(["Padrao", "Auditoria", "Estoque", "Custos"])
        self.btn_mensal_profile = QPushButton("Perfil")
        self.btn_mensal_save_profile = QPushButton("Salvar perfil")
        self.btn_mensal_colunas = QPushButton("Colunas")
        for widget in [
            QLabel("Numero"),
            self.mensal_filter_num_col,
            self.mensal_filter_num_min,
            self.mensal_filter_num_max,
            self.mensal_profile,
            self.btn_mensal_profile,
            self.btn_mensal_save_profile,
            self.btn_mensal_colunas,
        ]:
            filtros_avancados.addWidget(widget)
        filtros_avancados.addStretch()
        layout.addLayout(filtros_avancados)

        self.lbl_aba_mensal_status = QLabel(
            "Selecione um CNPJ para carregar a aba mensal."
        )
        self.lbl_aba_mensal_status.setStyleSheet(
            "QLabel { padding: 4px 8px; background: #101827; border: 1px solid #374151; border-radius: 4px; color: #e5e7eb; }"
        )
        layout.addWidget(self.lbl_aba_mensal_status)

        self.lbl_aba_mensal_filtros = QLabel("Filtros ativos: nenhum")
        self.lbl_aba_mensal_filtros.setStyleSheet(
            "QLabel { padding: 4px 8px; color: #dbeafe; background: #0f1b33; border: 1px solid #334155; border-radius: 4px; }"
        )
        layout.addWidget(self.lbl_aba_mensal_filtros)

        paginacao_mensal = QHBoxLayout()
        self.btn_aba_mensal_prev_page = QPushButton("< Anterior")
        self.btn_aba_mensal_prev_page.setToolTip("Ir para a pagina anterior")
        self.btn_aba_mensal_prev_page.setEnabled(False)
        self.lbl_aba_mensal_page = QLabel("Pagina 1/1 | 0 linhas filtradas")
        self.lbl_aba_mensal_page.setStyleSheet(
            "QLabel { padding: 4px 8px; color: #94a3b8; }"
        )
        self.btn_aba_mensal_next_page = QPushButton("Proximo >")
        self.btn_aba_mensal_next_page.setToolTip("Ir para a proxima pagina")
        self.btn_aba_mensal_next_page.setEnabled(False)
        paginacao_mensal.addWidget(self.btn_aba_mensal_prev_page)
        paginacao_mensal.addWidget(self.lbl_aba_mensal_page)
        paginacao_mensal.addStretch()
        paginacao_mensal.addWidget(self.btn_aba_mensal_next_page)
        layout.addLayout(paginacao_mensal)

        self.aba_mensal_table = QTableView()
        self.aba_mensal_table.setModel(self.aba_mensal_model)
        self.aba_mensal_table.setSelectionBehavior(QAbstractItemView.SelectItems)
        self.aba_mensal_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.aba_mensal_table.setAlternatingRowColors(True)
        self.aba_mensal_table.setSortingEnabled(True)
        self.aba_mensal_table.setWordWrap(True)
        self.aba_mensal_table.verticalHeader().setDefaultSectionSize(40)
        self.aba_mensal_table.horizontalHeader().setMinimumSectionSize(40)
        self.aba_mensal_table.horizontalHeader().setDefaultSectionSize(170)
        self.aba_mensal_table.horizontalHeader().setMaximumSectionSize(380)
        self.aba_mensal_table.horizontalHeader().setStretchLastSection(True)
        self.aba_mensal_table.horizontalHeader().setSectionsMovable(True)
        self.aba_mensal_table.horizontalHeader().setContextMenuPolicy(
            Qt.CustomContextMenu
        )
        self.aba_mensal_table.setStyleSheet(
            "QTableView::item { padding: 4px 2px; }"
            "QTableCornerButton::section { background: #18181b; border: 1px solid #3f3f46; }"
        )
        layout.addWidget(self.aba_mensal_table, 1)
        return tab

    def _build_tab_aba_codigo_original(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        tab.setStyleSheet(self.tab_aba_anual.styleSheet())

        self.lbl_aba_codigo_original_titulo = QLabel("Tabela: aba_codigo_original")
        self.lbl_aba_codigo_original_titulo.setStyleSheet(
            self.lbl_aba_anual_titulo.styleSheet()
        )
        layout.addWidget(self.lbl_aba_codigo_original_titulo)

        toolbar = QHBoxLayout()
        self.btn_refresh_aba_codigo_original = QPushButton("Recarregar")
        self.btn_refresh_aba_codigo_original.setIcon(
            QApplication.style().standardIcon(
                QApplication.style().StandardPixmap.SP_BrowserReload
            )
        )
        self.btn_apply_aba_codigo_original_filters = QPushButton("Aplicar filtros")
        self.btn_clear_aba_codigo_original_filters = QPushButton("Limpar filtros")
        self.btn_export_aba_codigo_original = QPushButton("Exportar Excel")
        self.btn_destacar_aba_codigo_original = self._criar_botao_destacar()
        self.cod_original_profile = QComboBox()
        self.cod_original_profile.addItems(["Padrao", "Auditoria", "Estoque", "Custos"])
        self.btn_cod_original_profile = QPushButton("Perfil")
        self.btn_cod_original_save_profile = QPushButton("Salvar perfil")
        self.btn_cod_original_colunas = QPushButton("Colunas")

        toolbar.addWidget(self.btn_refresh_aba_codigo_original)
        toolbar.addWidget(self.btn_apply_aba_codigo_original_filters)
        toolbar.addWidget(self.btn_clear_aba_codigo_original_filters)
        toolbar.addStretch()
        toolbar.addWidget(self.cod_original_profile)
        toolbar.addWidget(self.btn_cod_original_profile)
        toolbar.addWidget(self.btn_cod_original_save_profile)
        toolbar.addWidget(self.btn_cod_original_colunas)
        toolbar.addWidget(self.btn_destacar_aba_codigo_original)
        toolbar.addWidget(self.btn_export_aba_codigo_original)
        layout.addLayout(toolbar)

        filtros = QHBoxLayout()
        self.cod_original_filter_cod = QComboBox()
        self.cod_original_filter_cod.setEditable(True)
        self.cod_original_filter_cod.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
        self.cod_original_filter_cod.setMinimumWidth(220)
        self.cod_original_filter_cod.lineEdit().setPlaceholderText("Filtrar Cod_item")
        self.cod_original_filter_desc = QLineEdit()
        self.cod_original_filter_desc.setPlaceholderText("Filtrar descricao")
        self.cod_original_filter_ano = QComboBox()
        self.cod_original_filter_ano.addItem("Todos")
        self.cod_original_filter_mes = QComboBox()
        self.cod_original_filter_mes.addItems(
            ["Todos"] + [str(m) for m in range(1, 13)]
        )
        self.cod_original_filter_texto = QLineEdit()
        self.cod_original_filter_texto.setPlaceholderText("Busca ampla...")
        self.cod_original_filter_num_col = QComboBox()
        self.cod_original_filter_num_col.addItems(
            ["valor_entradas", "qtd_entradas", "valor_saidas", "qtd_saidas", "saldo_mes", "valor_estoque"]
        )
        self.cod_original_filter_num_min = QLineEdit()
        self.cod_original_filter_num_min.setPlaceholderText("Min numerico")
        self.cod_original_filter_num_max = QLineEdit()
        self.cod_original_filter_num_max.setPlaceholderText("Max numerico")

        for widget in [
            self.cod_original_filter_cod,
            self.cod_original_filter_desc,
            QLabel("Ano"),
            self.cod_original_filter_ano,
            QLabel("Mes"),
            self.cod_original_filter_mes,
            self.cod_original_filter_texto,
            QLabel("Numero"),
            self.cod_original_filter_num_col,
            self.cod_original_filter_num_min,
            self.cod_original_filter_num_max,
        ]:
            filtros.addWidget(widget)
        filtros.addStretch()
        layout.addLayout(filtros)

        self.lbl_aba_codigo_original_status = QLabel(
            "Selecione um CNPJ para carregar o resumo por codigo original."
        )
        self.lbl_aba_codigo_original_status.setStyleSheet(
            self.lbl_aba_anual_status.styleSheet()
        )
        layout.addWidget(self.lbl_aba_codigo_original_status)

        self.lbl_aba_codigo_original_filtros = QLabel("Filtros ativos: nenhum")
        self.lbl_aba_codigo_original_filtros.setStyleSheet(
            self.lbl_aba_anual_filtros.styleSheet()
        )
        layout.addWidget(self.lbl_aba_codigo_original_filtros)

        self.aba_codigo_original_table = QTableView()
        self.aba_codigo_original_table.setModel(self.aba_codigo_original_model)
        self.aba_codigo_original_table.setSelectionBehavior(QAbstractItemView.SelectItems)
        self.aba_codigo_original_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.aba_codigo_original_table.setAlternatingRowColors(True)
        self.aba_codigo_original_table.setSortingEnabled(True)
        self.aba_codigo_original_table.setWordWrap(True)
        self.aba_codigo_original_table.setStyleSheet(self.aba_anual_table.styleSheet())
        self.aba_codigo_original_table.horizontalHeader().setMinimumSectionSize(40)
        self.aba_codigo_original_table.horizontalHeader().setDefaultSectionSize(170)
        self.aba_codigo_original_table.horizontalHeader().setMaximumSectionSize(380)
        self.aba_codigo_original_table.horizontalHeader().setStretchLastSection(True)
        self.aba_codigo_original_table.horizontalHeader().setSectionsMovable(True)
        self.aba_codigo_original_table.horizontalHeader().setContextMenuPolicy(
            Qt.CustomContextMenu
        )
        self.aba_codigo_original_table.setStyleSheet(
            "QTableView::item { padding: 4px 2px; }"
            "QTableCornerButton::section { background: #18181b; border: 1px solid #3f3f46; }"
        )
        layout.addWidget(self.aba_codigo_original_table, 1)
        return tab
