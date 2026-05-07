from __future__ import annotations

from PySide6.QtCore import QDate, Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QComboBox,
    QDateEdit,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QPushButton,
    QTableView,
    QToolBar,
    QVBoxLayout,
    QWidget,
)


class AuditoriaWindowMixin:
    def _build_tab_consulta(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)

        filter_box = QGroupBox("Filtros")
        filter_layout = QVBoxLayout(filter_box)
        form = QHBoxLayout()
        self.filter_column = QComboBox()
        self.filter_operator = QComboBox()
        self.filter_operator.addItems(
            [
                "contem",
                "igual",
                "comeca com",
                "termina com",
                ">",
                ">=",
                "<",
                "<=",
                "e nulo",
                "nao e nulo",
            ]
        )
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
        self.consulta_profile = QComboBox()
        self.consulta_profile.addItems(["Padrao", "Auditoria", "Estoque", "Custos"])
        self.btn_apply_consulta_profile = QPushButton("Aplicar perfil")
        self.btn_save_consulta_profile = QPushButton("Salvar perfil")
        self.btn_consulta_destacar = self._criar_botao_destacar()
        self.btn_prev_page = QPushButton("Pagina anterior")
        self.btn_prev_page.setToolTip("Ir para a pagina anterior")
        self.btn_next_page = QPushButton("Proxima pagina")
        self.btn_next_page.setToolTip("Ir para a proxima pagina")
        self.lbl_page = QLabel("Pagina 0/0")
        filter_actions.addWidget(self.btn_remove_filter)
        filter_actions.addWidget(self.btn_choose_columns)
        filter_actions.addWidget(self.consulta_profile)
        filter_actions.addWidget(self.btn_apply_consulta_profile)
        filter_actions.addWidget(self.btn_save_consulta_profile)
        filter_actions.addWidget(self.btn_consulta_destacar)
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
        self.table_view.customContextMenuRequested.connect(
            lambda pos: self._abrir_menu_contexto_celula(
                "consulta", self.table_view, pos
            )
        )
        self.table_view.horizontalHeader().setSectionsMovable(True)
        self.table_view.horizontalHeader().setContextMenuPolicy(Qt.CustomContextMenu)
        layout.addWidget(self.table_view, 1)
        return tab
    def _build_tab_mov_estoque(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)

        self.lbl_mov_estoque_titulo = QLabel("Tabela: mov_estoque")
        self.lbl_mov_estoque_titulo.setStyleSheet(
            "QLabel { font-weight: bold; color: #e2e8f0; background: #1e293b; border: 1px solid #334155; border-radius: 4px; padding: 6px 10px; }"
        )
        layout.addWidget(self.lbl_mov_estoque_titulo)

        filtros = QHBoxLayout()
        self.mov_filter_id = QComboBox()
        self.mov_filter_id.setEditable(True)
        self.mov_filter_id.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
        self.mov_filter_id.setMinimumWidth(250)
        self.mov_filter_id.lineEdit().setPlaceholderText("Filtrar id_agrupado")

        self.mov_filter_desc = QLineEdit()
        self.mov_filter_desc.setPlaceholderText("Filtrar descriCAo")

        self.mov_filter_ncm = QLineEdit()
        self.mov_filter_ncm.setPlaceholderText("Filtrar NCM")

        self.mov_filter_tipo = QComboBox()
        self.mov_filter_tipo.addItems(["Todos", "Entradas", "Saidas"])

        self.mov_filter_texto = QLineEdit()
        self.mov_filter_texto.setPlaceholderText("Busca geral...")

        for widget in [
            self.mov_filter_id,
            self.mov_filter_desc,
            self.mov_filter_ncm,
            self.mov_filter_tipo,
            self.mov_filter_texto,
        ]:
            filtros.addWidget(widget)
        layout.addLayout(filtros)

        filtros_avancados = QHBoxLayout()
        self.mov_filter_data_col = QComboBox()
        self.mov_filter_data_col.addItems(["Dt_doc", "Dt_e_s"])
        self.mov_filter_data_ini = QDateEdit()
        self.mov_filter_data_ini.setCalendarPopup(True)
        self.mov_filter_data_ini.setDisplayFormat("dd/MM/yyyy")
        self.mov_filter_data_ini.setSpecialValueText("Data inicial")
        self.mov_filter_data_ini.setMinimumDate(QDate(1900, 1, 1))
        self.mov_filter_data_ini.setDate(self.mov_filter_data_ini.minimumDate())
        self.mov_filter_data_fim = QDateEdit()
        self.mov_filter_data_fim.setCalendarPopup(True)
        self.mov_filter_data_fim.setDisplayFormat("dd/MM/yyyy")
        self.mov_filter_data_fim.setSpecialValueText("Data final")
        self.mov_filter_data_fim.setMinimumDate(QDate(1900, 1, 1))
        self.mov_filter_data_fim.setDate(self.mov_filter_data_fim.minimumDate())
        self.mov_filter_num_col = QComboBox()
        self.mov_filter_num_col.addItems(
            [
                "saldo_estoque_anual",
                "custo_medio_anual",
                "entr_desac_anual",
                "q_conv",
                "preco_item",
                "preco_unit",
            ]
        )
        self.mov_filter_num_min = QLineEdit()
        self.mov_filter_num_min.setPlaceholderText("Min numerico")
        self.mov_filter_num_max = QLineEdit()
        self.mov_filter_num_max.setPlaceholderText("Max numerico")
        self.mov_profile = QComboBox()
        self.mov_profile.addItems(
            [
                "Padrao",
                "Contribuinte",
                "Auditoria",
                "Auditoria Fiscal",
                "Estoque",
                "Custos",
            ]
        )
        self.btn_mov_profile = QPushButton("Perfil")
        self.btn_mov_save_profile = QPushButton("Salvar perfil")
        self.btn_mov_colunas = QPushButton("Colunas")
        self.btn_mov_destacar = self._criar_botao_destacar()
        self.btn_export_mov_estoque = QPushButton("Exportar Excel")
        for widget in [
            QLabel("Data"),
            self.mov_filter_data_col,
            self.mov_filter_data_ini,
            self.mov_filter_data_fim,
            QLabel("Numero"),
            self.mov_filter_num_col,
            self.mov_filter_num_min,
            self.mov_filter_num_max,
            self.mov_profile,
            self.btn_mov_profile,
            self.btn_mov_save_profile,
            self.btn_mov_colunas,
            self.btn_mov_destacar,
            self.btn_export_mov_estoque,
        ]:
            filtros_avancados.addWidget(widget)
        layout.addLayout(filtros_avancados)

        self.lbl_mov_estoque_status = QLabel(
            "Selecione um CNPJ para carregar as movimentacoes."
        )
        self.lbl_mov_estoque_status.setStyleSheet(
            "QLabel { padding: 4px; color: #475569; }"
        )
        layout.addWidget(self.lbl_mov_estoque_status)

        self.lbl_mov_estoque_filtros = QLabel("Filtros ativos: nenhum")
        self.lbl_mov_estoque_filtros.setStyleSheet(
            "QLabel { padding: 4px 8px; color: #cbd5e1; background: #0f172a; border: 1px solid #334155; border-radius: 4px; }"
        )
        layout.addWidget(self.lbl_mov_estoque_filtros)

        paginacao_mov = QHBoxLayout()
        self.btn_mov_estoque_prev_page = QPushButton("< Anterior")
        self.btn_mov_estoque_prev_page.setToolTip("Ir para a pagina anterior")
        self.btn_mov_estoque_prev_page.setEnabled(False)
        self.lbl_mov_estoque_page = QLabel("Pagina 1/1 | 0 linhas filtradas")
        self.lbl_mov_estoque_page.setStyleSheet(
            "QLabel { padding: 4px 8px; color: #94a3b8; }"
        )
        self.btn_mov_estoque_next_page = QPushButton("Proximo >")
        self.btn_mov_estoque_next_page.setToolTip("Ir para a proxima pagina")
        self.btn_mov_estoque_next_page.setEnabled(False)
        paginacao_mov.addWidget(self.btn_mov_estoque_prev_page)
        paginacao_mov.addWidget(self.lbl_mov_estoque_page)
        paginacao_mov.addStretch()
        paginacao_mov.addWidget(self.btn_mov_estoque_next_page)
        layout.addLayout(paginacao_mov)

        self.mov_estoque_table = QTableView()
        self.mov_estoque_table.setModel(self.mov_estoque_model)
        self.mov_estoque_table.setSelectionBehavior(QAbstractItemView.SelectItems)
        self.mov_estoque_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.mov_estoque_table.setAlternatingRowColors(True)
        self.mov_estoque_table.setSortingEnabled(True)
        self.mov_estoque_table.setWordWrap(True)
        self.mov_estoque_table.verticalHeader().setDefaultSectionSize(40)
        self.mov_estoque_table.horizontalHeader().setMinimumSectionSize(40)
        self.mov_estoque_table.horizontalHeader().setDefaultSectionSize(110)
        self.mov_estoque_table.horizontalHeader().setMaximumSectionSize(400)
        self.mov_estoque_table.horizontalHeader().setSectionsMovable(True)
        self.mov_estoque_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.mov_estoque_table.customContextMenuRequested.connect(
            lambda pos: self._abrir_menu_contexto_celula(
                "mov_estoque", self.mov_estoque_table, pos
            )
        )
        self.mov_estoque_table.horizontalHeader().setContextMenuPolicy(
            Qt.CustomContextMenu
        )
        layout.addWidget(self.mov_estoque_table)

        return tab
    def _build_tab_nfe_entrada(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)

        self.lbl_nfe_entrada_titulo = QLabel("Tabela: nfe_entrada")
        self.lbl_nfe_entrada_titulo.setStyleSheet(
            "QLabel { font-weight: bold; color: #f8fafc; background: #1f2a44; border: 1px solid #334155; border-radius: 4px; padding: 6px 10px; }"
        )
        layout.addWidget(self.lbl_nfe_entrada_titulo)

        toolbar = QHBoxLayout()
        self.btn_extract_nfe_entrada = QPushButton("Extrair")
        self.btn_refresh_nfe_entrada = QPushButton("Recarregar")
        self.btn_apply_nfe_entrada_filters = QPushButton("Aplicar filtros")
        self.btn_clear_nfe_entrada_filters = QPushButton("Limpar filtros")
        self.nfe_entrada_profile = QComboBox()
        self.nfe_entrada_profile.addItems(["Padrao", "Auditoria", "Estoque", "Custos"])
        self.btn_nfe_entrada_profile = QPushButton("Perfil")
        self.btn_nfe_entrada_save_profile = QPushButton("Salvar perfil")
        self.btn_nfe_entrada_colunas = QPushButton("Colunas")
        self.btn_nfe_entrada_destacar = self._criar_botao_destacar()
        self.btn_export_nfe_entrada = QPushButton("Exportar Excel")
        for widget in [
            self.btn_extract_nfe_entrada,
            self.btn_refresh_nfe_entrada,
            self.btn_apply_nfe_entrada_filters,
            self.btn_clear_nfe_entrada_filters,
            self.nfe_entrada_profile,
            self.btn_nfe_entrada_profile,
            self.btn_nfe_entrada_save_profile,
            self.btn_nfe_entrada_colunas,
        ]:
            toolbar.addWidget(widget)
        toolbar.addStretch()
        toolbar.addWidget(self.btn_nfe_entrada_destacar)
        toolbar.addWidget(self.btn_export_nfe_entrada)
        layout.addLayout(toolbar)

        filtros = QHBoxLayout()
        self.nfe_entrada_filter_id = QComboBox()
        self.nfe_entrada_filter_id.setEditable(True)
        self.nfe_entrada_filter_id.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
        self.nfe_entrada_filter_id.setMinimumWidth(220)
        self.nfe_entrada_filter_id.lineEdit().setPlaceholderText("Filtrar id_agrupado")
        self.nfe_entrada_filter_desc = QLineEdit()
        self.nfe_entrada_filter_desc.setPlaceholderText("Filtrar descricao")
        self.nfe_entrada_filter_ncm = QLineEdit()
        self.nfe_entrada_filter_ncm.setPlaceholderText("Filtrar NCM")
        self.nfe_entrada_filter_sefin = QLineEdit()
        self.nfe_entrada_filter_sefin.setPlaceholderText("Filtrar co_sefin")
        self.nfe_entrada_filter_texto = QLineEdit()
        self.nfe_entrada_filter_texto.setPlaceholderText("Busca ampla...")
        for widget in [
            self.nfe_entrada_filter_id,
            self.nfe_entrada_filter_desc,
            self.nfe_entrada_filter_ncm,
            self.nfe_entrada_filter_sefin,
            self.nfe_entrada_filter_texto,
        ]:
            filtros.addWidget(widget)
        layout.addLayout(filtros)

        filtros_datas = QHBoxLayout()
        self.nfe_entrada_filter_data_ini = QDateEdit()
        self.nfe_entrada_filter_data_ini.setCalendarPopup(True)
        self.nfe_entrada_filter_data_ini.setDisplayFormat("dd/MM/yyyy")
        self.nfe_entrada_filter_data_ini.setSpecialValueText("Data inicial")
        self.nfe_entrada_filter_data_ini.setMinimumDate(QDate(1900, 1, 1))
        self.nfe_entrada_filter_data_ini.setDate(
            self.nfe_entrada_filter_data_ini.minimumDate()
        )
        self.nfe_entrada_filter_data_fim = QDateEdit()
        self.nfe_entrada_filter_data_fim.setCalendarPopup(True)
        self.nfe_entrada_filter_data_fim.setDisplayFormat("dd/MM/yyyy")
        self.nfe_entrada_filter_data_fim.setSpecialValueText("Data final")
        self.nfe_entrada_filter_data_fim.setMinimumDate(QDate(1900, 1, 1))
        self.nfe_entrada_filter_data_fim.setDate(
            self.nfe_entrada_filter_data_fim.minimumDate()
        )
        filtros_datas.addWidget(QLabel("Data inicial"))
        filtros_datas.addWidget(self.nfe_entrada_filter_data_ini)
        filtros_datas.addWidget(QLabel("Data final"))
        filtros_datas.addWidget(self.nfe_entrada_filter_data_fim)
        filtros_datas.addStretch()
        layout.addLayout(filtros_datas)

        self.lbl_nfe_entrada_status = QLabel(
            "Selecione um CNPJ para carregar as NFes de entrada."
        )
        self.lbl_nfe_entrada_status.setStyleSheet(
            "QLabel { padding: 4px 8px; background: #101827; border: 1px solid #374151; border-radius: 4px; color: #e5e7eb; }"
        )
        layout.addWidget(self.lbl_nfe_entrada_status)

        self.lbl_nfe_entrada_filtros = QLabel("Filtros ativos: nenhum")
        self.lbl_nfe_entrada_filtros.setStyleSheet(
            "QLabel { padding: 4px 8px; color: #dbeafe; background: #0f1b33; border: 1px solid #334155; border-radius: 4px; }"
        )
        layout.addWidget(self.lbl_nfe_entrada_filtros)

        self.nfe_entrada_table = QTableView()
        self.nfe_entrada_table.setModel(self.nfe_entrada_model)
        self.nfe_entrada_table.setSelectionBehavior(QAbstractItemView.SelectItems)
        self.nfe_entrada_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.nfe_entrada_table.setAlternatingRowColors(True)
        self.nfe_entrada_table.setSortingEnabled(True)
        self.nfe_entrada_table.setWordWrap(True)
        self.nfe_entrada_table.verticalHeader().setDefaultSectionSize(40)
        self.nfe_entrada_table.horizontalHeader().setMinimumSectionSize(40)
        self.nfe_entrada_table.horizontalHeader().setDefaultSectionSize(170)
        self.nfe_entrada_table.horizontalHeader().setMaximumSectionSize(420)
        self.nfe_entrada_table.horizontalHeader().setSectionsMovable(True)
        self.nfe_entrada_table.horizontalHeader().setContextMenuPolicy(
            Qt.CustomContextMenu
        )
        self.nfe_entrada_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.nfe_entrada_table.customContextMenuRequested.connect(
            lambda pos: self._abrir_menu_contexto_celula(
                "nfe_entrada", self.nfe_entrada_table, pos
            )
        )
        layout.addWidget(self.nfe_entrada_table, 1)
        return tab
