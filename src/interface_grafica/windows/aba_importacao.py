from PySide6.QtCore import QDate, Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QCheckBox,
    QComboBox,
    QDateEdit,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QTableView,
    QVBoxLayout,
    QWidget,
)


class AbaImportacao:
    """Construtores das abas operacionais de importacao/entrada."""

    def __init__(self, main_window):
        self.main = main_window

    def build_conversao(self) -> QWidget:
        main = self.main
        tab = QWidget()
        layout = QVBoxLayout(tab)

        toolbar = QHBoxLayout()
        main.btn_refresh_conversao = QPushButton("Recarregar")
        main.btn_refresh_conversao.setIcon(
            QApplication.style().standardIcon(
                QApplication.style().StandardPixmap.SP_BrowserReload
            )
        )
        main.chk_show_single_unit = QCheckBox("Mostrar itens de unidade unica")
        main.chk_show_single_unit.setChecked(False)
        main.btn_export_conversao = QPushButton("Exportar Excel")
        main.btn_import_conversao = QPushButton("Importar Excel")
        main.btn_conversao_destacar = main._criar_botao_destacar()
        main.btn_recalcular_fatores = main._criar_botao_destacar("Recalcular fatores")
        main.btn_recalcular_fatores.setEnabled(False)
        main.conversao_profile = QComboBox()
        main.conversao_profile.addItems(["Padrao", "Auditoria", "Estoque", "Custos"])
        main.btn_apply_conversao_profile = QPushButton("Perfil")
        main.btn_save_conversao_profile = QPushButton("Salvar perfil")
        main.btn_conversao_colunas = QPushButton("Colunas")

        toolbar.addWidget(main.btn_refresh_conversao)
        toolbar.addWidget(main.chk_show_single_unit)
        toolbar.addStretch()
        toolbar.addWidget(main.btn_recalcular_fatores)
        toolbar.addWidget(main.conversao_profile)
        toolbar.addWidget(main.btn_apply_conversao_profile)
        toolbar.addWidget(main.btn_save_conversao_profile)
        toolbar.addWidget(main.btn_conversao_colunas)
        toolbar.addWidget(main.btn_conversao_destacar)
        toolbar.addWidget(main.btn_import_conversao)
        layout.addLayout(toolbar)

        filtros = QHBoxLayout()
        main.conv_filter_id = QComboBox()
        main.conv_filter_id.setEditable(True)
        main.conv_filter_id.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
        main.conv_filter_id.setMinimumWidth(220)
        main.conv_filter_id.lineEdit().setPlaceholderText("Filtrar id_agrupado")
        main.conv_filter_desc = QLineEdit()
        main.conv_filter_desc.setPlaceholderText("Filtrar descr_padrao")
        filtros.addWidget(main.conv_filter_id)
        filtros.addWidget(main.conv_filter_desc)
        layout.addLayout(filtros)

        main.panel_unid_ref = QGroupBox(
            "Alterar Unidade de Referencia do Produto Selecionado"
        )
        panel_layout = QHBoxLayout(main.panel_unid_ref)
        main.lbl_produto_sel = QLabel("Nenhum produto selecionado")
        main.lbl_produto_sel.setStyleSheet("font-weight: bold; color: #1e40af;")
        main.combo_unid_ref = QComboBox()
        main.btn_apply_unid_ref = QPushButton("Aplicar a todos os itens")
        main.btn_apply_unid_ref.setStyleSheet("font-weight: bold;")
        main.btn_apply_unid_ref.setEnabled(False)
        main.combo_unid_ref.setEnabled(False)
        panel_layout.addWidget(main.lbl_produto_sel)
        panel_layout.addWidget(QLabel("   -> Nova unid_ref:"))
        panel_layout.addWidget(main.combo_unid_ref)
        panel_layout.addWidget(main.btn_apply_unid_ref)
        panel_layout.addStretch()
        layout.addWidget(main.panel_unid_ref)

        main.conversion_table = QTableView()
        main.conversion_table.setModel(main.conversion_model)
        main.conversion_table.setAlternatingRowColors(True)
        main.conversion_table.setSelectionBehavior(QAbstractItemView.SelectItems)
        main.conversion_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        main.conversion_table.setSortingEnabled(True)
        main.conversion_table.horizontalHeader().setSectionsMovable(True)
        main.conversion_table.horizontalHeader().setContextMenuPolicy(
            Qt.CustomContextMenu
        )
        layout.addWidget(main.conversion_table)

        return tab

    def build_nfe_entrada(self) -> QWidget:
        main = self.main
        tab = QWidget()
        layout = QVBoxLayout(tab)

        main.lbl_nfe_entrada_titulo = QLabel("Tabela: nfe_entrada")
        main.lbl_nfe_entrada_titulo.setStyleSheet(
            "QLabel { font-weight: bold; color: #f8fafc; background: #1f2a44; border: 1px solid #334155; border-radius: 4px; padding: 6px 10px; }"
        )
        layout.addWidget(main.lbl_nfe_entrada_titulo)

        toolbar = QHBoxLayout()
        main.btn_extract_nfe_entrada = QPushButton("Extrair")
        main.btn_refresh_nfe_entrada = QPushButton("Recarregar")
        main.btn_apply_nfe_entrada_filters = QPushButton("Aplicar filtros")
        main.btn_clear_nfe_entrada_filters = QPushButton("Limpar filtros")
        main.nfe_entrada_profile = QComboBox()
        main.nfe_entrada_profile.addItems(["Padrao", "Auditoria", "Estoque", "Custos"])
        main.btn_nfe_entrada_profile = QPushButton("Perfil")
        main.btn_nfe_entrada_save_profile = QPushButton("Salvar perfil")
        main.btn_nfe_entrada_colunas = QPushButton("Colunas")
        main.btn_nfe_entrada_destacar = main._criar_botao_destacar()
        main.btn_export_nfe_entrada = QPushButton("Exportar Excel")
        for widget in [
            main.btn_extract_nfe_entrada,
            main.btn_refresh_nfe_entrada,
            main.btn_apply_nfe_entrada_filters,
            main.btn_clear_nfe_entrada_filters,
            main.nfe_entrada_profile,
            main.btn_nfe_entrada_profile,
            main.btn_nfe_entrada_save_profile,
            main.btn_nfe_entrada_colunas,
        ]:
            toolbar.addWidget(widget)
        toolbar.addStretch()
        toolbar.addWidget(main.btn_nfe_entrada_destacar)
        toolbar.addWidget(main.btn_export_nfe_entrada)
        layout.addLayout(toolbar)

        filtros = QHBoxLayout()
        main.nfe_entrada_filter_id = QComboBox()
        main.nfe_entrada_filter_id.setEditable(True)
        main.nfe_entrada_filter_id.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
        main.nfe_entrada_filter_id.setMinimumWidth(220)
        main.nfe_entrada_filter_id.lineEdit().setPlaceholderText("Filtrar id_agrupado")
        main.nfe_entrada_filter_desc = QLineEdit()
        main.nfe_entrada_filter_desc.setPlaceholderText("Filtrar descricao")
        main.nfe_entrada_filter_ncm = QLineEdit()
        main.nfe_entrada_filter_ncm.setPlaceholderText("Filtrar NCM")
        main.nfe_entrada_filter_sefin = QLineEdit()
        main.nfe_entrada_filter_sefin.setPlaceholderText("Filtrar co_sefin")
        main.nfe_entrada_filter_texto = QLineEdit()
        main.nfe_entrada_filter_texto.setPlaceholderText("Busca ampla...")
        for widget in [
            main.nfe_entrada_filter_id,
            main.nfe_entrada_filter_desc,
            main.nfe_entrada_filter_ncm,
            main.nfe_entrada_filter_sefin,
            main.nfe_entrada_filter_texto,
        ]:
            filtros.addWidget(widget)
        layout.addLayout(filtros)

        filtros_datas = QHBoxLayout()
        main.nfe_entrada_filter_data_ini = QDateEdit()
        main.nfe_entrada_filter_data_ini.setCalendarPopup(True)
        main.nfe_entrada_filter_data_ini.setDisplayFormat("dd/MM/yyyy")
        main.nfe_entrada_filter_data_ini.setSpecialValueText("Data inicial")
        main.nfe_entrada_filter_data_ini.setMinimumDate(QDate(1900, 1, 1))
        main.nfe_entrada_filter_data_ini.setDate(
            main.nfe_entrada_filter_data_ini.minimumDate()
        )
        main.nfe_entrada_filter_data_fim = QDateEdit()
        main.nfe_entrada_filter_data_fim.setCalendarPopup(True)
        main.nfe_entrada_filter_data_fim.setDisplayFormat("dd/MM/yyyy")
        main.nfe_entrada_filter_data_fim.setSpecialValueText("Data final")
        main.nfe_entrada_filter_data_fim.setMinimumDate(QDate(1900, 1, 1))
        main.nfe_entrada_filter_data_fim.setDate(
            main.nfe_entrada_filter_data_fim.minimumDate()
        )
        filtros_datas.addWidget(QLabel("Data inicial"))
        filtros_datas.addWidget(main.nfe_entrada_filter_data_ini)
        filtros_datas.addWidget(QLabel("Data final"))
        filtros_datas.addWidget(main.nfe_entrada_filter_data_fim)
        filtros_datas.addStretch()
        layout.addLayout(filtros_datas)

        main.lbl_nfe_entrada_status = QLabel(
            "Selecione um CNPJ para carregar as NFes de entrada."
        )
        main.lbl_nfe_entrada_status.setStyleSheet(
            "QLabel { padding: 4px 8px; background: #101827; border: 1px solid #374151; border-radius: 4px; color: #e5e7eb; }"
        )
        layout.addWidget(main.lbl_nfe_entrada_status)

        main.lbl_nfe_entrada_filtros = QLabel("Filtros ativos: nenhum")
        main.lbl_nfe_entrada_filtros.setStyleSheet(
            "QLabel { padding: 4px 8px; color: #dbeafe; background: #0f1b33; border: 1px solid #334155; border-radius: 4px; }"
        )
        layout.addWidget(main.lbl_nfe_entrada_filtros)

        main.nfe_entrada_table = QTableView()
        main.nfe_entrada_table.setModel(main.nfe_entrada_model)
        main.nfe_entrada_table.setSelectionBehavior(QAbstractItemView.SelectItems)
        main.nfe_entrada_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        main.nfe_entrada_table.setAlternatingRowColors(True)
        main.nfe_entrada_table.setSortingEnabled(True)
        main.nfe_entrada_table.setWordWrap(True)
        main.nfe_entrada_table.verticalHeader().setDefaultSectionSize(40)
        main.nfe_entrada_table.horizontalHeader().setMinimumSectionSize(40)
        main.nfe_entrada_table.horizontalHeader().setDefaultSectionSize(170)
        main.nfe_entrada_table.horizontalHeader().setMaximumSectionSize(420)
        main.nfe_entrada_table.horizontalHeader().setSectionsMovable(True)
        main.nfe_entrada_table.horizontalHeader().setContextMenuPolicy(
            Qt.CustomContextMenu
        )
        layout.addWidget(main.nfe_entrada_table, 1)
        return tab
