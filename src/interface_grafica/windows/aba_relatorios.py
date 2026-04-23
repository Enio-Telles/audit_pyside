from pathlib import Path

from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QLabel,
    QPushButton,
    QHBoxLayout,
    QTableView,
    QComboBox,
    QLineEdit,
    QApplication,
    QAbstractItemView,
    QTabWidget,
)
from PySide6.QtCore import Qt


class AbaRelatorios:
    """Aba de relatórios extraída de main_window.py (P3).

    Esta classe cria a estrutura de abas relacionadas a "Estoque" / relatórios
    e atribui os widgets criados como atributos em `main` (MainWindow) para
    preservar compatibilidade com o restante do código.
    """

    def __init__(self, main_window):
        self.main = main_window
        self.root = QWidget()
        self._setup_ui()

    def _setup_ui(self) -> None:
        main = self.main
        layout = QVBoxLayout(self.root)

        estoque_tabs = QTabWidget()

        # Reuse existing mov_estoque builder from main (keeps original wiring)
        tab_mov_estoque = main._build_tab_mov_estoque()
        main.tab_mov_estoque = tab_mov_estoque
        estoque_tabs.addTab(main.tab_mov_estoque, "Tabela mov_estoque")

        # Mensal (moved builder)
        tab_aba_mensal = self._build_tab_aba_mensal()
        main.tab_aba_mensal = tab_aba_mensal
        estoque_tabs.addTab(main.tab_aba_mensal, "Tabela mensal")

        # Anual / Periodos / Resumo Global reuse existing builders where possible
        tab_aba_anual = main._build_tab_aba_anual()
        main.tab_aba_anual = tab_aba_anual
        estoque_tabs.addTab(main.tab_aba_anual, "Tabela anual")

        tab_aba_periodos = main._build_tab_aba_periodos()
        main.tab_aba_periodos = tab_aba_periodos
        estoque_tabs.addTab(main.tab_aba_periodos, "Tabela períodos")

        tab_resumo_global = self._build_tab_resumo_global()
        main.tab_resumo_global = tab_resumo_global
        estoque_tabs.addTab(main.tab_resumo_global, "Resumo Global")

        tab_produtos_selecionados = main._build_tab_produtos_selecionados()
        main.tab_produtos_selecionados = tab_produtos_selecionados
        estoque_tabs.addTab(main.tab_produtos_selecionados, "Produtos selecionados")

        tab_id_agrupados = main._build_tab_id_agrupados()
        main.tab_id_agrupados = tab_id_agrupados
        estoque_tabs.addTab(main.tab_id_agrupados, "id_agrupados")

        layout.addWidget(estoque_tabs)

        # Expose estoque_tabs on main for compatibility with signal wiring
        main.estoque_tabs = estoque_tabs

    def _build_tab_resumo_global(self) -> QWidget:
        main = self.main
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

        main.lbl_resumo_global_titulo = QLabel("Tabela: Resumo Global (Mensal e Anual)")
        main.lbl_resumo_global_titulo.setStyleSheet(
            "QLabel { font-weight: bold; color: #f8fafc; background: #1f2a44; border: 1px solid #334155; border-radius: 4px; padding: 6px 10px; }"
        )
        layout.addWidget(main.lbl_resumo_global_titulo)

        toolbar = QHBoxLayout()
        main.btn_refresh_resumo_global = QPushButton("Atualizar Resumo Global")
        main.btn_export_resumo_global = QPushButton("Exportar Excel")
        toolbar.addWidget(main.btn_refresh_resumo_global)
        toolbar.addStretch()
        toolbar.addWidget(main.btn_export_resumo_global)
        layout.addLayout(toolbar)

        main.lbl_resumo_global_status = QLabel(
            "Aguardando carregamento da aba mensal e anual..."
        )
        main.lbl_resumo_global_status.setStyleSheet(
            "QLabel { padding: 4px 8px; background: #101827; border: 1px solid #374151; border-radius: 4px; color: #e5e7eb; }"
        )
        layout.addWidget(main.lbl_resumo_global_status)

        main.resumo_global_table = QTableView()
        main.resumo_global_table.setModel(main.resumo_global_model)
        main.resumo_global_table.setSelectionBehavior(QAbstractItemView.SelectItems)
        main.resumo_global_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        main.resumo_global_table.setAlternatingRowColors(True)
        main.resumo_global_table.setSortingEnabled(True)
        main.resumo_global_table.setWordWrap(True)
        main.resumo_global_table.verticalHeader().setDefaultSectionSize(40)
        main.resumo_global_table.horizontalHeader().setMinimumSectionSize(80)
        main.resumo_global_table.horizontalHeader().setDefaultSectionSize(180)
        main.resumo_global_table.horizontalHeader().setStretchLastSection(True)
        main.resumo_global_table.setStyleSheet(
            "QTableView::item { padding: 4px 2px; }"
            "QTableCornerButton::section { background: #18181b; border: 1px solid #3f3f46; }"
        )
        layout.addWidget(main.resumo_global_table, 1)

        return tab

    def _build_tab_aba_mensal(self) -> QWidget:
        main = self.main
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

        main.lbl_aba_mensal_titulo = QLabel("Tabela: aba_mensal")
        main.lbl_aba_mensal_titulo.setStyleSheet(
            "QLabel { font-weight: bold; color: #f8fafc; background: #1f2a44; border: 1px solid #334155; border-radius: 4px; padding: 6px 10px; }"
        )
        layout.addWidget(main.lbl_aba_mensal_titulo)

        toolbar = QHBoxLayout()
        main.btn_refresh_aba_mensal = QPushButton("Recarregar")
        main.btn_refresh_aba_mensal.setIcon(
            QApplication.style().standardIcon(
                QApplication.style().StandardPixmap.SP_BrowserReload
            )
        )
        main.btn_apply_aba_mensal_filters = QPushButton("Aplicar filtros")
        main.btn_clear_aba_mensal_filters = QPushButton("Limpar filtros")
        main.btn_export_aba_mensal = QPushButton("Exportar Excel")
        main.btn_destacar_aba_mensal = main._criar_botao_destacar()
        toolbar.addWidget(main.btn_refresh_aba_mensal)
        toolbar.addWidget(main.btn_apply_aba_mensal_filters)
        toolbar.addWidget(main.btn_clear_aba_mensal_filters)
        toolbar.addStretch()
        toolbar.addWidget(main.btn_destacar_aba_mensal)
        toolbar.addWidget(main.btn_export_aba_mensal)
        layout.addLayout(toolbar)

        filtros = QHBoxLayout()
        main.mensal_filter_id = QComboBox()
        main.mensal_filter_id.setEditable(True)
        main.mensal_filter_id.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
        main.mensal_filter_id.setMinimumWidth(220)
        main.mensal_filter_id.lineEdit().setPlaceholderText("Filtrar id_agregado")
        main.mensal_filter_desc = QLineEdit()
        main.mensal_filter_desc.setPlaceholderText("Filtrar descricao")
        main.mensal_filter_ano = QComboBox()
        main.mensal_filter_ano.addItem("Todos")
        main.mensal_filter_ano.setMinimumWidth(100)
        main.mensal_filter_ano.setToolTip("Filtrar por ano")
        main.mensal_filter_mes = QComboBox()
        main.mensal_filter_mes.addItems(["Todos"] + [str(i) for i in range(1, 13)])
        main.mensal_filter_mes.setMinimumWidth(100)
        main.mensal_filter_mes.setToolTip("Filtrar por mes")
        main.mensal_filter_texto = QLineEdit()
        main.mensal_filter_texto.setPlaceholderText("Busca ampla...")
        for widget in [
            main.mensal_filter_id,
            main.mensal_filter_desc,
            QLabel("Ano"),
            main.mensal_filter_ano,
            QLabel("Mes"),
            main.mensal_filter_mes,
            main.mensal_filter_texto,
        ]:
            filtros.addWidget(widget)
        layout.addLayout(filtros)

        filtros_avancados = QHBoxLayout()
        main.mensal_filter_num_col = QComboBox()
        main.mensal_filter_num_col.addItems(
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
        main.mensal_filter_num_min = QLineEdit()
        main.mensal_filter_num_min.setPlaceholderText("Min numerico")
        main.mensal_filter_num_max = QLineEdit()
        main.mensal_filter_num_max.setPlaceholderText("Max numerico")
        main.mensal_profile = QComboBox()
        main.mensal_profile.addItems(["Padrao", "Auditoria", "Estoque", "Custos"])
        main.btn_mensal_profile = QPushButton("Perfil")
        main.btn_mensal_save_profile = QPushButton("Salvar perfil")
        main.btn_mensal_colunas = QPushButton("Colunas")
        for widget in [
            QLabel("Numero"),
            main.mensal_filter_num_col,
            main.mensal_filter_num_min,
            main.mensal_filter_num_max,
            main.mensal_profile,
            main.btn_mensal_profile,
            main.btn_mensal_save_profile,
            main.btn_mensal_colunas,
        ]:
            filtros_avancados.addWidget(widget)
        filtros_avancados.addStretch()
        layout.addLayout(filtros_avancados)

        main.lbl_aba_mensal_status = QLabel(
            "Selecione um CNPJ para carregar a aba mensal."
        )
        main.lbl_aba_mensal_status.setStyleSheet(
            "QLabel { padding: 4px 8px; background: #101827; border: 1px solid #374151; border-radius: 4px; color: #e5e7eb; }"
        )
        layout.addWidget(main.lbl_aba_mensal_status)

        main.lbl_aba_mensal_filtros = QLabel("Filtros ativos: nenhum")
        main.lbl_aba_mensal_filtros.setStyleSheet(
            "QLabel { padding: 4px 8px; color: #dbeafe; background: #0f1b33; border: 1px solid #334155; border-radius: 4px; }"
        )
        layout.addWidget(main.lbl_aba_mensal_filtros)

        main.aba_mensal_table = QTableView()
        main.aba_mensal_table.setModel(main.aba_mensal_model)
        main.aba_mensal_table.setSelectionBehavior(QAbstractItemView.SelectItems)
        main.aba_mensal_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        main.aba_mensal_table.setAlternatingRowColors(True)
        main.aba_mensal_table.setSortingEnabled(True)
        main.aba_mensal_table.setWordWrap(True)
        main.aba_mensal_table.verticalHeader().setDefaultSectionSize(40)
        main.aba_mensal_table.horizontalHeader().setMinimumSectionSize(40)
        main.aba_mensal_table.horizontalHeader().setDefaultSectionSize(170)
        main.aba_mensal_table.horizontalHeader().setMaximumSectionSize(380)
        main.aba_mensal_table.horizontalHeader().setStretchLastSection(True)
        main.aba_mensal_table.horizontalHeader().setSectionsMovable(True)
        main.aba_mensal_table.horizontalHeader().setContextMenuPolicy(
            Qt.CustomContextMenu
        )
        main.aba_mensal_table.setStyleSheet(
            "QTableView::item { padding: 4px 2px; }"
            "QTableCornerButton::section { background: #18181b; border: 1px solid #3f3f46; }"
        )
        layout.addWidget(main.aba_mensal_table, 1)
        return tab
