from __future__ import annotations

from PySide6.QtCore import QDate, Qt
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDateEdit,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QPushButton,
    QPlainTextEdit,
    QScrollArea,
    QTreeWidget,
    QVBoxLayout,
    QWidget,
)


class ImportacaoWindowMixin:
    def _build_left_panel(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)

        cnpj_box = QGroupBox("CPF/CNPJ")
        cnpj_layout = QVBoxLayout(cnpj_box)
        input_line = QHBoxLayout()
        self.cnpj_input = QLineEdit()
        self.cnpj_input.setPlaceholderText("Digite o CPF ou CNPJ com ou sem mascara")
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
        self.btn_apagar_dados.setToolTip(
            "Apaga todos os parquets e análises do CNPJ (mantém o registro)"
        )
        self.btn_apagar_cnpj = QPushButton("Apagar CNPJ")
        self.btn_apagar_cnpj.setStyleSheet(
            "QPushButton { color: #ef5350; font-weight: bold; }"
        )
        self.btn_apagar_cnpj.setToolTip(
            "Remove permanentemente a pasta inteira e os registros no banco"
        )
        actions_row3.addWidget(self.btn_apagar_dados)
        actions_row3.addWidget(self.btn_apagar_cnpj)
        cnpj_layout.addLayout(actions_row3)

        actions_row4 = QHBoxLayout()
        self.btn_limpar_tudo = QPushButton("Limpar tudo")
        self.btn_limpar_tudo.setStyleSheet(
            "QPushButton { color: #ef5350; font-weight: bold; }"
        )
        self.btn_limpar_tudo.setToolTip(
            "Remove permanentemente os dados de TODOS os CNPJs cadastrados"
        )
        actions_row4.addWidget(self.btn_limpar_tudo)
        cnpj_layout.addLayout(actions_row4)

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

    def _build_tab_configuracoes(self) -> QWidget:
        """Aba de configuração de conexões Oracle e opções gerais da aplicação."""
        from dotenv import dotenv_values
        from interface_grafica.fisconforme.path_resolver import get_env_path

        env_path = get_env_path()
        env_vars = dotenv_values(env_path) if env_path.exists() else {}

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        container = QWidget()
        scroll.setWidget(container)
        root = QVBoxLayout(container)
        root.setContentsMargins(16, 16, 16, 16)
        root.setSpacing(12)

        # ── helpers ──────────────────────────────────────────────────
        def _field(
            key: str, placeholder: str = "", password: bool = False
        ) -> QLineEdit:
            le = QLineEdit()
            le.setText(str(env_vars.get(key, "")))
            if placeholder:
                le.setPlaceholderText(placeholder)
            if password:
                le.setEchoMode(QLineEdit.Password)
            return le

        def _status_label() -> QLabel:
            lbl = QLabel("—")
            lbl.setWordWrap(True)
            lbl.setMinimumHeight(36)
            return lbl

        def _test_button(texto: str = "Testar Conexão") -> QPushButton:
            btn = QPushButton(texto)
            btn.setFixedWidth(160)
            return btn

        # ── Status de Conexão — painel de destaque no topo ────────────
        grp_status = QGroupBox("Status da Conexão Oracle")
        sl = QVBoxLayout(grp_status)
        sl.setSpacing(6)
        sl.setContentsMargins(12, 8, 12, 8)

        r1 = QHBoxLayout()
        lbl_c1_title = QLabel("Conexão 1 — Principal:")
        lbl_c1_title.setFixedWidth(190)
        self._cfg_conn_lbl_1 = QLabel("— não verificado")
        self._cfg_conn_lbl_1.setWordWrap(True)
        r1.addWidget(lbl_c1_title)
        r1.addWidget(self._cfg_conn_lbl_1)
        r1.addStretch()
        sl.addLayout(r1)

        r2 = QHBoxLayout()
        lbl_c2_title = QLabel("Conexão 2 — Secundária:")
        lbl_c2_title.setFixedWidth(190)
        self._cfg_conn_lbl_2 = QLabel("— não verificado")
        self._cfg_conn_lbl_2.setWordWrap(True)
        r2.addWidget(lbl_c2_title)
        r2.addWidget(self._cfg_conn_lbl_2)
        r2.addStretch()
        sl.addLayout(r2)

        btn_verify_all = QPushButton("↺  Verificar Conexões")
        btn_verify_all.setFixedWidth(180)
        btn_verify_all.clicked.connect(self._verificar_conexoes)
        sl.addWidget(btn_verify_all)
        root.addWidget(grp_status)

        # ── Conexão Oracle 1 (Principal) ─────────────────────────────
        grp1 = QGroupBox("Conexão Oracle 1 — Principal")
        form1 = QFormLayout(grp1)
        form1.setLabelAlignment(Qt.AlignRight)
        form1.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)
        self._cfg_host = _field("ORACLE_HOST", "ex: exa01-scan.sefin.ro.gov.br")
        self._cfg_port = _field("ORACLE_PORT", "1521")
        self._cfg_service = _field("ORACLE_SERVICE", "ex: sefindw")
        self._cfg_user = _field("DB_USER", "CPF ou usuário")
        self._cfg_password = _field("DB_PASSWORD", "Senha", password=True)
        form1.addRow("Host:", self._cfg_host)
        form1.addRow("Porta:", self._cfg_port)
        form1.addRow("Serviço:", self._cfg_service)
        form1.addRow("Usuário:", self._cfg_user)
        form1.addRow("Senha:", self._cfg_password)

        self._cfg_test_status_1 = _status_label()
        self._cfg_btn_test_1 = _test_button()
        self._cfg_btn_test_1.clicked.connect(
            lambda: self._testar_conexao(
                self._cfg_host,
                self._cfg_port,
                self._cfg_service,
                self._cfg_user,
                self._cfg_password,
                self._cfg_btn_test_1,
                self._cfg_test_status_1,
                "_oracle_test_worker_1",
            )
        )
        test_row1 = QHBoxLayout()
        test_row1.addWidget(self._cfg_btn_test_1)
        test_row1.addWidget(self._cfg_test_status_1)
        test_row1.addStretch()
        form1.addRow("Teste:", test_row1)
        root.addWidget(grp1)

        # ── Conexão Oracle 2 (Secundária) ────────────────────────────
        grp2 = QGroupBox("Conexão Oracle 2 — Secundária")
        form2 = QFormLayout(grp2)
        form2.setLabelAlignment(Qt.AlignRight)
        form2.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)
        self._cfg_host_1 = _field(
            "ORACLE_HOST_1", "ex: exacc-x10-sefinscan.sefin.ro.gov.br"
        )
        self._cfg_port_1 = _field("ORACLE_PORT_1", "1521")
        self._cfg_service_1 = _field("ORACLE_SERVICE_1", "ex: svc.bi.users")
        self._cfg_user_1 = _field("DB_USER_1", "CPF ou usuário")
        self._cfg_password_1 = _field("DB_PASSWORD_1", "Senha", password=True)
        form2.addRow("Host:", self._cfg_host_1)
        form2.addRow("Porta:", self._cfg_port_1)
        form2.addRow("Serviço:", self._cfg_service_1)
        form2.addRow("Usuário:", self._cfg_user_1)
        form2.addRow("Senha:", self._cfg_password_1)

        self._cfg_test_status_2 = _status_label()
        self._cfg_btn_test_2 = _test_button()
        self._cfg_btn_test_2.clicked.connect(
            lambda: self._testar_conexao(
                self._cfg_host_1,
                self._cfg_port_1,
                self._cfg_service_1,
                self._cfg_user_1,
                self._cfg_password_1,
                self._cfg_btn_test_2,
                self._cfg_test_status_2,
                "_oracle_test_worker_2",
            )
        )
        test_row2 = QHBoxLayout()
        test_row2.addWidget(self._cfg_btn_test_2)
        test_row2.addWidget(self._cfg_test_status_2)
        test_row2.addStretch()
        form2.addRow("Teste:", test_row2)
        root.addWidget(grp2)

        # ── Configurações do Aplicativo ───────────────────────────────
        grp3 = QGroupBox("Configurações do Aplicativo")
        form3 = QFormLayout(grp3)
        form3.setLabelAlignment(Qt.AlignRight)
        form3.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)

        self._cfg_log_level = QComboBox()
        self._cfg_log_level.addItems(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
        current_level = env_vars.get("LOG_LEVEL", "INFO").upper()
        idx = self._cfg_log_level.findText(current_level)
        if idx >= 0:
            self._cfg_log_level.setCurrentIndex(idx)

        self._cfg_cache_enabled = QCheckBox("Ativar cache")
        self._cfg_cache_enabled.setChecked(
            env_vars.get("CACHE_ENABLED", "true").lower() == "true"
        )

        self._cfg_cache_ttl = _field("CACHE_TTL", "3600 (segundos)")

        self._cfg_theme = QComboBox()
        self._cfg_theme.addItems(["dark", "light"])
        current_theme = env_vars.get("DASHBOARD_THEME", "dark").lower()
        theme_idx = self._cfg_theme.findText(current_theme)
        if theme_idx >= 0:
            self._cfg_theme.setCurrentIndex(theme_idx)

        form3.addRow("Nível de log:", self._cfg_log_level)
        form3.addRow("Cache:", self._cfg_cache_enabled)
        form3.addRow("TTL do cache (s):", self._cfg_cache_ttl)
        form3.addRow("Tema do dashboard:", self._cfg_theme)
        root.addWidget(grp3)

        # ── Botão salvar ──────────────────────────────────────────────
        btn_row = QHBoxLayout()
        self._cfg_status_label = QLabel("")
        btn_salvar = QPushButton("Salvar Configurações")
        btn_salvar.setStyleSheet(self._estilo_botao_destacar())
        btn_salvar.clicked.connect(self._salvar_configuracoes)
        btn_row.addStretch()
        btn_row.addWidget(self._cfg_status_label)
        btn_row.addWidget(btn_salvar)
        root.addLayout(btn_row)
        root.addStretch()

        # init worker slots
        self._oracle_test_worker_1: object | None = None
        self._oracle_test_worker_2: object | None = None
        self._oracle_verify_worker_1: object | None = None
        self._oracle_verify_worker_2: object | None = None

        return scroll

    def _build_tab_analise_lote_cnpj(self) -> QWidget:
        """Retorna o painel Fisconforme não Atendido como aba do QTabWidget."""
        try:
            from ..fisconforme import FisconformeNaoAtendidoPanel

            return FisconformeNaoAtendidoPanel()
        except Exception as exc:
            import logging

            logging.getLogger(__name__).warning(
                "Não foi possível carregar o painel Fisconforme: %s", exc
            )
            from PySide6.QtWidgets import QLabel

            lbl = QLabel(f"Painel Fisconforme indisponível: {exc}")
            lbl.setWordWrap(True)
            return lbl

    def _build_tab_logs(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        toolbar = QHBoxLayout()
        self.btn_refresh_logs = QPushButton("Atualizar logs")
        self.lbl_logs_status = QLabel("")
        toolbar.addWidget(self.btn_refresh_logs)
        toolbar.addWidget(self.lbl_logs_status)
        toolbar.addStretch()
        layout.addLayout(toolbar)
        self.log_view = QPlainTextEdit()
        self.log_view.setReadOnly(True)
        layout.addWidget(self.log_view)
        return tab
