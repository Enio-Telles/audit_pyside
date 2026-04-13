"""
Painel "Fisconforme não Atendido" para incorporação no QTabWidget do sistema.

Converte a lógica de FisconformeApp (QMainWindow standalone) para um QWidget
embutível, preservando toda a lógica de orquestração do wizard.
"""
from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QApplication,
    QFrame,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QScrollArea,
    QStackedWidget,
    QVBoxLayout,
    QWidget,
)

from .components import ActionFooter, PageHeader
from .pages import AuditorPage, CNPJsPage, DatabaseConfigPage, PeriodPage, ProcessingPage
from .state import WizardState
from .theme import SIDEBAR_WIDTH, build_stylesheet


class FisconformeNaoAtendidoPanel(QWidget):
    """
    Painel embeddable do pipeline Fisconforme não Atendido.

    Encapsula o wizard de 5 etapas (Banco → CNPJs → Auditor/DSF → Período → Processamento)
    como um QWidget que pode ser adicionado ao QTabWidget de main_window.py.
    """

    ETAPAS = [
        "Banco de Dados",
        "CNPJs",
        "Auditor / DSF",
        "Período",
        "Processamento",
    ]

    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self.state = WizardState()
        self._page_wrappers: list[QScrollArea] = []

        root_layout = QHBoxLayout(self)
        root_layout.setContentsMargins(0, 0, 0, 0)
        root_layout.setSpacing(0)

        self.sidebar = self._build_sidebar()
        root_layout.addWidget(self.sidebar)

        content = QWidget()
        content.setObjectName("ContentFrame")
        content_layout = QVBoxLayout(content)
        content_layout.setContentsMargins(0, 0, 0, 0)
        content_layout.setSpacing(0)

        header_frame = QFrame()
        header_frame.setObjectName("ContentHeader")
        header_layout = QVBoxLayout(header_frame)
        header_layout.setContentsMargins(20, 16, 20, 12)
        self.header = PageHeader()
        header_layout.addWidget(self.header)
        content_layout.addWidget(header_frame)

        self.stack = QStackedWidget()
        self.pages = [
            DatabaseConfigPage(),
            CNPJsPage(),
            AuditorPage(),
            PeriodPage(),
            ProcessingPage(),
        ]
        for page in self.pages:
            page.action_updated.connect(lambda page=page: self._handle_page_update(page))
            page.workflow_requested.connect(self._handle_workflow_request)
            wrapper = self._wrap_page(page)
            self._page_wrappers.append(wrapper)
            self.stack.addWidget(wrapper)
        content_layout.addWidget(self.stack, 1)

        self.footer = ActionFooter()
        self.footer.previous_clicked.connect(self._go_previous)
        self.footer.primary_clicked.connect(self._go_primary)
        content_layout.addWidget(self.footer)

        root_layout.addWidget(content, 1)

        # Aplica o stylesheet do Fisconforme somente a este widget
        self.setStyleSheet(build_stylesheet())

        self._activate_step(0, persist_current=False)

    # ------------------------------------------------------------------
    # Sidebar
    # ------------------------------------------------------------------

    def _build_sidebar(self) -> QWidget:
        sidebar = QWidget()
        sidebar.setObjectName("Sidebar")
        layout = QVBoxLayout(sidebar)
        layout.setContentsMargins(14, 16, 14, 14)
        layout.setSpacing(10)

        title = QLabel("SEFIN / RO")
        title.setObjectName("SidebarTitle")
        layout.addWidget(title)

        subtitle = QLabel("Pipeline Fisconforme\nNotificações não atendidas")
        subtitle.setObjectName("SidebarSubtitle")
        subtitle.setWordWrap(True)
        layout.addWidget(subtitle)

        self.step_buttons: list[QPushButton] = []
        for index, etapa in enumerate(self.ETAPAS):
            button = QPushButton(f"{index + 1}. {etapa}")
            button.setProperty("step", True)
            button.clicked.connect(lambda checked=False, idx=index: self._activate_step(idx))
            self.step_buttons.append(button)
            layout.addWidget(button)

        summary_card = QFrame()
        summary_card.setObjectName("SidebarSummaryCard")
        summary_layout = QVBoxLayout(summary_card)
        summary_layout.setContentsMargins(10, 10, 10, 10)
        summary_layout.setSpacing(8)

        summary_title = QLabel("Resumo operacional")
        summary_title.setStyleSheet("color: #FFFFFF; font-weight: 700;")
        summary_layout.addWidget(summary_title)

        self.summary_labels: dict[str, QLabel] = {}
        for label in ("CNPJs", "DSF", "Auditor", "Período"):
            key = QLabel(label)
            key.setObjectName("SidebarSummaryLabel")
            summary_layout.addWidget(key)

            value = QLabel("—")
            value.setObjectName("SidebarSummaryValue")
            value.setWordWrap(True)
            summary_layout.addWidget(value)
            self.summary_labels[label] = value

        preview_key = QLabel("Prévia")
        preview_key.setObjectName("SidebarSummaryLabel")
        summary_layout.addWidget(preview_key)

        self.preview_value = QLabel("Nenhum CNPJ carregado.")
        self.preview_value.setObjectName("SidebarSummaryValue")
        self.preview_value.setWordWrap(True)
        summary_layout.addWidget(self.preview_value)

        layout.addWidget(summary_card)
        layout.addStretch()

        version = QLabel("v2.0 • integrado ao Sistema PySiSDE")
        version.setObjectName("SidebarSubtitle")
        layout.addWidget(version)
        sidebar.setFixedWidth(SIDEBAR_WIDTH)
        return sidebar

    # ------------------------------------------------------------------
    # Page wrapping
    # ------------------------------------------------------------------

    def _wrap_page(self, page: QWidget) -> QScrollArea:
        container = QWidget()
        container_layout = QVBoxLayout(container)
        container_layout.setContentsMargins(20, 16, 20, 16)
        container_layout.addWidget(page)
        container_layout.addStretch()

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setWidget(container)
        return scroll

    # ------------------------------------------------------------------
    # Step navigation
    # ------------------------------------------------------------------

    def _persist_current_page(self):
        page = self.pages[self.state.etapa_atual]
        page.persist_state(self.state)

    def _activate_step(self, step_index: int, persist_current: bool = True):
        current_page = self.pages[self.state.etapa_atual]
        if persist_current:
            current_page.persist_state(self.state)
        if self.state.etapa_atual == 4 and not current_page.allow_back(self.state):
            return

        self.state.etapa_atual = step_index
        page = self.pages[step_index]
        page.load_state(self.state)
        self.stack.setCurrentIndex(step_index)
        self.header.set_content(page.page_title, page.page_subtitle)
        self._refresh_chrome()

    def _go_previous(self):
        page = self.pages[self.state.etapa_atual]
        self._persist_current_page()
        if self.state.etapa_atual > 0 and page.allow_back(self.state):
            self._activate_step(self.state.etapa_atual - 1, persist_current=False)

    def _go_primary(self):
        page = self.pages[self.state.etapa_atual]
        self._persist_current_page()
        if not page.validate(self.state):
            return
        action = page.handle_primary_action(self.state)
        self._refresh_chrome()
        if action == "advance" and self.state.etapa_atual < len(self.pages) - 1:
            self._activate_step(self.state.etapa_atual + 1, persist_current=False)

    # ------------------------------------------------------------------
    # Chrome refresh
    # ------------------------------------------------------------------

    def _refresh_chrome(self):
        self._refresh_sidebar()
        self._refresh_footer()

    def _handle_page_update(self, page: QWidget):
        if hasattr(page, "persist_state"):
            page.persist_state(self.state)
        self._refresh_chrome()

    def _refresh_sidebar(self):
        for index, button in enumerate(self.step_buttons):
            state_name = (
                "active"
                if index == self.state.etapa_atual
                else "done"
                if index < self.state.etapa_atual
                else "idle"
            )
            button.setProperty("stepState", state_name)
            button.style().unpolish(button)
            button.style().polish(button)

        resumo = self.state.resumo_sidebar()
        for key, value in resumo.items():
            self.summary_labels[key].setText(value)

        validos = self.state.cnpjs_validos()
        if not validos:
            self.preview_value.setText("Nenhum CNPJ carregado.")
        else:
            preview = "\n".join(validos[:3])
            if len(validos) > 3:
                preview += f"\n... +{len(validos) - 3} restante(s)"
            self.preview_value.setText(preview)

    def _refresh_footer(self):
        page = self.pages[self.state.etapa_atual]
        self.footer.configure(
            primary_label=page.primary_action_label(self.state),
            context_text=page.footer_context(self.state),
            primary_object_name=page.primary_button_name(self.state),
            show_previous=self.state.etapa_atual > 0 and page.allow_back(self.state),
            primary_enabled=page.primary_action_enabled(self.state),
        )

    def _handle_workflow_request(self, action: str):
        if action == "new_dsf":
            self._reset_for_new_dsf()

    def _reset_for_new_dsf(self):
        db_config = dict(self.state.db_config)
        diretorio_saida = self.state.diretorio_saida
        self.state = WizardState(db_config=db_config, diretorio_saida=diretorio_saida)
        for page in self.pages:
            page.load_state(self.state)
        self._activate_step(1, persist_current=False)
