from __future__ import annotations

import re
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from PySide6.QtCore import Qt, QThreadPool, Signal
from PySide6.QtWidgets import (
    QAbstractItemView,
    QFileDialog,
    QComboBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QProgressBar,
    QSplitter,
    QTextEdit,
    QVBoxLayout,
    QWidget,
    QHeaderView,
)

from .extracao import conectar_oracle, limpar_cnpj, validar_cnpj
from .extracao_cadastral import exportar_cache_completo, obter_estatisticas_cache
from .config_service import (
    carregar_config_db,
    carregar_dados_salvos,
    salvar_config_db,
    salvar_dados_manuais,
)

# Resolvedor de caminhos do pacote integrado
from .path_resolver import get_root_dir
ROOT_DIR = get_root_dir()

from .components import (
    AdaptiveFieldGrid,
    DataTable,
    ElidedLabel,
    MetadataGrid,
    SectionCard,
    StatusBanner,
)
from .models import CNPJTableModel, ResultsTableModel
from .state import CNPJRecord, ProcessingResult, WizardState
from .theme import COLORS, SPACING
from .workers import BuscaRazaoSocialTask, WorkerThread


class BaseWizardPage(QWidget):
    """Contrato padrão das páginas do wizard."""

    action_updated = Signal()
    workflow_requested = Signal(str)

    page_title = ""
    page_subtitle = ""

    def load_state(self, state: WizardState):
        """Atualiza a página a partir do estado compartilhado."""

    def persist_state(self, state: WizardState):
        """Persiste o estado visual no objeto compartilhado."""

    def validate(self, state: WizardState) -> bool:
        return True

    def primary_action_label(self, state: WizardState) -> str:
        return "Próximo"

    def primary_button_name(self, state: WizardState) -> str:
        return "PrimaryButton"

    def primary_action_enabled(self, state: WizardState) -> bool:
        return True

    def allow_back(self, state: WizardState) -> bool:
        return True

    def handle_primary_action(self, state: WizardState) -> str:
        return "advance"

    def footer_context(self, state: WizardState) -> str:
        return ""


class DatabaseConfigPage(BaseWizardPage):
    page_title = "Configuração do Banco de Dados"
    page_subtitle = (
        "Defina as credenciais do Oracle Data Warehouse. O layout foi reduzido para "
        "uma estrutura mais estável em telas menores, sem botões de avanço duplicados."
    )

    def __init__(self):
        super().__init__()
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(SPACING["lg"])

        self.status_banner = StatusBanner()
        layout.addWidget(self.status_banner)

        config_card = SectionCard("Conexão Oracle DW")
        config_layout = QVBoxLayout(config_card)
        config_layout.setSpacing(SPACING["md"])

        self.inputs: Dict[str, QLineEdit] = {}
        self.field_grid = AdaptiveFieldGrid()
        campos = [
            ("ORACLE_HOST", "Host (Servidor)", "exa01-scan.sefin.ro.gov.br"),
            ("ORACLE_PORT", "Porta", "1521"),
            ("ORACLE_SERVICE", "Serviço", "sefindw"),
            ("DB_USER", "Usuário", "SEU_LOGIN"),
            ("DB_PASSWORD", "Senha", "Senha do banco"),
        ]
        for key, label, placeholder in campos:
            inp = QLineEdit()
            inp.setPlaceholderText(placeholder)
            inp.setMinimumHeight(36)
            if key == "DB_PASSWORD":
                inp.setEchoMode(QLineEdit.Password)
            self.inputs[key] = inp
            self.field_grid.add_field(label, inp)
        config_layout.addWidget(self.field_grid)

        action_row = QHBoxLayout()
        action_row.setSpacing(SPACING["sm"])
        self.btn_limpar = QPushButton("Limpar credenciais")
        self.btn_limpar.clicked.connect(self._clear_credentials)
        action_row.addWidget(self.btn_limpar)

        action_row.addStretch()

        self.btn_testar = QPushButton("Testar conexão")
        self.btn_testar.setObjectName("SecondaryButton")
        self.btn_testar.clicked.connect(self._test_connection)
        action_row.addWidget(self.btn_testar)

        self.btn_salvar = QPushButton("Salvar configurações")
        self.btn_salvar.setObjectName("PrimaryButton")
        self.btn_salvar.clicked.connect(self._save_config)
        action_row.addWidget(self.btn_salvar)
        config_layout.addLayout(action_row)

        layout.addWidget(config_card)
        layout.addStretch()

        self._load_config()

    def _load_config(self):
        config = carregar_config_db()
        for key, value in config.items():
            if key in self.inputs:
                self.inputs[key].setText(value)

    def _clear_credentials(self):
        self.inputs["DB_USER"].clear()
        self.inputs["DB_PASSWORD"].clear()
        self.status_banner.set_status("info", "Credenciais limpas do formulário atual.")
        self.action_updated.emit()

    def _save_config(self) -> bool:
        dados = {key: widget.text().strip() for key, widget in self.inputs.items()}
        if salvar_config_db(dados):
            self.status_banner.set_status("success", "Configurações salvas com sucesso no arquivo .env.")
            return True
        self.status_banner.set_status("danger", "Não foi possível salvar as configurações no arquivo .env.")
        return False

    def _test_connection(self):
        self.status_banner.set_status("info", "Testando conexão com o Oracle DW...")
        self._save_config()
        try:
            conexao = conectar_oracle()
            if conexao:
                conexao.close()
                self.status_banner.set_status("success", "Conexão estabelecida com sucesso.")
            else:
                self.status_banner.set_status("danger", "Falha na conexão. Revise host, usuário e senha.")
        except Exception as exc:  # pragma: no cover - depende de ambiente
            self.status_banner.set_status("danger", f"Erro ao testar a conexão: {exc}")

    def load_state(self, state: WizardState):
        if state.db_config:
            for key, value in state.db_config.items():
                if key in self.inputs:
                    self.inputs[key].setText(value)
        else:
            self._load_config()

    def persist_state(self, state: WizardState):
        state.db_config = {key: widget.text().strip() for key, widget in self.inputs.items()}

    def validate(self, state: WizardState) -> bool:
        db_user = state.db_config.get("DB_USER", "")
        db_password = state.db_config.get("DB_PASSWORD", "")
        if not db_user or not db_password:
            resposta = QMessageBox.question(
                self,
                "Prosseguir sem credenciais?",
                "Usuário ou senha do banco não foram informados.\n"
                "O processamento pode falhar caso o Oracle seja necessário.\n\n"
                "Deseja continuar mesmo assim?",
                QMessageBox.Yes | QMessageBox.No,
            )
            if resposta != QMessageBox.Yes:
                return False
        self._save_config()
        return True

    def primary_action_label(self, state: WizardState) -> str:
        return "Confirmar banco e seguir"

    def footer_context(self, state: WizardState) -> str:
        return "Etapa 1 de 5  •  Configure a base de conexão para as próximas consultas."


class CNPJsPage(BaseWizardPage):
    page_title = "Seleção de CNPJs"
    page_subtitle = (
        "Os contribuintes agora são exibidos em grade estruturada, com colunas separadas "
        "para facilitar leitura, ordenação e evitar colisão de textos longos."
    )

    def __init__(self):
        super().__init__()
        self._thread_pool = QThreadPool.globalInstance()
        self._thread_pool.setMaxThreadCount(4)
        self._pending_lookups = 0
        self._lookup_workers: Dict[str, BuscaRazaoSocialTask] = {}

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(SPACING["lg"])

        self.status_banner = StatusBanner()
        layout.addWidget(self.status_banner)

        arquivo_card = SectionCard("Carregar de arquivo")
        arquivo_layout = QHBoxLayout(arquivo_card)
        arquivo_layout.setSpacing(SPACING["md"])
        self.lbl_arquivo = ElidedLabel("Nenhum arquivo selecionado")
        self.lbl_arquivo.setStyleSheet(f"color: {COLORS['muted']}; font-style: italic;")
        arquivo_layout.addWidget(self.lbl_arquivo, 1)

        btn_carregar = QPushButton("Selecionar arquivo TXT")
        btn_carregar.setObjectName("PrimaryButton")
        btn_carregar.clicked.connect(self._load_file)
        arquivo_layout.addWidget(btn_carregar)
        layout.addWidget(arquivo_card)

        manual_card = SectionCard("Adicionar CNPJs")
        manual_layout = QVBoxLayout(manual_card)
        self.input_cnpj = QTextEdit()
        self.input_cnpj.setPlaceholderText(
            "Cole ou digite CNPJs separados por linha, vírgula, ponto-e-vírgula ou espaço."
        )
        self.input_cnpj.setMinimumHeight(96)
        manual_layout.addWidget(self.input_cnpj)

        manual_actions = QHBoxLayout()
        manual_actions.addStretch()
        btn_adicionar = QPushButton("Adicionar todos")
        btn_adicionar.setObjectName("PrimaryButton")
        btn_adicionar.clicked.connect(self._add_batch)
        manual_actions.addWidget(btn_adicionar)
        manual_layout.addLayout(manual_actions)
        layout.addWidget(manual_card)

        tabela_card = SectionCard("Contribuintes para processamento")
        tabela_layout = QVBoxLayout(tabela_card)
        tabela_layout.setSpacing(SPACING["md"])

        info_label = QLabel(
            "A grade mantém tooltip completo por linha e evita misturar CNPJ com razão social na mesma célula."
        )
        info_label.setWordWrap(True)
        info_label.setStyleSheet(f"color: {COLORS['muted']};")
        tabela_layout.addWidget(info_label)

        self.lookup_progress = QProgressBar()
        self.lookup_progress.setVisible(False)
        self.lookup_progress.setTextVisible(False)
        tabela_layout.addWidget(self.lookup_progress)

        self.model = CNPJTableModel()
        self.view = DataTable()
        self.view.setModel(self.model)
        self.view.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.view.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.view.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.view.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        self.view.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.view.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
        self.view.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeToContents)
        self.view.horizontalHeader().setSectionResizeMode(6, QHeaderView.ResizeToContents)
        tabela_layout.addWidget(self.view, 1)

        bottom_row = QHBoxLayout()
        bottom_row.setSpacing(SPACING["sm"])
        self.lbl_count = QLabel("0 CNPJ(s) na grade")
        self.lbl_count.setStyleSheet(f"color: {COLORS['muted']}; font-weight: 600;")
        bottom_row.addWidget(self.lbl_count)

        self.lbl_loading = QLabel("")
        self.lbl_loading.setStyleSheet(f"color: {COLORS['secondary']};")
        bottom_row.addWidget(self.lbl_loading)
        bottom_row.addStretch()

        btn_remover = QPushButton("Remover selecionados")
        btn_remover.clicked.connect(self._remove_selected)
        bottom_row.addWidget(btn_remover)

        btn_limpar = QPushButton("Limpar tudo")
        btn_limpar.setObjectName("DangerButton")
        btn_limpar.clicked.connect(self._clear_all)
        bottom_row.addWidget(btn_limpar)

        btn_recuperar = QPushButton("Atualizar dados")
        btn_recuperar.setObjectName("SecondaryButton")
        btn_recuperar.clicked.connect(self._refresh_pending)
        bottom_row.addWidget(btn_recuperar)

        btn_exportar = QPushButton("Exportar cache Parquet")
        btn_exportar.clicked.connect(self._export_cache)
        bottom_row.addWidget(btn_exportar)

        tabela_layout.addLayout(bottom_row)
        layout.addWidget(tabela_card, 1)

    def load_state(self, state: WizardState):
        self.model.set_records(state.cnpj_records)
        self._update_count()

    def persist_state(self, state: WizardState):
        state.cnpj_records = self.model.records()

    def validate(self, state: WizardState) -> bool:
        validos = state.cnpjs_validos()
        if not validos:
            QMessageBox.warning(self, "Atenção", "Adicione ao menos um CNPJ válido para continuar.")
            return False
        resposta = QMessageBox.question(
            self,
            "Confirmar CNPJs",
            f"Foram selecionados {len(validos)} CNPJ(s) válidos.\n\nDeseja seguir para os dados do auditor?",
            QMessageBox.Yes | QMessageBox.No,
        )
        return resposta == QMessageBox.Yes

    def primary_action_label(self, state: WizardState) -> str:
        return "Confirmar CNPJs e seguir"

    def footer_context(self, state: WizardState) -> str:
        return "Etapa 2 de 5  •  Organize a base de contribuintes com ordenação e tooltips completos."

    def _load_file(self):
        caminho, _ = QFileDialog.getOpenFileName(
            self,
            "Selecionar arquivo de CNPJs",
            str(ROOT_DIR),
            "Arquivos de texto (*.txt);;Todos os arquivos (*)",
        )
        if not caminho:
            return
        arquivo = Path(caminho)
        self.lbl_arquivo.setText(str(arquivo))

        try:
            with open(arquivo, "r", encoding="utf-8") as handle:
                for linha in handle:
                    linha = linha.strip()
                    if linha and not linha.startswith("#"):
                        self._add_cnpj(linha)
            self.status_banner.set_status("success", f"Arquivo carregado: {arquivo.name}")
        except Exception as exc:
            self.status_banner.set_status("danger", f"Erro ao ler arquivo: {exc}")

    def _add_batch(self):
        texto = self.input_cnpj.toPlainText().strip()
        if not texto:
            self.status_banner.set_status("warning", "Cole ou digite ao menos um CNPJ antes de adicionar.")
            return

        partes = re.split(r"[\n,;\t ]+", texto)
        adicionados = 0
        for parte in partes:
            parte = parte.strip()
            if not parte:
                continue
            if self._add_cnpj(parte):
                adicionados += 1
        self.input_cnpj.clear()
        if adicionados:
            self.status_banner.set_status("success", f"{adicionados} CNPJ(s) adicionados à grade.")
        else:
            self.status_banner.set_status("warning", "Nenhum novo CNPJ foi adicionado. Verifique duplicidades e formato.")

    def _add_cnpj(self, raw_value: str) -> bool:
        cnpj = limpar_cnpj(raw_value)
        if not cnpj or self.model.has_cnpj(cnpj):
            return False

        valido = validar_cnpj(cnpj)
        record = CNPJRecord(
            seq=self.model.rowCount() + 1,
            cnpj=cnpj,
            valido=valido,
            status="Buscando dados" if valido else "Inválido",
            carregando=valido,
            tooltip=f"CNPJ: {cnpj}\nAguardando busca cadastral..." if valido else f"CNPJ inválido: {cnpj}",
        )
        self.model.add_record(record)
        self._update_count()
        if valido:
            self._lookup_cadastral(cnpj)
        self.action_updated.emit()
        return True

    def _lookup_cadastral(self, cnpj: str):
        self._pending_lookups += 1
        self._update_lookup_feedback()
        worker = BuscaRazaoSocialTask(cnpj)
        worker.signals.finished.connect(self._on_lookup_finished)
        self._lookup_workers[cnpj] = worker
        self._thread_pool.start(worker)

    def _on_lookup_finished(self, cnpj: str, dados: Optional[dict]):
        self._lookup_workers.pop(cnpj, None)
        self._pending_lookups = max(0, self._pending_lookups - 1)
        self._update_lookup_feedback()

        if dados:
            razao_social = dados.get("RAZAO_SOCIAL", "Não informada")
            origem = "Cache Parquet" if dados.get("_FROM_PARQUET") else "Oracle DW"
            tooltip = [
                f"Razão Social: {razao_social}",
                f"CNPJ: {cnpj}",
                f"Fantasia: {dados.get('NOME_FANTASIA', '—')}",
                f"Município/UF: {dados.get('MUNICIPIO', '—')}/{dados.get('UF', '')}".rstrip("/"),
                f"Situação: {dados.get('SITUACAO_DA_IE', '—')}",
                f"Regime: {dados.get('REGIME_DE_PAGAMENTO', '—')}",
                f"Origem: {origem}",
            ]
            self.model.update_record(
                cnpj,
                razao_social=razao_social,
                municipio=dados.get("MUNICIPIO", ""),
                uf=dados.get("UF", ""),
                situacao=dados.get("SITUACAO_DA_IE", ""),
                origem=origem,
                nome_fantasia=dados.get("NOME_FANTASIA", ""),
                regime=dados.get("REGIME_DE_PAGAMENTO", ""),
                status="Pronto",
                carregando=False,
                erro="",
                tooltip="\n".join(tooltip),
            )
        else:
            self.model.update_record(
                cnpj,
                razao_social="Não encontrada",
                origem="Oracle DW",
                status="Sem cadastro",
                carregando=False,
                erro="Razão social não encontrada",
                tooltip=f"Não foi possível encontrar dados cadastrais para o CNPJ {cnpj}.",
            )
        self.action_updated.emit()

    def _update_lookup_feedback(self):
        if self._pending_lookups > 0:
            self.lookup_progress.setVisible(True)
            self.lookup_progress.setRange(0, 0)
            self.lbl_loading.setText(f"Consultando {self._pending_lookups} cadastro(s)...")
        else:
            self.lookup_progress.setVisible(False)
            self.lbl_loading.setText("")

    def _remove_selected(self):
        rows = [index.row() for index in self.view.selectionModel().selectedRows()]
        if not rows:
            return
        self.model.remove_rows(rows)
        self._update_count()
        self.action_updated.emit()

    def _clear_all(self):
        if self.model.rowCount() == 0:
            return
        resposta = QMessageBox.question(
            self,
            "Limpar lista",
            "Deseja remover todos os CNPJs atualmente exibidos?",
            QMessageBox.Yes | QMessageBox.No,
        )
        if resposta == QMessageBox.Yes:
            self.model.clear()
            self._update_count()
            self.status_banner.clear()
            self.action_updated.emit()

    def _refresh_pending(self):
        registros = self.model.records()
        pendentes = [
            registro.cnpj
            for registro in registros
            if registro.valido and (registro.carregando or registro.erro or not registro.razao_social)
        ]
        if not pendentes:
            self.status_banner.set_status("info", "Todos os CNPJs já possuem dados cadastrais carregados.")
            return
        for cnpj in pendentes:
            self.model.update_record(cnpj, status="Buscando dados", carregando=True)
            self._lookup_cadastral(cnpj)
        self.status_banner.set_status("info", f"Atualizando {len(pendentes)} cadastro(s) pendente(s).")

    def _export_cache(self):
        stats = obter_estatisticas_cache()
        if stats.get("total", 0) == 0:
            QMessageBox.information(self, "Cache vazio", "Nenhum dado cadastral em cache foi encontrado.")
            return

        nome_sugerido = f"dados_cadastrais_cache_{datetime.now():%Y%m%d_%H%M%S}.parquet"
        caminho_saida, _ = QFileDialog.getSaveFileName(
            self,
            "Exportar cache de dados cadastrais",
            str(ROOT_DIR / nome_sugerido),
            "Arquivos Parquet (*.parquet)",
        )
        if not caminho_saida:
            return

        exportado = exportar_cache_completo(Path(caminho_saida))
        if exportado:
            self.status_banner.set_status(
                "success",
                f"Cache exportado com sucesso para {Path(caminho_saida).name} ({stats['total']} linha(s)).",
            )
        else:
            self.status_banner.set_status("danger", "Não foi possível exportar o cache cadastral.")

    def _update_count(self):
        total = self.model.rowCount()
        validos = len([registro for registro in self.model.records() if registro.valido])
        self.lbl_count.setText(f"{total} registro(s) na grade  •  {validos} válido(s)")


class AuditorPage(BaseWizardPage):
    page_title = "Dados do Auditor e DSF"
    page_subtitle = (
        "Os campos foram reorganizados em blocos adaptativos para manter legibilidade "
        "em larguras menores, sem caixas comprimidas nem rótulos sobrepostos."
    )

    def __init__(self):
        super().__init__()
        self._configs: Dict[str, Dict[str, str]] = {}
        self._pdf_path: Optional[Path] = None
        self._output_dir: Optional[Path] = None

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(SPACING["lg"])

        self.status_banner = StatusBanner()
        layout.addWidget(self.status_banner)

        config_card = SectionCard("Configurações salvas")
        config_layout = QHBoxLayout(config_card)
        config_layout.addWidget(QLabel("Perfil salvo"))
        self.combo_config = QComboBox()
        self.combo_config.currentIndexChanged.connect(self._config_selected)
        config_layout.addWidget(self.combo_config, 1)
        layout.addWidget(config_card)

        auditor_card = SectionCard("Dados do auditor")
        auditor_layout = QVBoxLayout(auditor_card)
        self.auditor_grid = AdaptiveFieldGrid()
        self.inputs: Dict[str, QLineEdit] = {}
        for key, label, placeholder in [
            ("auditor", "Nome do auditor", "Ex: João da Silva"),
            ("matricula", "Matrícula", "Ex: 300201625"),
            ("contato", "Contato", "Ex: auditor@sefin.ro.gov.br"),
            ("orgao", "Órgão", "Ex: GEFIS"),
        ]:
            inp = QLineEdit()
            inp.setPlaceholderText(placeholder)
            inp.setMinimumHeight(36)
            self.inputs[key] = inp
            self.auditor_grid.add_field(label, inp)
        auditor_layout.addWidget(self.auditor_grid)

        titulo_row = QHBoxLayout()
        titulo_row.addWidget(QLabel("Título"))
        self.combo_titulo = QComboBox()
        self.combo_titulo.addItems(["Auditor", "Auditora"])
        titulo_row.addWidget(self.combo_titulo)
        titulo_row.addStretch()
        auditor_layout.addLayout(titulo_row)
        layout.addWidget(auditor_card)

        dsf_card = SectionCard("Documento de Solicitação Fiscal (DSF)")
        dsf_layout = QVBoxLayout(dsf_card)
        self.dsf_grid = AdaptiveFieldGrid()
        self.input_dsf = QLineEdit()
        self.input_dsf.setPlaceholderText("Ex: 20263710400285")
        self.input_dsf.setMinimumHeight(36)
        self.dsf_grid.add_field("Número da DSF", self.input_dsf)
        dsf_layout.addWidget(self.dsf_grid)

        pdf_row = QHBoxLayout()
        pdf_row.setSpacing(SPACING["md"])
        pdf_row.addWidget(QLabel("Arquivo PDF"))
        self.lbl_pdf = ElidedLabel("Nenhum arquivo selecionado")
        self.lbl_pdf.setStyleSheet(f"color: {COLORS['muted']}; font-style: italic;")
        pdf_row.addWidget(self.lbl_pdf, 1)
        btn_pdf = QPushButton("Selecionar PDF")
        btn_pdf.setObjectName("PrimaryButton")
        btn_pdf.clicked.connect(self._select_pdf)
        pdf_row.addWidget(btn_pdf)
        pdf_row.addWidget(btn_pdf)
        dsf_layout.addLayout(pdf_row)

        output_row = QHBoxLayout()
        output_row.setSpacing(SPACING["md"])
        output_row.addWidget(QLabel("Salvar em"))
        self.lbl_output = ElidedLabel("Pasta padrão: notificacoes/")
        self.lbl_output.setStyleSheet(f"color: {COLORS['muted']}; font-style: italic;")
        output_row.addWidget(self.lbl_output, 1)
        btn_output = QPushButton("Selecionar pasta")
        btn_output.setObjectName("SecondaryButton")
        btn_output.clicked.connect(self._select_output_directory)
        output_row.addWidget(btn_output)
        dsf_layout.addLayout(output_row)
        
        layout.addWidget(dsf_card)

        salvar_card = SectionCard("Salvar configuração reutilizável")
        salvar_layout = QHBoxLayout(salvar_card)
        self.input_nome_config = QLineEdit()
        self.input_nome_config.setPlaceholderText("Nome da configuração")
        self.input_nome_config.setMaximumWidth(360)
        salvar_layout.addWidget(self.input_nome_config)
        salvar_layout.addStretch()
        btn_salvar = QPushButton("Salvar perfil")
        btn_salvar.setObjectName("SuccessButton")
        btn_salvar.clicked.connect(self._save_profile)
        salvar_layout.addWidget(btn_salvar)
        layout.addWidget(salvar_card)

        layout.addStretch()
        self._load_saved_configs()

    def _load_saved_configs(self, selecionar: Optional[str] = None):
        self._configs = carregar_dados_salvos()
        self.combo_config.blockSignals(True)
        self.combo_config.clear()
        self.combo_config.addItem("— Novo preenchimento —")
        selected_index = 0
        for idx, nome in enumerate(self._configs, start=1):
            self.combo_config.addItem(nome)
            if nome == selecionar:
                selected_index = idx
        self.combo_config.setCurrentIndex(selected_index)
        self.combo_config.blockSignals(False)

    def _config_selected(self, index: int):
        if index <= 0:
            return
        nome = self.combo_config.currentText().strip()
        dados = self._configs.get(nome, {})
        self.inputs["auditor"].setText(dados.get("AUDITOR", ""))
        self.inputs["matricula"].setText(dados.get("MATRICULA", ""))
        self.inputs["contato"].setText(dados.get("CONTATO", ""))
        self.inputs["orgao"].setText(dados.get("ORGAO", ""))
        self.input_dsf.setText(dados.get("DSF", ""))
        titulo = dados.get("CARGO_TITULO", "Auditor")
        idx = self.combo_titulo.findText(titulo)
        if idx >= 0:
            self.combo_titulo.setCurrentIndex(idx)
        self.status_banner.set_status("info", f"Perfil '{nome}' carregado para edição.")

    def _select_pdf(self):
        caminho, _ = QFileDialog.getOpenFileName(
            self,
            "Selecionar PDF da DSF",
            str(ROOT_DIR / "dsf"),
            "Arquivos PDF (*.pdf)",
        )
        if not caminho:
            return
        self._pdf_path = Path(caminho)
        self.lbl_pdf.setText(str(self._pdf_path))
        self.status_banner.set_status("success", f"PDF selecionado: {self._pdf_path.name}")
        self.action_updated.emit()

    def _select_output_directory(self):
        diretorio_atual = str(self._output_dir) if self._output_dir else str(ROOT_DIR)
        pasta_selecionada = QFileDialog.getExistingDirectory(
            self,
            "Selecionar pasta de saída das notificações",
            diretorio_atual,
            QFileDialog.ShowDirsOnly | QFileDialog.DontResolveSymlinks,
        )
        if pasta_selecionada:
            self._output_dir = Path(pasta_selecionada)
            self.lbl_output.setText(str(self._output_dir))
            self.lbl_output.setStyleSheet(f"color: {COLORS['secondary']}; font-weight: 600;")
            self.status_banner.set_status("success", f"Pasta de saída definida: {self._output_dir.name}")
            self.action_updated.emit()

    def _save_profile(self):
        nome = self.input_nome_config.text().strip()
        if not nome:
            QMessageBox.warning(self, "Nome obrigatório", "Informe um nome para salvar o perfil.")
            return
        dados = self._collect_data()
        try:
            salvar_dados_manuais(nome, dados)
            self._load_saved_configs(selecionar=nome)
            self.input_nome_config.clear()
            self.status_banner.set_status("success", f"Perfil '{nome}' salvo com sucesso.")
        except Exception as exc:
            self.status_banner.set_status("danger", f"Não foi possível salvar o perfil: {exc}")

    def _collect_data(self) -> Dict[str, str]:
        return {
            "AUDITOR": self.inputs["auditor"].text().strip(),
            "MATRICULA": self.inputs["matricula"].text().strip(),
            "CONTATO": self.inputs["contato"].text().strip(),
            "ORGAO": self.inputs["orgao"].text().strip(),
            "CARGO_TITULO": self.combo_titulo.currentText().strip(),
            "DSF": self.input_dsf.text().strip(),
        }

    def load_state(self, state: WizardState):
        self._load_saved_configs()
        if not state.auditor_data:
            for campo in self.inputs.values():
                campo.clear()
            self.input_dsf.clear()
            self.combo_titulo.setCurrentIndex(0)
            self._pdf_path = None
            self.lbl_pdf.setText("Nenhum arquivo selecionado")
            return
        if state.auditor_data:
            dados = state.auditor_data
            self.inputs["auditor"].setText(dados.get("AUDITOR", ""))
            self.inputs["matricula"].setText(dados.get("MATRICULA", ""))
            self.inputs["contato"].setText(dados.get("CONTATO", ""))
            self.inputs["orgao"].setText(dados.get("ORGAO", ""))
            self.input_dsf.setText(dados.get("DSF", ""))
            idx = self.combo_titulo.findText(dados.get("CARGO_TITULO", "Auditor"))
            if idx >= 0:
                self.combo_titulo.setCurrentIndex(idx)
        if state.pdf_dsf:
            self._pdf_path = state.pdf_dsf
            self.lbl_pdf.setText(str(state.pdf_dsf))
        
        if state.diretorio_saida:
            self._output_dir = state.diretorio_saida
            self.lbl_output.setText(str(state.diretorio_saida))
            self.lbl_output.setStyleSheet(f"color: {COLORS['secondary']}; font-weight: 600;")
        else:
            self._output_dir = None
            self.lbl_output.setText("Pasta padrão: notificacoes/")
            self.lbl_output.setStyleSheet(f"color: {COLORS['muted']}; font-style: italic;")

    def persist_state(self, state: WizardState):
        state.auditor_data = self._collect_data()
        state.pdf_dsf = self._pdf_path
        state.diretorio_saida = self._output_dir

    def validate(self, state: WizardState) -> bool:
        if not state.auditor_data.get("DSF"):
            QMessageBox.warning(self, "Atenção", "Informe pelo menos o número da DSF para continuar.")
            return False
        return True

    def primary_action_label(self, state: WizardState) -> str:
        return "Confirmar auditor e seguir"

    def footer_context(self, state: WizardState) -> str:
        return "Etapa 3 de 5  •  Cadastre o responsável e a referência documental da DSF."


class PeriodPage(BaseWizardPage):
    page_title = "Período de Análise"
    page_subtitle = (
        "O resumo executivo foi convertido em grade de metadados, mais estável visualmente "
        "e mais claro para revisar antes do processamento."
    )

    def __init__(self):
        super().__init__()
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(SPACING["lg"])

        period_card = SectionCard("Recorte temporal")
        period_layout = QVBoxLayout(period_card)
        self.period_grid = AdaptiveFieldGrid()
        self.input_ini = QLineEdit("01/2021")
        self.input_ini.setPlaceholderText("MM/AAAA")
        self.input_ini.setMinimumHeight(36)
        self.period_grid.add_field("Data inicial", self.input_ini)

        self.input_fim = QLineEdit("12/2025")
        self.input_fim.setPlaceholderText("MM/AAAA")
        self.input_fim.setMinimumHeight(36)
        self.period_grid.add_field("Data final", self.input_fim)
        period_layout.addWidget(self.period_grid)
        layout.addWidget(period_card)

        resumo_card = SectionCard("Resumo da execução")
        resumo_layout = QVBoxLayout(resumo_card)
        self.metadata = MetadataGrid()
        resumo_layout.addWidget(self.metadata)
        layout.addWidget(resumo_card)
        layout.addStretch()

    def load_state(self, state: WizardState):
        self.input_ini.setText(state.periodo_inicio or "01/2021")
        self.input_fim.setText(state.periodo_fim or "12/2025")
        self._refresh_metadata(state)

    def persist_state(self, state: WizardState):
        state.periodo_inicio = self.input_ini.text().strip()
        state.periodo_fim = self.input_fim.text().strip()
        self._refresh_metadata(state)

    def validate(self, state: WizardState) -> bool:
        if not state.periodo_inicio or not state.periodo_fim:
            QMessageBox.warning(self, "Atenção", "Informe data inicial e data final antes de continuar.")
            return False
        return True

    def primary_action_label(self, state: WizardState) -> str:
        return "Preparar processamento"

    def primary_button_name(self, state: WizardState) -> str:
        return "SuccessButton"

    def footer_context(self, state: WizardState) -> str:
        return "Etapa 4 de 5  •  Revise os dados agregados antes de abrir a etapa operacional."

    def _refresh_metadata(self, state: WizardState):
        itens = [
            ("CNPJs válidos", str(len(state.cnpjs_validos()))),
            ("Auditor", state.auditor_data.get("AUDITOR", "—")),
            ("Matrícula", state.auditor_data.get("MATRICULA", "—")),
            ("DSF", state.auditor_data.get("DSF", "—")),
            ("Órgão", state.auditor_data.get("ORGAO", "—")),
            ("PDF DSF", state.pdf_dsf.name if state.pdf_dsf else "—"),
            ("Período", f"{state.periodo_inicio or '—'} a {state.periodo_fim or '—'}"),
        ]
        self.metadata.set_items(itens)


class ProcessingPage(BaseWizardPage):
    page_title = "Processamento"
    page_subtitle = (
        "A etapa final foi reorganizada com log e resultados em splitter vertical, "
        "sem limites fixos que comprimam o conteúdo."
    )

    def __init__(self):
        super().__init__()
        self._worker: Optional[WorkerThread] = None
        self._running = False
        self._completed = False
        self._output_dir: Optional[Path] = None
        self._signature = None

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(SPACING["lg"])

        self.status_banner = StatusBanner()
        layout.addWidget(self.status_banner)

        progress_card = SectionCard("Acompanhamento")
        progress_layout = QVBoxLayout(progress_card)
        top_row = QHBoxLayout()
        self.lbl_progress = QLabel("Pronto para iniciar.")
        self.lbl_progress.setStyleSheet("font-size: 15px; font-weight: 700;")
        top_row.addWidget(self.lbl_progress)
        top_row.addStretch()

        self.btn_cancel = QPushButton("Cancelar")
        self.btn_cancel.setObjectName("DangerButton")
        self.btn_cancel.setEnabled(False)
        self.btn_cancel.clicked.connect(self._cancel)
        top_row.addWidget(self.btn_cancel)

        self.btn_open = QPushButton("Abrir pasta de saída")
        self.btn_open.setEnabled(False)
        self.btn_open.clicked.connect(self._open_output_folder)
        top_row.addWidget(self.btn_open)
        progress_layout.addLayout(top_row)

        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        self.progress_bar.setFormat("%v / %m (%p%)")
        progress_layout.addWidget(self.progress_bar)
        layout.addWidget(progress_card)

        splitter = QSplitter(Qt.Vertical)
        splitter.setChildrenCollapsible(False)

        log_card = SectionCard("Log de execução")
        log_layout = QVBoxLayout(log_card)
        self.log_output = QTextEdit()
        self.log_output.setObjectName("LogOutput")
        self.log_output.setReadOnly(True)
        log_layout.addWidget(self.log_output)
        splitter.addWidget(log_card)

        result_card = SectionCard("Resultados")
        result_layout = QVBoxLayout(result_card)
        self.results_model = ResultsTableModel()
        self.results_view = DataTable()
        self.results_view.setModel(self.results_model)
        self.results_view.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.results_view.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.results_view.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        result_layout.addWidget(self.results_view)
        splitter.addWidget(result_card)
        splitter.setSizes([280, 280])
        layout.addWidget(splitter, 1)

    def load_state(self, state: WizardState):
        signature = (
            tuple(state.cnpjs_validos()),
            state.auditor_data.get("DSF"),
            state.periodo_inicio,
            state.periodo_fim,
        )
        if signature != self._signature and not self._running:
            self._signature = signature
            self._prepare_execution(state)
        elif state.logs_processamento and not self._running:
            self._restore_state(state)
        
        self._output_dir = state.diretorio_saida

    def persist_state(self, state: WizardState):
        state.logs_processamento = self.log_output.toPlainText().splitlines()
        state.resultados_processamento = self.results_model.results()
        state.resumo_processamento = {
            "diretorio_saida": self._output_dir,
            "concluido": self._completed,
            "executando": self._running,
        }

    def validate(self, state: WizardState) -> bool:
        if not state.cnpjs_validos():
            QMessageBox.warning(self, "Atenção", "Não há CNPJs válidos para processar.")
            return False
        return True

    def primary_action_label(self, state: WizardState) -> str:
        if self._running:
            return "Processando..."
        if self._completed:
            return "Processamento concluído"
        return "Iniciar processamento"

    def primary_button_name(self, state: WizardState) -> str:
        return "SuccessButton"

    def primary_action_enabled(self, state: WizardState) -> bool:
        return not self._running and not self._completed and bool(state.cnpjs_validos())

    def allow_back(self, state: WizardState) -> bool:
        return not self._running

    def handle_primary_action(self, state: WizardState) -> str:
        if self._running or self._completed:
            return "stay"
        self._start_processing(state)
        return "stay"

    def footer_context(self, state: WizardState) -> str:
        if self._completed:
            return "Etapa 5 de 5  •  Execução concluída. Revise resultados e abra a pasta de saída se necessário."
        if self._running:
            return "Etapa 5 de 5  •  Processamento em andamento. A navegação foi bloqueada para preservar o contexto."
        return "Etapa 5 de 5  •  Inicie a geração e acompanhe log e resultados na mesma tela."

    def _prepare_execution(self, state: WizardState):
        total = len(state.cnpjs_validos())
        self.status_banner.set_status(
            "info",
            f"Pronto para processar {total} CNPJ(s) no período {state.periodo_inicio} a {state.periodo_fim}.",
        )
        self.lbl_progress.setText(f"Pronto para iniciar o lote com {total} CNPJ(s).")
        self.progress_bar.setValue(0)
        self.progress_bar.setMaximum(max(1, total))
        self.results_model.clear()
        self.log_output.clear()
        self._completed = False
        # Usa o diretório do state ou o padrão
        self._output_dir = state.diretorio_saida
        self.btn_open.setEnabled(False)
        self.btn_cancel.setEnabled(False)
        self.action_updated.emit()

    def _restore_state(self, state: WizardState):
        self.log_output.setPlainText("\n".join(state.logs_processamento))
        self.results_model.clear()
        for result in state.resultados_processamento:
            self.results_model.add_result(result)
        self._completed = bool(state.resumo_processamento.get("concluido"))
        self._running = bool(state.resumo_processamento.get("executando"))
        self._output_dir = state.resumo_processamento.get("diretorio_saida")
        self.btn_open.setEnabled(bool(self._output_dir) and self._completed)
        self.btn_cancel.setEnabled(self._running)
        self.action_updated.emit()

    def _start_processing(self, state: WizardState):
        self._running = True
        self._completed = False
        self.results_model.clear()
        self.log_output.clear()
        self.progress_bar.setValue(0)
        self.progress_bar.setMaximum(max(1, len(state.cnpjs_validos())))
        self.btn_cancel.setEnabled(True)
        self.btn_open.setEnabled(False)
        self.status_banner.set_status("info", "Processamento iniciado. O log será atualizado em tempo real.")
        self.lbl_progress.setText("Iniciando processamento...")

        self._worker = WorkerThread(
            state.cnpjs_validos(),
            state.auditor_data,
            (state.periodo_inicio, state.periodo_fim),
            state.pdf_dsf,
            diretorio_saida=self._output_dir,
        )
        self._worker.progresso.connect(self._update_progress)
        self._worker.log_msg.connect(self._append_log)
        self._worker.cnpj_resultado.connect(self._append_result)
        self._worker.concluido.connect(self._finish_processing)
        self._worker.start()
        self.action_updated.emit()

    def _update_progress(self, atual: int, total: int):
        self.progress_bar.setMaximum(max(1, total))
        self.progress_bar.setValue(atual)
        self.lbl_progress.setText(f"Processando {atual} de {total} CNPJ(s)...")

    def _append_log(self, message: str):
        self.log_output.append(message)
        scrollbar = self.log_output.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def _append_result(self, cnpj: str, sucesso: bool, detalhe: str):
        self.results_model.add_result(ProcessingResult(cnpj=cnpj, sucesso=sucesso, detalhe=detalhe))

    def _finish_processing(self, resumo: dict):
        self._running = False
        self._completed = True
        self._output_dir = resumo.get("diretorio_saida")
        self.btn_cancel.setEnabled(False)
        self.btn_open.setEnabled(True)
        ok = resumo.get("sucessos", 0)
        falhas = resumo.get("falhas", 0)
        self.lbl_progress.setText(f"Concluído: {ok} sucesso(s), {falhas} falha(s)")
        if falhas:
            self.status_banner.set_status(
                "warning",
                f"Processamento concluído com {falhas} falha(s). Consulte a tabela de resultados para detalhes.",
            )
        else:
            self.status_banner.set_status("success", "Processamento concluído sem falhas.")
        self.action_updated.emit()
        self._prompt_next_action()

    def _cancel(self):
        if self._worker and self._worker.isRunning():
            self._worker.cancelar()
            self.btn_cancel.setEnabled(False)
            self.status_banner.set_status("warning", "Solicitação de cancelamento enviada. Aguarde a finalização da thread.")

    def _open_output_folder(self):
        if not self._output_dir:
            return
        subprocess.run(["explorer", str(self._output_dir)], check=False)

    def _prompt_next_action(self):
        msg = QMessageBox(self)
        msg.setWindowTitle("Processamento concluído")
        msg.setText("Deseja processar uma nova DSF ou fechar o programa?")
        msg.setIcon(QMessageBox.Question)

        nova_dsf_btn = msg.addButton("Processar nova DSF", QMessageBox.AcceptRole)
        fechar_btn = msg.addButton("Fechar programa", QMessageBox.RejectRole)
        msg.setDefaultButton(nova_dsf_btn)
        msg.exec()

        if msg.clickedButton() == nova_dsf_btn:
            self.workflow_requested.emit("new_dsf")
        elif msg.clickedButton() == fechar_btn:
            self.workflow_requested.emit("close")
