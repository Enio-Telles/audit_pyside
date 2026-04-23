from __future__ import annotations

import re

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QLabel, QLineEdit, QMessageBox, QPushButton

from interface_grafica.controllers.workers import PipelineWorker
from interface_grafica.ui.dialogs import DialogoSelecaoConsultas, DialogoSelecaoTabelas
from interface_grafica.services.pipeline_funcoes_service import ResultadoPipeline


class ImportacaoControllerMixin:
    def _verificar_conexoes(self) -> None:
        """Testa ambas as conexões Oracle e atualiza o painel de status no topo da aba."""
        if not hasattr(self, "_cfg_host"):
            return  # aba ainda não construída
        self._testar_conexao_para_status(
            self._cfg_host,
            self._cfg_port,
            self._cfg_service,
            self._cfg_user,
            self._cfg_password,
            self._cfg_conn_lbl_1,
            "_oracle_verify_worker_1",
        )
        self._testar_conexao_para_status(
            self._cfg_host_1,
            self._cfg_port_1,
            self._cfg_service_1,
            self._cfg_user_1,
            self._cfg_password_1,
            self._cfg_conn_lbl_2,
            "_oracle_verify_worker_2",
        )

    def _testar_conexao_para_status(
        self,
        f_host: QLineEdit,
        f_port: QLineEdit,
        f_service: QLineEdit,
        f_user: QLineEdit,
        f_password: QLineEdit,
        lbl: QLabel,
        worker_attr: str,
    ) -> None:
        """Worker isolado que atualiza apenas o label de status (sem botão dedicado)."""
        from interface_grafica.services.oracle_test_worker import (
            OracleConnectionTestWorker,
        )

        existing = getattr(self, worker_attr, None)
        if existing is not None and existing.isRunning():
            return

        lbl.setText("⏳ verificando…")
        lbl.setStyleSheet("color: #ccaa00;")

        worker = OracleConnectionTestWorker(
            host=f_host.text(),
            port=f_port.text(),
            service=f_service.text(),
            user=f_user.text(),
            password=f_password.text(),
            parent=self,
        )
        setattr(self, worker_attr, worker)

        def _on(ok: bool, msg: str, ms: int) -> None:
            first_line = msg.splitlines()[0] if msg else ""
            if ok:
                lbl.setText(f"✔ {first_line}")
                lbl.setStyleSheet("color: #4caf50; font-weight: bold;")
                self.status.showMessage(
                    f"[Oracle] {lbl.parent().parent().parent().title() if False else worker_attr.replace('_oracle_verify_worker_','Conexão ')} — OK ({ms} ms)",
                    5000,
                )
            else:
                short = first_line[:100]
                lbl.setText(f"✖ {short}")
                lbl.setStyleSheet("color: #e57373;")
            worker.deleteLater()
            setattr(self, worker_attr, None)

        worker.resultado.connect(_on)
        worker.start()

    def _testar_conexao(
        self,
        f_host: QLineEdit,
        f_port: QLineEdit,
        f_service: QLineEdit,
        f_user: QLineEdit,
        f_password: QLineEdit,
        btn: QPushButton,
        lbl: QLabel,
        worker_attr: str,
    ) -> None:
        """Lança o teste de conexão Oracle em background (não bloqueia a UI)."""
        from interface_grafica.services.oracle_test_worker import (
            OracleConnectionTestWorker,
        )

        # evitar múltiplos testes simultâneos no mesmo slot
        existing: OracleConnectionTestWorker | None = getattr(self, worker_attr, None)
        if existing is not None and existing.isRunning():
            return

        btn.setEnabled(False)
        lbl.setText("⏳ Testando…")
        lbl.setStyleSheet("color: #ccaa00;")

        worker = OracleConnectionTestWorker(
            host=f_host.text(),
            port=f_port.text(),
            service=f_service.text(),
            user=f_user.text(),
            password=f_password.text(),
            parent=self,
        )
        setattr(self, worker_attr, worker)

        def _on_result(ok: bool, msg: str, _ms: int) -> None:
            if ok:
                lbl.setText(f"✔ {msg}")
                lbl.setStyleSheet("color: #4caf50;")
            else:
                lbl.setText(f"✖ {msg}")
                lbl.setStyleSheet("color: #e57373;")
            btn.setEnabled(True)
            worker.deleteLater()
            setattr(self, worker_attr, None)

        worker.resultado.connect(_on_result)
        worker.start()

    def _salvar_configuracoes(self) -> None:
        """Escreve todos os campos do painel de configurações no arquivo .env."""
        from interface_grafica.fisconforme.path_resolver import get_env_path

        env_path = get_env_path()
        conteudo = env_path.read_text(encoding="utf-8") if env_path.exists() else ""

        campos: dict[str, str] = {
            "ORACLE_HOST": self._cfg_host.text().strip(),
            "ORACLE_PORT": self._cfg_port.text().strip(),
            "ORACLE_SERVICE": self._cfg_service.text().strip(),
            "DB_USER": self._cfg_user.text().strip(),
            "DB_PASSWORD": self._cfg_password.text().strip(),
            "ORACLE_HOST_1": self._cfg_host_1.text().strip(),
            "ORACLE_PORT_1": self._cfg_port_1.text().strip(),
            "ORACLE_SERVICE_1": self._cfg_service_1.text().strip(),
            "DB_USER_1": self._cfg_user_1.text().strip(),
            "DB_PASSWORD_1": self._cfg_password_1.text().strip(),
            "LOG_LEVEL": self._cfg_log_level.currentText(),
            "CACHE_ENABLED": "true" if self._cfg_cache_enabled.isChecked() else "false",
            "CACHE_TTL": self._cfg_cache_ttl.text().strip(),
            "DASHBOARD_THEME": self._cfg_theme.currentText(),
        }

        for chave, valor in campos.items():
            if re.search(rf"^{chave}=", conteudo, flags=re.MULTILINE):
                conteudo = re.sub(
                    rf"^{chave}=.*$",
                    f"{chave}={valor}",
                    conteudo,
                    flags=re.MULTILINE,
                )
            else:
                conteudo = conteudo.rstrip() + f"\n{chave}={valor}\n"

        env_path.parent.mkdir(parents=True, exist_ok=True)
        env_path.write_text(conteudo.strip() + "\n", encoding="utf-8")
        self._cfg_status_label.setText("✔ Configurações salvas.")
        self.status.showMessage("Configurações Oracle salvas com sucesso.", 4000)

    def refresh_cnpjs(self) -> None:
        known = {record.cnpj for record in self.registry_service.list_records()}
        known.update(self.parquet_service.list_cnpjs())
        current = self.state.current_cnpj
        self.cnpj_list.clear()
        for cnpj in sorted(known):
            self.cnpj_list.addItem(cnpj)
        if current:
            matches = self.cnpj_list.findItems(current, Qt.MatchExactly)
            if matches:
                self.cnpj_list.setCurrentItem(matches[0])

    def run_pipeline_for_input(self) -> None:
        try:
            cnpj = self.servico_pipeline_funcoes.servico_extracao.sanitizar_cnpj(
                self.cnpj_input.text()
            )
        except Exception as exc:
            self.show_error("CPF/CNPJ invalido", str(exc))
            return

        # 1. Selecionar Consultas SQL
        consultas_disp = (
            self.servico_pipeline_funcoes.servico_extracao.listar_consultas()
        )
        if not consultas_disp:
            self.show_error(
                "Sem consultas", "Nenhum arquivo .sql encontrado na pasta sql/"
            )
            return

        pre_sql = self.selection_service.get_selections("ultimas_consultas")
        dlg_sql = DialogoSelecaoConsultas(
            consultas_disp, self, pre_selecionados=pre_sql
        )
        if not dlg_sql.exec():
            return
        sql_selecionados = dlg_sql.consultas_selecionadas()
        self.selection_service.set_selections("ultimas_consultas", sql_selecionados)

        # 2. Selecionar Tabelas
        tabelas_disp = self.servico_pipeline_funcoes.servico_tabelas.listar_tabelas()
        pre_tabs = self.selection_service.get_selections("ultimas_tabelas")
        dlg_tab = DialogoSelecaoTabelas(tabelas_disp, self, pre_selecionados=pre_tabs)
        if not dlg_tab.exec():
            return
        tabelas_selecionadas = dlg_tab.tabelas_selecionadas()
        self.selection_service.set_selections("ultimas_tabelas", tabelas_selecionadas)

        if not sql_selecionados and not tabelas_selecionadas:
            return

        self.btn_run_pipeline.setEnabled(False)
        self.status.showMessage(f"Executando pipeline para {cnpj}...")

        data_limite = self.date_input.date().toString("dd/MM/yyyy")
        self.pipeline_worker = PipelineWorker(
            self.servico_pipeline_funcoes,
            cnpj,
            sql_selecionados,
            tabelas_selecionadas,
            data_limite,
        )
        self.pipeline_worker.finished_ok.connect(self.on_pipeline_finished)
        self.pipeline_worker.failed.connect(self.on_pipeline_failed)
        self.pipeline_worker.progress.connect(self.status.showMessage)
        self._registrar_limpeza_worker("pipeline_worker", self.pipeline_worker)
        self.pipeline_worker.start()

    def on_pipeline_finished(self, result: ResultadoPipeline) -> None:
        self.btn_run_pipeline.setEnabled(True)
        self.registry_service.upsert(result.cnpj, ran_now=True)
        self.status.showMessage(f"Pipeline concluido para {result.cnpj}.")
        self.refresh_cnpjs()
        matches = self.cnpj_list.findItems(result.cnpj, Qt.MatchExactly)
        if matches:
            self.cnpj_list.setCurrentItem(matches[0])
            self.refresh_file_tree(result.cnpj)
            self.atualizar_aba_conversao()
            self.atualizar_aba_mensal()
            self.atualizar_aba_anual()
            self.atualizar_aba_periodos()

        msg = (
            "\n".join(result.mensagens[-10:])
            if result.mensagens
            else "Processado com sucesso."
        )
        self.show_info(
            "Pipeline concluido",
            f"CNPJ {result.cnpj} processado.\n\nUltimas mensagens:\n{msg}",
        )

    def on_pipeline_failed(self, message: str) -> None:
        self.btn_run_pipeline.setEnabled(True)
        self.status.showMessage("Falha na execucao do pipeline.")
        self.show_error("Falha nao pipeline", message)

    def extrair_tabelas_brutas(self) -> None:
        """Executa apenas a extracao SQL (fase 1 do pipeline)."""
        cnpj = self._obter_cnpj_valido()
        if not cnpj:
            return

        consultas_disp = (
            self.servico_pipeline_funcoes.servico_extracao.listar_consultas()
        )
        if not consultas_disp:
            self.show_error(
                "Sem consultas", "Nenhum arquivo .sql encontrado na pasta sql/"
            )
            return

        pre_sql = self.selection_service.get_selections("ultimas_consultas")
        dlg_sql = DialogoSelecaoConsultas(
            consultas_disp, self, pre_selecionados=pre_sql
        )
        if not dlg_sql.exec():
            return
        sql_selecionados = dlg_sql.consultas_selecionadas()
        self.selection_service.set_selections("ultimas_consultas", sql_selecionados)

        self.btn_extrair_brutas.setEnabled(False)
        self.status.showMessage(f"Extraindo tabelas brutas para {cnpj}...")

        data_limite = self.date_input.date().toString("dd/MM/yyyy")
        self.pipeline_worker = PipelineWorker(
            self.servico_pipeline_funcoes,
            cnpj,
            sql_selecionados,
            [],  # sem tabelas a apenas extracao
            data_limite,
        )
        self.pipeline_worker.finished_ok.connect(self._on_extracao_finished)
        self.pipeline_worker.failed.connect(self._on_extracao_failed)
        self.pipeline_worker.progress.connect(self.status.showMessage)
        self._registrar_limpeza_worker("pipeline_worker", self.pipeline_worker)
        self.pipeline_worker.start()

    def _on_extracao_finished(self, result: ResultadoPipeline) -> None:
        self.btn_extrair_brutas.setEnabled(True)
        self._atualizar_estado_botao_nfe_entrada()
        self.status.showMessage(f"Extracao concluida para {result.cnpj}.")
        self.refresh_cnpjs()
        matches = self.cnpj_list.findItems(result.cnpj, Qt.MatchExactly)
        if matches:
            self.cnpj_list.setCurrentItem(matches[0])
            self.refresh_file_tree(result.cnpj)
        msg = (
            "\n".join(result.mensagens[-10:])
            if result.mensagens
            else "Extracao concluida."
        )
        self.show_info("Extracao concluida", f"CNPJ {result.cnpj}.\n\n{msg}")

    def _on_extracao_failed(self, message: str) -> None:
        self.btn_extrair_brutas.setEnabled(True)
        self._atualizar_estado_botao_nfe_entrada()
        self.status.showMessage("Falha na extracao.")
        self.show_error("Falha na extracao", message)

    def extrair_dados_nfe_entrada(self) -> None:
        cnpj = self._obter_cnpj_valido()
        if not cnpj:
            return
        if self.pipeline_worker is not None and self.pipeline_worker.isRunning():
            self.show_error(
                "Aguarde", "Ja existe uma extracao/processamento em execucao."
            )
            return
        if self.service_worker is not None and self.service_worker.isRunning():
            self.show_error("Aguarde", "Ja existe um processamento pesado em execucao.")
            return
        if self.query_worker is not None and self.query_worker.isRunning():
            self.show_error("Aguarde", "Ja existe uma consulta SQL em execucao.")
            return

        consultas_disp = (
            self.servico_pipeline_funcoes.servico_extracao.listar_consultas()
        )
        sql_nfe = next(
            (
                sql_id
                for sql_id in consultas_disp
                if sql_id.lower().endswith("/nfe.sql")
            ),
            None,
        )
        sql_nfce = next(
            (
                sql_id
                for sql_id in consultas_disp
                if sql_id.lower().endswith("/nfce.sql")
            ),
            None,
        )
        consultas_nfe_entrada = [p for p in [sql_nfe, sql_nfce] if p is not None]
        if not consultas_nfe_entrada:
            self.show_error(
                "SQL nao encontrada",
                "Nao foi possivel localizar as consultas NFe.sql/NFCe.sql na pasta sql/.",
            )
            return

        tabelas_necessarias = [
            "item_unidades",
            "itens",
            "descricao_produtos",
            "produtos_final",
            "fontes_produtos",
        ]
        self.btn_extract_nfe_entrada.setEnabled(False)
        self.status.showMessage(f"Extraindo dados da NFe Entrada para {cnpj}...")
        data_limite = self.date_input.date().toString("dd/MM/yyyy")
        self.pipeline_worker = PipelineWorker(
            self.servico_pipeline_funcoes,
            cnpj,
            consultas_nfe_entrada,
            tabelas_necessarias,
            data_limite,
        )
        self.pipeline_worker.finished_ok.connect(self._on_nfe_entrada_extract_finished)
        self.pipeline_worker.failed.connect(self._on_nfe_entrada_extract_failed)
        self.pipeline_worker.progress.connect(self.status.showMessage)
        self._registrar_limpeza_worker("pipeline_worker", self.pipeline_worker)
        self.pipeline_worker.start()

    def _on_nfe_entrada_extract_finished(self, result: ResultadoPipeline) -> None:
        self.btn_extract_nfe_entrada.setEnabled(True)
        self._atualizar_estado_botao_nfe_entrada()
        self.registry_service.upsert(result.cnpj, ran_now=True)
        self.status.showMessage(
            f"Extracao da NFe Entrada concluida para {result.cnpj}."
        )
        self.refresh_cnpjs()
        matches = self.cnpj_list.findItems(result.cnpj, Qt.MatchExactly)
        if matches:
            self.cnpj_list.setCurrentItem(matches[0])
            self.refresh_file_tree(result.cnpj)
        self.atualizar_aba_nfe_entrada()
        self.atualizar_aba_id_agrupados()
        msg = (
            "\n".join(result.mensagens[-10:])
            if result.mensagens
            else "Dados da NFe Entrada preparados com sucesso."
        )
        self.show_info("NFe Entrada concluida", f"CNPJ {result.cnpj}.\n\n{msg}")

    def _on_nfe_entrada_extract_failed(self, message: str) -> None:
        self.btn_extract_nfe_entrada.setEnabled(True)
        self._atualizar_estado_botao_nfe_entrada()
        self.status.showMessage("Falha na extracao da NFe Entrada.")
        self.show_error("Falha na NFe Entrada", message)

    def executar_processamento(self) -> None:
        """Executa apenas a geracao de tabelas (fase 2 do pipeline)."""
        cnpj = self._obter_cnpj_valido()
        if not cnpj:
            return

        tabelas_disp = self.servico_pipeline_funcoes.servico_tabelas.listar_tabelas()
        pre_tabs = self.selection_service.get_selections("ultimas_tabelas")
        dlg_tab = DialogoSelecaoTabelas(tabelas_disp, self, pre_selecionados=pre_tabs)
        if not dlg_tab.exec():
            return
        tabelas_selecionadas = dlg_tab.tabelas_selecionadas()
        self.selection_service.set_selections("ultimas_tabelas", tabelas_selecionadas)

        self.btn_processamento.setEnabled(False)
        self.status.showMessage(f"Gerando tabelas para {cnpj}...")

        self.pipeline_worker = PipelineWorker(
            self.servico_pipeline_funcoes,
            cnpj,
            [],  # sem consultas SQL a apenas processamento
            tabelas_selecionadas,
            None,
        )
        self.pipeline_worker.finished_ok.connect(self._on_processamento_finished)
        self.pipeline_worker.failed.connect(self._on_processamento_failed)
        self.pipeline_worker.progress.connect(self.status.showMessage)
        self._registrar_limpeza_worker("pipeline_worker", self.pipeline_worker)
        self.pipeline_worker.start()

    def _on_processamento_finished(self, result: ResultadoPipeline) -> None:
        self.btn_processamento.setEnabled(True)
        self._atualizar_estado_botao_nfe_entrada()
        self.status.showMessage(f"Processamento concluido para {result.cnpj}.")
        self.refresh_cnpjs()
        matches = self.cnpj_list.findItems(result.cnpj, Qt.MatchExactly)
        if matches:
            self.cnpj_list.setCurrentItem(matches[0])
            self.refresh_file_tree(result.cnpj)
            self.atualizar_aba_conversao()
            self.atualizar_aba_id_agrupados()
        msg = (
            "\n".join(result.mensagens[-10:])
            if result.mensagens
            else "Processamento concluido."
        )
        self.show_info("Processamento concluido", f"CNPJ {result.cnpj}.\n\n{msg}")

    def _on_processamento_failed(self, message: str) -> None:
        self.btn_processamento.setEnabled(True)
        self._atualizar_estado_botao_nfe_entrada()
        self.status.showMessage("Falha nao processamento.")
        self.show_error("Falha nao processamento", message)

    def apagar_dados_cnpj(self) -> None:
        """Apaga analises/ e arquivos_parquet/ do CNPJ selecionado (mantem pasta raiz)."""
        cnpj = self._obter_cnpj_valido()
        if not cnpj:
            return

        resp = QMessageBox.warning(
            self,
            "Apagar dados",
            f"Deseja apagar todos os dados (parquets e analises) do CNPJ {cnpj}?\n\nEsta acao nao pode ser desfeita.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if resp != QMessageBox.Yes:
            return

        ok = self.servico_pipeline_funcoes.servico_extracao.apagar_dados_cnpj(cnpj)
        if ok:
            self.show_info(
                "Dados apagados", f"Os dados do CNPJ {cnpj} foram removidos."
            )
            self.refresh_file_tree(cnpj)

    def apagar_cnpj_completo(self) -> None:
        """Remove a pasta inteira do CNPJ do filesystem e do registro SQL."""
        cnpj = self._obter_cnpj_valido()
        if not cnpj:
            return

        ret = QMessageBox.critical(
            self,
            "PERIGO: Apagar CNPJ",
            f"Isso removera permanentemente TODA a pasta do CNPJ {cnpj} e seus registros no banco.\n\nTem certeza absoluta?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if ret != QMessageBox.Yes:
            return

        try:
            self.servico_pipeline_funcoes.servico_extracao.apagar_cnpj_total(cnpj)
            self.registry_service.delete_by_cnpj(cnpj)
            self.show_info("Removido", f"CNPJ {cnpj} removido com sucesso.")
            self.refresh_cnpjs()
            self.file_tree.clear()
        except Exception as e:
            self.show_error("Erro ao apagar", str(e))
