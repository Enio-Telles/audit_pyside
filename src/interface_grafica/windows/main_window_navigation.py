from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import polars as pl
from interface_grafica.services.parquet_service import FilterCondition
from interface_grafica.controllers.workers import ServiceTaskWorker
from PySide6.QtCore import QDate, Qt, QUrl
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import QTreeWidgetItem


class MainWindowNavigationMixin:
    def _obter_cnpj_valido(self) -> str | None:
        if not self.state.current_cnpj:
            self.show_error(
                "CNPJ nao selecionado", "Selecione um CNPJ na lista a esquerda."
            )
            return None
        return self.state.current_cnpj

    def _executar_em_worker(
        self,
        func: Callable,
        *args,
        mensagem_inicial: str,
        on_success: Callable[[object], None],
        on_failure: Callable[[str], None] | None = None,
        **kwargs,
    ) -> bool:
        if self.service_worker is not None and self.service_worker.isRunning():
            self.show_error("Aguarde", "Ja existe um processamento pesado em execucao.")
            return False

        self.status.showMessage(mensagem_inicial)
        worker = ServiceTaskWorker(func, *args, **kwargs)
        self.service_worker = worker
        worker.progress.connect(self.status.showMessage)

        def _finalizar_ok(resultado) -> None:
            self.service_worker = None
            on_success(resultado)

        def _finalizar_erro(mensagem: str) -> None:
            self.service_worker = None
            if on_failure is not None:
                on_failure(mensagem)
            else:
                self.show_error("Erro", mensagem)

        worker.finished_ok.connect(_finalizar_ok)
        worker.failed.connect(_finalizar_erro)
        self._registrar_limpeza_worker("service_worker", worker)
        worker.start()
        return True

    def on_cnpj_selected(self) -> None:
        item = self.cnpj_list.currentItem()
        if not item:
            return
        cnpj = item.text()
        self.state.current_cnpj = cnpj
        self._produtos_sel_preselecionado_cnpj = None
        self._atualizar_estado_botao_nfe_entrada()
        self._reset_table_resize_flag("consulta")
        self._reset_table_resize_flag("conversao")
        self._reset_table_resize_flag("mov_estoque")
        self._reset_table_resize_flag("aba_mensal")
        self._reset_table_resize_flag("aba_anual")
        self._reset_table_resize_flag("estoque_codigo_produto")
        self._reset_table_resize_flag("nfe_entrada")
        self._reset_table_resize_flag("produtos_selecionados")
        self._reset_table_resize_flag("agregacao_top")
        self._reset_table_resize_flag("agregacao_bottom")
        self.status.showMessage(f"CNPJ selecionado: {cnpj}")
        self._refresh_profile_combos()
        self.refresh_file_tree(cnpj)

        # Lazy Loading: carregar apenas a aba visivel. A aba Consulta nao abre
        # automaticamente o primeiro arquivo para evitar I/O pesado inesperado.
        self._carregar_aba_atual()
        self.recarregar_historico_agregacao(cnpj)

        # Automacao de Data limite EFD baseada nao reg_0000
        data_efd = (
            self.servico_pipeline_funcoes.servico_extracao.obter_data_entrega_reg0000(
                cnpj
            )
        )
        if data_efd:
            qdate = QDate.fromString(data_efd, "dd/MM/yyyy")
            if qdate.isValid():
                self.date_input.setDate(qdate)

    def _carregar_aba_atual(self) -> None:
        """Carrega os dados da aba que está visível no momento para o CNPJ atual."""
        if not self.state.current_cnpj:
            return

        aba_idx = self.tabs.currentIndex()
        texto_aba = self.tabs.tabText(aba_idx).strip().lower()

        # Mapeamento de abas para funções de atualização
        if texto_aba == "agregacao":
            self.atualizar_tabelas_agregacao()
        elif texto_aba == "conversao":
            self.atualizar_aba_conversao()
        elif texto_aba == "estoque":
            self.atualizar_aba_mov_estoque()
        elif texto_aba == "nfe entrada":
            self.atualizar_aba_nfe_entrada()
        elif texto_aba == "logs":
            self.refresh_logs()
        elif "mensal" in texto_aba:
            self.atualizar_aba_mensal()
        elif "anual" in texto_aba:
            self.atualizar_aba_anual()
        elif "periodos" in texto_aba:
            self.atualizar_aba_periodos()
        elif "id" in texto_aba and "agrupado" in texto_aba:
            self.atualizar_aba_id_agrupados()

    def refresh_file_tree(self, cnpj: str) -> None:
        self.file_tree.clear()

        root_path = self.parquet_service.cnpj_dir(cnpj)

        cat_brutas = QTreeWidgetItem(
            ["Tabelas brutas (SQL)", str(root_path / "arquivos_parquet")]
        )
        cat_analises = QTreeWidgetItem(
            ["Analises de Produtos", str(root_path / "analises" / "produtos")]
        )
        cat_outros = QTreeWidgetItem(["Outros Parquets", str(root_path)])

        self.file_tree.addTopLevelItem(cat_brutas)
        self.file_tree.addTopLevelItem(cat_analises)
        self.file_tree.addTopLevelItem(cat_outros)

        first_leaf: QTreeWidgetItem | None = None

        for path in self.parquet_service.list_parquet_files(cnpj):
            # Identificar categoria
            if "arquivos_parquet" in str(path.parent):
                parent = cat_brutas
            elif "analises" in str(path.parent) or "produtos" in str(path.parent):
                parent = cat_analises
            else:
                parent = cat_outros

            item = QTreeWidgetItem([path.name, str(path.parent)])
            item.setData(0, Qt.UserRole, str(path))
            parent.addChild(item)
            if first_leaf is None:
                first_leaf = item

        cat_brutas.setExpanded(True)
        cat_analises.setExpanded(True)

        # Limpar categorias vazias
        for cat in [cat_brutas, cat_analises, cat_outros]:
            if cat.childCount() == 0:
                self.file_tree.takeTopLevelItem(self.file_tree.indexOfTopLevelItem(cat))

        self.state.current_file = None
        self.state.all_columns = []
        self.state.visible_columns = []
        self.state.total_rows = 0
        self.state.current_page = 1
        self.state.filters = []
        self.current_page_df_all = pl.DataFrame()
        self.current_page_df_visible = pl.DataFrame()
        self.table_model.set_dataframe(pl.DataFrame())
        self.filter_column.clear()
        self._refresh_filter_list_widget()
        self._update_page_label()
        self._update_context_label()
        if first_leaf is not None:
            self.file_tree.setCurrentItem(first_leaf)
            self.status.showMessage(
                "CNPJ selecionado. Escolha um arquivo na arvore para carregar a consulta."
            )

    def on_file_activated(self, item: QTreeWidgetItem, _column: int) -> None:
        raw_path = item.data(0, Qt.UserRole)
        if not raw_path:
            return
        self.state.current_file = Path(raw_path)
        self._reset_table_resize_flag("consulta")
        self.state.current_page = 1
        self.state.filters = []
        self.current_page_df_all = pl.DataFrame()
        self.current_page_df_visible = pl.DataFrame()
        self.load_current_file(reset_columns=True)
        self.tabs.setCurrentIndex(0)

    def load_current_file(self, reset_columns: bool = False) -> None:
        if self.state.current_file is None:
            return
        try:
            all_columns = self.parquet_service.get_schema(self.state.current_file)
        except Exception as exc:
            self.show_error("Erro ao abrir Parquet", str(exc))
            return
        self.state.all_columns = all_columns
        prefs = self._carregar_preferencias_tabela("consulta", self._consulta_scope())
        self._refresh_profile_combos()
        pref_visiveis = (
            prefs.get("visible_columns") if isinstance(prefs, dict) else None
        )
        if reset_columns or not self.state.visible_columns:
            self.state.visible_columns = (
                pref_visiveis
                if isinstance(pref_visiveis, list) and pref_visiveis
                else all_columns[:]
            )
        self.filter_column.clear()
        self.filter_column.addItems(all_columns)
        self.reload_table()

    def reload_table(self, update_main_view: bool = True) -> None:
        if self.state.current_file is None:
            return

        if not update_main_view:
            try:
                page_result = self.parquet_service.get_page(
                    parquet_path=self.state.current_file,
                    conditions=self.state.filters or [],
                    visible_columns=self.state.visible_columns or [],
                    page=self.state.current_page,
                    page_size=self.state.page_size,
                )
                self.state.total_rows = page_result.total_rows
                self.current_page_df_all = page_result.df_all_columns
                self.current_page_df_visible = page_result.df_visible
            except Exception as exc:
                self.show_error("Erro ao carregar dados", str(exc))
            return

        self._table_page_request_id += 1
        request_id = self._table_page_request_id
        parquet_path = self.state.current_file
        conditions = list(self.state.filters or [])
        visible_columns = list(self.state.visible_columns or [])
        page = self.state.current_page
        page_size = self.state.page_size

        if self.table_page_worker is not None and self.table_page_worker.isRunning():
            self.status.showMessage("Nova carga solicitada; resultado anterior sera ignorado.")

        def _load_page():
            return self.parquet_service.get_page(
                parquet_path=parquet_path,
                conditions=conditions,
                visible_columns=visible_columns,
                page=page,
                page_size=page_size,
            )

        worker = ServiceTaskWorker(_load_page)
        self.table_page_worker = worker
        self.status.showMessage(f"Carregando pagina {page}...")
        self.lbl_page.setText(f"Pagina {page} | carregando...")

        def _on_success(page_result) -> None:
            if request_id != self._table_page_request_id or parquet_path != self.state.current_file:
                return
            self.table_page_worker = None
            self.state.total_rows = page_result.total_rows
            self.current_page_df_all = page_result.df_all_columns
            self.current_page_df_visible = page_result.df_visible
            self.table_model.set_dataframe(self.current_page_df_visible)
            self._update_page_label()
            self._update_context_label()
            self._refresh_filter_list_widget()
            self._resize_table_once(self.table_view, "consulta")
            self._aplicar_preferencias_tabela(
                "consulta",
                self.table_view,
                self.table_model,
                self._consulta_scope(),
            )
            self.status.showMessage(
                f"Pagina {self.state.current_page} carregada: {self.current_page_df_visible.height:,} linhas visiveis."
            )

        def _on_failed(mensagem: str) -> None:
            if request_id != self._table_page_request_id:
                return
            self.table_page_worker = None
            self.show_error("Erro ao carregar dados", mensagem)

        def _cleanup() -> None:
            if self.table_page_worker is worker:
                self.table_page_worker = None
            worker.deleteLater()

        worker.finished_ok.connect(_on_success)
        worker.failed.connect(_on_failed)
        worker.finished.connect(_cleanup)
        worker.start()

    def _update_page_label(self) -> None:
        total_pages = max(
            1,
            (
                ((self.state.total_rows - 1) // self.state.page_size) + 1
                if self.state.total_rows
                else 1
            ),
        )
        if self.state.current_page > total_pages:
            self.state.current_page = total_pages
        self.lbl_page.setText(
            f"Pagina {self.state.current_page}/{total_pages} | Linhas filtradas: {self.state.total_rows}"
        )

    def _update_context_label(self) -> None:
        if self.state.current_file is None:
            self.lbl_context.setText(
                f"CNPJ: {self.state.current_cnpj or '-'} | Nenhum arquivo selecionado"
            )
            return
        self.lbl_context.setText(
            f"CNPJ: {self.state.current_cnpj or '-'} | Arquivo: {self.state.current_file.name} | "
            f"Colunas visiveis: {len(self.state.visible_columns or [])}/{len(self.state.all_columns or [])}"
        )

    def add_filter_from_form(self) -> None:
        column = self.filter_column.currentText().strip()
        operator = self.filter_operator.currentText().strip()
        value = self.filter_value.text().strip()
        if not column:
            self.show_error("Filtro invalido", "Selecione uma coluna para filtrar.")
            return
        if operator not in {"e nulo", "nao e nulo"} and value == "":
            self.show_error(
                "Filtro invalido", "Informe um valor para o filtro escolhido."
            )
            return
        self.state.filters = self.state.filters or []
        self.state.filters.append(
            FilterCondition(column=column, operator=operator, value=value)
        )
        self.state.current_page = 1
        self.filter_value.clear()
        self.reload_table()

    def clear_filters(self) -> None:
        self.state.filters = []
        self.state.current_page = 1
        self.reload_table()

    def remove_selected_filter(self) -> None:
        row = self.filter_list.currentRow()
        if row < 0 or not self.state.filters:
            return
        self.state.filters.pop(row)
        self.state.current_page = 1
        self.reload_table()

    def refresh_logs(self) -> None:
        import json
        from interface_grafica.config import CNPJ_ROOT

        cnpj = self.state.current_cnpj
        if not cnpj:
            self.log_view.setPlainText("Nenhum CNPJ selecionado.")
            return

        pasta = CNPJ_ROOT / cnpj / "analises" / "produtos"
        if not pasta.exists():
            self.log_view.setPlainText(f"Pasta de analises nao encontrada para {cnpj}.")
            return

        arquivos = sorted(pasta.glob("log_*.json"))
        if not arquivos:
            self.log_view.setPlainText(f"Nenhum arquivo de log encontrado para {cnpj}.")
            return

        secoes: list[str] = []
        for arq in arquivos:
            try:
                with open(arq, encoding="utf-8") as f:
                    dados = json.load(f)
                conteudo = json.dumps(dados, ensure_ascii=False, indent=2)
            except Exception as e:
                conteudo = f"(erro ao ler: {e})"
            secoes.append(f"=== {arq.name} ===\n{conteudo}")

        # Log de agregacoes (lista de eventos)
        linhas_agr = self.servico_agregacao.ler_linhas_log(cnpj)
        if linhas_agr:
            limite = 1000
            if len(linhas_agr) > limite:
                linhas_agr = linhas_agr[-limite:]
                cabecalho = f"=== log_agregacoes (ultimas {limite} entradas) ==="
            else:
                cabecalho = f"=== log_agregacoes ({len(linhas_agr)} entradas) ==="
            eventos = [json.dumps(e, ensure_ascii=False) for e in linhas_agr]
            secoes.append(cabecalho + "\n" + "\n".join(eventos))

        self.log_view.setPlainText("\n\n".join(secoes))
        if hasattr(self, "lbl_logs_status"):
            self.lbl_logs_status.setText(
                f"{len(arquivos) + (1 if linhas_agr else 0)} arquivo(s) carregado(s) — CNPJ {cnpj}"
            )

    def open_cnpj_folder(self) -> None:
        if not self.state.current_cnpj:
            self.show_error(
                "CNPJ nao selecionado", "Selecione um CNPJ para abrir a pasta."
            )
            return
        target = self.parquet_service.cnpj_dir(self.state.current_cnpj)
        if not target.exists():
            self.show_error(
                "Pasta inexistente", f"A pasta {target} ainda nao foi criada."
            )
            return
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(target)))
