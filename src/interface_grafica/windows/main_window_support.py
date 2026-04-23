from __future__ import annotations

from collections.abc import Callable
import logging

import polars as pl
from interface_grafica.config import CNPJ_ROOT
from interface_grafica.models.table_model import PolarsTableModel
from interface_grafica.services.parquet_service import FilterCondition
from interface_grafica.widgets.detached_table_window import DetachedTableWindow
from PySide6.QtCore import QThread, Qt, QTimer
from PySide6.QtGui import QGuiApplication, QKeySequence, QShortcut
from PySide6.QtWidgets import QMenu, QMessageBox, QPushButton, QTableView

logger = logging.getLogger(__name__)


class MainWindowSupportMixin:
    def _executar_callback_debounce(self, key: str) -> None:
        callback = self._debounce_callbacks.get(key)
        if callback is None:
            return
        callback()
    def _schedule_debounced(
        self, key: str, callback: Callable[[], None], delay_ms: int = 280
    ) -> None:
        timer = self._debounce_timers.get(key)
        if timer is None:
            timer = QTimer(self)
            timer.setSingleShot(True)
            timer.timeout.connect(lambda key=key: self._executar_callback_debounce(key))
            self._debounce_timers[key] = timer
        self._debounce_callbacks[key] = callback
        timer.start(delay_ms)
    def _registrar_limpeza_worker(self, attr_name: str, worker: QThread) -> None:
        def _cleanup() -> None:
            if getattr(self, attr_name, None) is worker:
                setattr(self, attr_name, None)
            worker.deleteLater()
            self._atualizar_estado_botao_nfe_entrada()
            if self._closing_after_workers:
                self._tentar_fechar_apos_workers()

        worker.finished.connect(_cleanup)
    def _workers_ativos(self) -> list[QThread]:
        ativos: list[QThread] = []
        for worker in (self.pipeline_worker, self.query_worker, self.service_worker):
            if worker is not None and worker.isRunning():
                ativos.append(worker)
        return ativos
    def _atualizar_estado_botao_nfe_entrada(self) -> None:
        if not hasattr(self, "btn_extract_nfe_entrada"):
            return
        habilitado = bool(self.state.current_cnpj) and not self._workers_ativos()
        self.btn_extract_nfe_entrada.setEnabled(habilitado)
    def _tentar_fechar_apos_workers(self) -> None:
        if self._workers_ativos():
            return
        self._closing_after_workers = False
        self.close()
    def closeEvent(self, event) -> None:
        ativos = self._workers_ativos()
        if not ativos:
            super().closeEvent(event)
            return

        if not self._closing_after_workers:
            self._closing_after_workers = True
            self.status.showMessage(
                "Aguardando o termino das operacoes em execucao para fechar a janela..."
            )
            self.setEnabled(False)
            for worker in ativos:
                worker.finished.connect(self._tentar_fechar_apos_workers)
            QTimer.singleShot(100, self._tentar_fechar_apos_workers)
        event.ignore()
    def _resize_table_once(self, table: QTableView, key: str) -> None:
        if key in self._auto_resized_tables:
            return
        table.resizeColumnsToContents()
        self._auto_resized_tables.add(key)
    def _reset_table_resize_flag(self, key: str) -> None:
        self._auto_resized_tables.discard(key)
    def _estilo_botao_destacar(self) -> str:
        return (
            "QPushButton { background: #0e639c; color: #ffffff; border: 1px solid #1177bb; "
            "border-radius: 4px; padding: 6px 10px; font-weight: bold; }"
            "QPushButton:hover { background: #1177bb; }"
            "QPushButton:pressed { background: #0b4f7c; }"
        )
    def _criar_botao_destacar(self, texto: str = "Destacar") -> QPushButton:
        botao = QPushButton(texto)
        botao.setStyleSheet(self._estilo_botao_destacar())
        return botao
    def _abrir_fio_de_ouro(self, id_agrupado: str) -> None:
        if not self.state.current_cnpj:
            return

        pasta_analises = CNPJ_ROOT / self.state.current_cnpj / "analises" / "produtos"
        arquivos = list(
            pasta_analises.glob(f"*_enriquecido_{self.state.current_cnpj}.parquet")
        )
        dfs = []
        filtro_id = [
            FilterCondition(column="id_agrupado", operator="igual", value=id_agrupado)
        ]
        for arq in arquivos:
            try:
                schema = self.parquet_service.get_schema(arq)
                if "id_agrupado" not in schema:
                    continue
                df = self.parquet_service.load_dataset(arq, filtro_id)
                if not df.is_empty():
                    df = df.with_columns(
                        pl.lit(arq.name.split("_enriquecido")[0].upper()).alias(
                            "origem_fio_ouro"
                        )
                    )
                    dfs.append(df)
            except Exception:
                logger.exception(
                    "Falha ao carregar parquet enriquecido para Fio de Ouro: %s",
                    arq,
                )

        if not dfs:
            self.show_info(
                "Fio de Ouro",
                f"Nenhum registro enriquecido encontrado para: {id_agrupado}.",
            )
            return

        try:
            df_final = pl.concat(dfs, how="diagonal_relaxed")
            from interface_grafica.ui.dialogs import DialogoFioDeOuro

            dlg = DialogoFioDeOuro(df_final, self)
            dlg.exec()
        except Exception as e:
            self.show_error("Fio de Ouro", f"Erro ao gerar trilha de auditoria: {e}")
    def _copiar_valor_celula(self, table: QTableView, index) -> None:
        if not index or not index.isValid():
            return
        valor = index.data(Qt.DisplayRole)
        QGuiApplication.clipboard().setText("" if valor is None else str(valor))
    def _abrir_menu_contexto_celula(
        self, contexto: str, table: QTableView, pos
    ) -> None:
        index = table.indexAt(pos)
        if not index.isValid():
            return

        menu = QMenu(self)
        acao_copiar = menu.addAction("Copiar valor")
        acao_copiar.triggered.connect(lambda: self._copiar_valor_celula(table, index))

        model = table.model()
        if (
            contexto == "mov_estoque"
            and isinstance(model, PolarsTableModel)
            and not model.get_dataframe().is_empty()
            and "id_agrupado" in model.get_dataframe().columns
        ):
            try:
                id_agrupado = model.get_dataframe()["id_agrupado"][index.row()]
            except Exception:
                id_agrupado = None
            if id_agrupado:
                menu.addSeparator()
                acao = menu.addAction(f"Auditoria 'Fio de Ouro' ({id_agrupado})")
                acao.triggered.connect(lambda: self._abrir_fio_de_ouro(id_agrupado))

        menu.exec(table.viewport().mapToGlobal(pos))
    def show_error(self, title: str, message: str) -> None:
        QMessageBox.critical(self, title, message)
    def show_info(self, title: str, message: str) -> None:
        QMessageBox.information(self, title, message)
    def _setup_copy_shortcut(self) -> None:
        self.shortcut_copy = QShortcut(QKeySequence.StandardKey.Copy, self)
        self.shortcut_copy.activated.connect(self._copy_selection_from_active_table)
    def _copy_selection_from_active_table(self) -> None:
        tables = [
            self.table_view,
            self.aggregation_table_view,
            self.results_table_view,
            self.sql_result_table,
            self.conversion_table,
            self.mov_estoque_table,
            self.aba_mensal_table,
            self.aba_anual_table,
            self.nfe_entrada_table,
            self.id_agrupados_table,
            self.produtos_sel_table,
        ]
        tables.extend(
            janela.table
            for janela in self._detached_windows.values()
            if janela is not None
        )
        active_table = next((t for t in tables if t and t.hasFocus()), None)
        if active_table is None:
            return

        selected_indexes = active_table.selectedIndexes()
        if not selected_indexes:
            return

        selected_indexes = sorted(selected_indexes, key=lambda i: (i.row(), i.column()))
        row_min = min(i.row() for i in selected_indexes)
        row_max = max(i.row() for i in selected_indexes)
        col_min = min(i.column() for i in selected_indexes)
        col_max = max(i.column() for i in selected_indexes)
        selected_map = {(i.row(), i.column()): i for i in selected_indexes}

        lines: list[str] = []
        for r in range(row_min, row_max + 1):
            vals: list[str] = []
            for c in range(col_min, col_max + 1):
                idx = selected_map.get((r, c))
                vals.append(str(idx.data() if idx is not None else ""))
            lines.append("\t".join(vals))

        QGuiApplication.clipboard().setText("\n".join(lines))
    def _detached_title(self, contexto: str) -> str:
        cnpj = self.state.current_cnpj or "sem CNPJ"
        mapa = {
            "consulta": f"Consulta - {cnpj}",
            "sql_result": f"Consulta SQL - {cnpj}",
            "agregacao_top": f"Agregacao - Tabela Superior - {cnpj}",
            "agregacao_bottom": f"Agregacao - Tabela Inferior - {cnpj}",
            "conversao": f"Conversao - {cnpj}",
            "mov_estoque": f"Movimentacao de Estoque - {cnpj}",
            "aba_mensal": f"Tabela Mensal - {cnpj}",
            "aba_anual": f"Tabela Anual - {cnpj}",
            "nfe_entrada": f"NFe Entrada - {cnpj}",
            "id_agrupados": f"id_agrupados - {cnpj}",
            "produtos_selecionados": f"Produtos Selecionados - {cnpj}",
        }
        return mapa.get(contexto, f"Tabela Destacada - {cnpj}")
    def _detached_assets(
        self, contexto: str
    ) -> tuple[QTableView | None, PolarsTableModel | None]:
        mapa = {
            "consulta": (self.table_view, self.table_model),
            "sql_result": (self.sql_result_table, self.sql_result_model),
            "agregacao_top": (self.aggregation_table, self.aggregation_table_model),
            "agregacao_bottom": (self.results_table, self.results_table_model),
            "conversao": (self.conversion_table, self.conversion_model),
            "mov_estoque": (self.mov_estoque_table, self.mov_estoque_model),
            "aba_mensal": (self.aba_mensal_table, self.aba_mensal_model),
            "aba_anual": (self.aba_anual_table, self.aba_anual_model),
            "nfe_entrada": (self.nfe_entrada_table, self.nfe_entrada_model),
            "id_agrupados": (self.id_agrupados_table, self.id_agrupados_model),
            "produtos_selecionados": (
                self.produtos_sel_table,
                self.produtos_selecionados_model,
            ),
        }
        return mapa.get(contexto, (None, None))
    def _detached_scope(self, contexto: str) -> str | None:
        if contexto == "consulta":
            return self._consulta_scope()
        return None
    def _on_detached_window_closed(self, contexto: str) -> None:
        self._detached_windows.pop(contexto, None)
    def _destacar_tabela(self, contexto: str) -> None:
        table, source_model = self._detached_assets(contexto)
        if table is None or source_model is None:
            self.show_error(
                "Tabela indisponivel",
                "Nao foi possivel localizar a tabela para destacar.",
            )
            return
        if source_model.dataframe.is_empty():
            self.show_error(
                "Tabela vazia", "Nao ha dados carregados nessa tabela para destacar."
            )
            return

        janela_existente = self._detached_windows.get(contexto)
        if janela_existente is not None:
            janela_existente.show()
            janela_existente.raise_()
            janela_existente.activateWindow()
            return

        janela = DetachedTableWindow(
            self._detached_title(contexto), contexto, source_model, self
        )
        self._atualizar_combo_perfis_tabela(
            janela.profile_combo,
            contexto,
            ["Padrao", "Auditoria", "Estoque", "Custos"],
            scope=self._detached_scope(contexto),
        )
        janela.btn_apply_profile.clicked.connect(
            lambda _checked=False,
            ctx=contexto,
            t=janela.table,
            m=janela.table_model,
            combo=janela.profile_combo: self._aplicar_perfil_tabela(
                ctx, t, m, combo.currentText(), ctx, scope=self._detached_scope(ctx)
            )
        )
        janela.btn_save_profile.clicked.connect(
            lambda _checked=False,
            ctx=contexto,
            t=janela.table,
            m=janela.table_model,
            combo=janela.profile_combo: self._salvar_perfil_tabela_com_dialogo(
                ctx,
                t,
                m,
                combo,
                ["Padrao", "Auditoria", "Estoque", "Custos"],
                scope=self._detached_scope(ctx),
            )
        )
        janela.btn_columns.clicked.connect(
            lambda _checked=False,
            ctx=contexto,
            t=janela.table: self._abrir_menu_colunas_tabela(
                ctx, t, scope=self._detached_scope(ctx)
            )
        )
        janela.closed.connect(self._on_detached_window_closed)
        janela.table.customContextMenuRequested.connect(
            lambda pos, t=janela.table, ctx=contexto: self._abrir_menu_contexto_celula(
                ctx, t, pos
            )
        )
        janela.table.horizontalHeader().customContextMenuRequested.connect(
            lambda pos,
            t=janela.table,
            m=janela.table_model,
            ctx=contexto: self._abrir_menu_colunas_tabela(
                ctx, t, pos, scope=self._detached_scope(ctx)
            )
        )
        janela.table.horizontalHeader().sectionMoved.connect(
            lambda *_args,
            t=janela.table,
            m=janela.table_model,
            ctx=contexto: self._salvar_preferencias_tabela(
                ctx, t, m, scope=self._detached_scope(ctx)
            )
        )
        janela.table.horizontalHeader().sectionResized.connect(
            lambda *_args,
            t=janela.table,
            m=janela.table_model,
            ctx=contexto: self._salvar_preferencias_tabela(
                ctx, t, m, scope=self._detached_scope(ctx)
            )
        )
        janela.table.horizontalHeader().sortIndicatorChanged.connect(
            lambda _index,
            _order,
            t=janela.table,
            m=janela.table_model,
            ctx=contexto: self._salvar_preferencias_tabela(
                ctx, t, m, scope=self._detached_scope(ctx)
            )
        )
        self._aplicar_preferencias_tabela(
            contexto,
            janela.table,
            janela.table_model,
            scope=self._detached_scope(contexto),
        )
        janela.show()
        self._detached_windows[contexto] = janela
    def _destacar_tabela_estoque(self, contexto: str) -> None:
        self._destacar_tabela(contexto)
    def _toggle_left_panel(self, checked: bool) -> None:
        if checked:
            self.left_panel_widget.hide()
            self.btn_toggle_panel.setText(">> Mostrar Painel Lateral")
        else:
            self.left_panel_widget.show()
            self.btn_toggle_panel.setText("<< Ocultar Painel Lateral")
    def _atualizar_titulo_aba_mov_estoque(
        self, visiveis: int | None = None, total: int | None = None
    ) -> None:
        if not hasattr(self, "estoque_tabs") or not hasattr(self, "tab_mov_estoque"):
            return
        idx = self.estoque_tabs.indexOf(self.tab_mov_estoque)
        if idx < 0:
            return
        if visiveis is None:
            self.estoque_tabs.setTabText(idx, "Tabela mov_estoque")
            return
        self.estoque_tabs.setTabText(idx, f"Tabela mov_estoque ({visiveis})")
    def _atualizar_titulo_aba_produtos_selecionados(
        self, visiveis: int | None = None, total: int | None = None
    ) -> None:
        if not hasattr(self, "estoque_tabs") or not hasattr(
            self, "tab_produtos_selecionados"
        ):
            return
        idx = self.estoque_tabs.indexOf(self.tab_produtos_selecionados)
        if idx < 0:
            return
        if visiveis is None:
            self.estoque_tabs.setTabText(idx, "Produtos selecionados")
            return
        if total is None:
            self.estoque_tabs.setTabText(idx, f"Produtos selecionados ({visiveis})")
            return
        self.estoque_tabs.setTabText(idx, f"Produtos selecionados ({visiveis}/{total})")
    def _atualizar_titulo_aba_id_agrupados(
        self, visiveis: int | None = None, total: int | None = None
    ) -> None:
        if not hasattr(self, "estoque_tabs") or not hasattr(self, "tab_id_agrupados"):
            return
        idx = self.estoque_tabs.indexOf(self.tab_id_agrupados)
        if idx < 0:
            return
        if visiveis is None:
            self.estoque_tabs.setTabText(idx, "id_agrupados")
            return
        if total is None:
            self.estoque_tabs.setTabText(idx, f"id_agrupados ({visiveis})")
            return
        self.estoque_tabs.setTabText(idx, f"id_agrupados ({visiveis}/{total})")
    def _atualizar_titulo_aba_mensal(
        self, visiveis: int | None = None, total: int | None = None
    ) -> None:
        if not hasattr(self, "estoque_tabs") or not hasattr(self, "tab_aba_mensal"):
            return
        idx = self.estoque_tabs.indexOf(self.tab_aba_mensal)
        if idx < 0:
            return
        if visiveis is None or total is None:
            self.estoque_tabs.setTabText(idx, "Tabela mensal")
            return
        self.estoque_tabs.setTabText(idx, f"Tabela mensal ({visiveis}/{total})")
    def _atualizar_titulo_aba_anual(
        self, visiveis: int | None = None, total: int | None = None
    ) -> None:
        if not hasattr(self, "estoque_tabs") or not hasattr(self, "tab_aba_anual"):
            return
        idx = self.estoque_tabs.indexOf(self.tab_aba_anual)
        if idx < 0:
            return
        if visiveis is None or total is None:
            self.estoque_tabs.setTabText(idx, "Tabela anual")
            return
        self.estoque_tabs.setTabText(idx, f"Tabela anual ({visiveis}/{total})")
    def _atualizar_titulo_aba_nfe_entrada(
        self, visiveis: int | None = None, total: int | None = None
    ) -> None:
        if not hasattr(self, "tabs") or not hasattr(self, "tab_nfe_entrada"):
            return
        idx = self.tabs.indexOf(self.tab_nfe_entrada)
        if idx < 0:
            return
        if visiveis is None:
            self.tabs.setTabText(idx, "NFe Entrada")
            return
        if total is None:
            self.tabs.setTabText(idx, f"NFe Entrada ({visiveis})")
            return
        self.tabs.setTabText(idx, f"NFe Entrada ({visiveis}/{total})")
    def _atualizar_titulo_aba_periodos(
        self, visiveis: int | None = None, total: int | None = None
    ) -> None:
        if not hasattr(self, "estoque_tabs") or not hasattr(self, "tab_aba_periodos"):
            return
        idx = self.estoque_tabs.indexOf(self.tab_aba_periodos)
        if idx < 0:
            return
        if visiveis is None:
            self.estoque_tabs.setTabText(idx, "Tabela períodos")
            return
        if total is None:
            self.estoque_tabs.setTabText(idx, f"Tabela períodos ({visiveis})")
            return
        self.estoque_tabs.setTabText(idx, f"Tabela períodos ({visiveis}/{total})")
