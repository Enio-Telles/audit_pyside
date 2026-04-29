from __future__ import annotations

import re

import polars as pl

from interface_grafica.services.query_worker import QueryWorker
from interface_grafica.services.sql_service import ParamInfo, WIDGET_DATE
from interface_grafica.utils.safe_slot import safe_slot
from interface_grafica.utils.validators import validate_cnpj, validate_date_range
from PySide6.QtCore import QDate
from PySide6.QtWidgets import QDateEdit, QLabel, QLineEdit


class SqlQueryControllerMixin:
    def _populate_sql_combo(self) -> None:
        """Carrega a lista de arquivos SQL disponiveis no combo."""
        self._sql_files = self.sql_service.list_sql_files()
        self.sql_combo.blockSignals(True)
        self.sql_combo.clear()
        self.sql_combo.addItem("- Selecione uma consulta -")
        for info in self._sql_files:
            self.sql_combo.addItem(
                f"{info.display_name}  [{info.source_dir}]", info.sql_id
            )
        self.sql_combo.blockSignals(False)
    def _on_sql_selected(self, index: int) -> None:
        """Ao selecionar um SQL no combo: le, exibe e gera o formulario de parametros."""
        if index <= 0:
            self.sql_text_view.setPlainText("")
            self._clear_param_form()
            self._sql_current_sql = ""
            return
        path_str = self.sql_combo.itemData(index)
        if not path_str:
            return
        try:
            sql_text = self.sql_service.read_sql(path_str)
        except Exception as exc:
            self.show_error("Erro ao ler SQL", str(exc))
            return
        self._sql_current_sql = sql_text
        self.sql_text_view.setPlainText(sql_text)
        params = self.sql_service.extract_params(sql_text)
        self._rebuild_param_form(params)
    def _clear_param_form(self) -> None:
        """Remove todos os campos do formulario de parametros."""
        while self.sql_param_form.rowCount() > 0:
            self.sql_param_form.removeRow(0)
        self._sql_param_widgets.clear()
    def _rebuild_param_form(self, params: list[ParamInfo]) -> None:
        """Reconstroi o formulario de parametros conforme os parametros detectados."""
        self._clear_param_form()
        for param in params:
            label = QLabel(f":{param.name}")
            label.setStyleSheet("font-weight: bold; color: #1e40af;")
            if param.widget_type == WIDGET_DATE:
                widget = QDateEdit()
                widget.setCalendarPopup(True)
                widget.setDate(QDate.currentDate())
                widget.setDisplayFormat("dd/MM/yyyy")
            else:
                widget = QLineEdit()
                if param.placeholder:
                    widget.setPlaceholderText(param.placeholder)
                # Pre-preencher CNPJ se disponAvel
                if "cnpj" in param.name.lower() and self.state.current_cnpj:
                    widget.setText(self.state.current_cnpj)
            self.sql_param_form.addRow(label, widget)
            self._sql_param_widgets[param.name] = widget
    def _collect_param_values(self) -> dict[str, str]:
        """Coleta os valores do formulario de parametros."""
        values: dict[str, str] = {}
        for name, widget in self._sql_param_widgets.items():
            if isinstance(widget, QDateEdit):
                values[name] = widget.date().toString("dd/MM/yyyy")
            elif isinstance(widget, QLineEdit):
                values[name] = widget.text().strip()
            else:
                values[name] = ""
        return values
    @safe_slot
    def _execute_sql_query(self) -> None:
        """Executa a consulta SQL em thread separada."""
        if not self._sql_current_sql:
            self.show_error("Nenhum SQL", "Selecione um arquivo SQL antes de executar.")
            return
        if self.query_worker is not None and self.query_worker.isRunning():
            self.show_error("Aguarde", "Uma consulta ja estA em execucao.")
            return

        values = self._collect_param_values()
        try:
            self._validate_sql_param_values(values)
        except ValueError as exc:
            self.show_error("Parametros invalidos", str(exc))
            return
        binds = self.sql_service.build_binds(self._sql_current_sql, values)

        self.btn_sql_execute.setEnabled(False)
        self._set_sql_status("a3 Conectando ao Oracle...", "#fef9c3", "#92400e")

        self.query_worker = QueryWorker(self._sql_current_sql, binds)
        self.query_worker.progress.connect(
            lambda msg: self._set_sql_status(f"a3 {msg}", "#fef9c3", "#92400e")
        )
        self.query_worker.finished_ok.connect(self._on_query_finished)
        self.query_worker.failed.connect(self._on_query_failed)
        self._registrar_limpeza_worker("query_worker", self.query_worker)
        self.query_worker.start()
    def _validate_sql_param_values(self, values: dict[str, str]) -> None:
        normalized_names = {name.lower(): name for name in values}
        for name, value in list(values.items()):
            if value and "cnpj" in name.lower():
                values[name] = validate_cnpj(value)

        starts: dict[tuple[str, ...], str] = {}
        ends: dict[tuple[str, ...], str] = {}
        for key, original in normalized_names.items():
            role, group = self._sql_date_param_role(key)
            if role == "start":
                starts[group] = original
            elif role == "end":
                ends[group] = original

        for group in starts.keys() & ends.keys():
            start_key = starts[group]
            end_key = ends[group]
            if values.get(start_key) and values.get(end_key):
                validate_date_range(values[start_key], values[end_key])

    @staticmethod
    def _sql_date_param_role(name: str) -> tuple[str | None, tuple[str, ...]]:
        tokens = [token for token in re.split(r"[^a-z0-9]+", name.lower()) if token]
        if not tokens:
            return None, ()

        for index, token in enumerate(tokens):
            if token in {"inicio", "start"}:
                return "start", tuple(tokens[:index] + tokens[index + 1 :])
            if token in {"fim", "end"}:
                return "end", tuple(tokens[:index] + tokens[index + 1 :])

        for index in range(len(tokens) - 1):
            pair = f"{tokens[index]}_{tokens[index + 1]}"
            if pair in {"data_ini", "dt_ini"}:
                return "start", tuple(tokens[:index] + tokens[index + 2 :])
            if pair in {"data_fim", "dt_fim"}:
                return "end", tuple(tokens[:index] + tokens[index + 2 :])
        return None, ()

    def _on_query_finished(self, df: pl.DataFrame) -> None:
        """Callback quando a consulta Oracle finaliza com sucesso."""
        self.btn_sql_execute.setEnabled(True)
        self._sql_result_df = df
        self._sql_result_page = 1
        self._reset_table_resize_flag("sql_result")
        if df.height == 0:
            self._set_sql_status(
                "a1i   Consulta retornou 0 resultados.", "#e0e7ff", "#3730a3"
            )
            self.sql_result_model.set_dataframe(pl.DataFrame())
        else:
            self._set_sql_status(
                f"a... {df.height:,} linhas, {df.width} colunas.", "#dcfce7", "#166534"
            )
            self._show_sql_result_page()
    def _on_query_failed(self, message: str) -> None:
        """Callback quando a consulta Oracle falha."""
        self.btn_sql_execute.setEnabled(True)
        self._set_sql_status(f"a Erro: {message[:200]}", "#fee2e2", "#991b1b")
    def _set_sql_status(self, text: str, bg: str, fg: str) -> None:
        self.sql_status_label.setText(text)
        self.sql_status_label.setStyleSheet(
            f"QLabel {{ padding: 4px 8px; background: {bg}; border-radius: 4px; "
            f"border: 1px solid {bg}; color: {fg}; font-weight: bold; }}"
        )
    def _show_sql_result_page(self) -> None:
        """Exibe a pAgina atual dos resultados SQL."""
        df = self._sql_result_df
        if df.height == 0:
            return
        total_pages = max(1, ((df.height - 1) // self._sql_result_page_size) + 1)
        self._sql_result_page = max(1, min(self._sql_result_page, total_pages))
        offset = (self._sql_result_page - 1) * self._sql_result_page_size
        page_df = df.slice(offset, self._sql_result_page_size)
        self.sql_result_model.set_dataframe(page_df)
        self._resize_table_once(self.sql_result_table, "sql_result")
        self.sql_result_page_label.setText(
            f"Pagina {self._sql_result_page}/{total_pages} | Total: {df.height:,}"
        )
    def _sql_prev_page(self) -> None:
        if self._sql_result_page > 1:
            self._sql_result_page -= 1
            self._show_sql_result_page()
    def _sql_next_page(self) -> None:
        total_pages = max(
            1, ((self._sql_result_df.height - 1) // self._sql_result_page_size) + 1
        )
        if self._sql_result_page < total_pages:
            self._sql_result_page += 1
            self._show_sql_result_page()
    def _filter_sql_results(self) -> None:
        """Aplica filtro textual global sobre os resultados SQL."""
        search = self.sql_result_search.text().strip().lower()
        if not search or self._sql_result_df.height == 0:
            self._sql_result_page = 1
            self._show_sql_result_page()
            return
        # Filtrar em todas as colunas (cast para string)
        exprs = [
            pl.col(c)
            .cast(pl.Utf8, strict=False)
            .fill_null("")
            .str.to_lowercase()
            .str.contains(search, literal=True)
            for c in self._sql_result_df.columns
        ]
        filtered = self._sql_result_df.filter(pl.any_horizontal(exprs))
        if filtered.height == 0:
            self._set_sql_status(
                f"Busca '{search}' nao encontrou resultados.", "#e0e7ff", "#3730a3"
            )
            self.sql_result_model.set_dataframe(pl.DataFrame())
        else:
            self._set_sql_status(
                f"a... Busca '{search}': {filtered.height:,} de {self._sql_result_df.height:,} linhas.",
                "#dcfce7",
                "#166534",
            )
            # Show first page of filtered results
            page_df = filtered.head(self._sql_result_page_size)
            self.sql_result_model.set_dataframe(page_df)
            self._resize_table_once(self.sql_result_table, "sql_result")
            total_pages = max(
                1, ((filtered.height - 1) // self._sql_result_page_size) + 1
            )
            self.sql_result_page_label.setText(
                f"Pagina 1/{total_pages} | Filtrado: {filtered.height:,}"
            )
    def _export_sql_results(self) -> None:
        """Exporta os resultados da consulta SQL para Excel."""
        if self._sql_result_df.height == 0:
            self.show_error("Sem dados", "Execute uma consulta antes de exportar.")
            return
        target = self._save_dialog(
            "Exportar resultados SQL para Excel", "Excel (*.xlsx)"
        )
        if not target:
            return
        try:
            sql_name = (
                self.sql_combo.currentText().split("[")[0].strip() or "consulta_sql"
            )
            df = self._dataframe_colunas_visiveis(
                self.sql_result_table, self.sql_result_model, self._sql_result_df
            )
            self.export_service.export_excel(target, df, sheet_name=sql_name[:31])
            self.show_info("Exportacao concluida", f"Arquivo gerado em:\n{target}")
        except Exception as exc:
            self.show_error("Falha na exportacao", str(exc))
