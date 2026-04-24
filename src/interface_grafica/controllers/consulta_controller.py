from __future__ import annotations

from pathlib import Path

import polars as pl
from interface_grafica.config import CONSULTAS_ROOT
from PySide6.QtWidgets import QFileDialog

from interface_grafica.ui.dialogs import ColumnSelectorDialog
from interface_grafica.utils.retry import retry_on_io


class ConsultaControllerMixin:
    def _refresh_filter_list_widget(self) -> None:
        self.filter_list.clear()
        for cond in self.state.filters or []:
            text = f"{cond.column} {cond.operator} {cond.value}".strip()
            self.filter_list.addItem(text)
    def choose_columns(self) -> None:
        if not self.state.all_columns:
            return
        dialog = ColumnSelectorDialog(
            self.state.all_columns,
            self.state.visible_columns or self.state.all_columns,
            self,
        )
        if dialog.exec():
            selected = dialog.selected_columns()
            if not selected:
                self.show_error(
                    "Selecao invAlida", "Pelo menos uma coluna deve permanecer visivel."
                )
                return
            self.state.visible_columns = selected
            self.state.current_page = 1
            prefs = self._carregar_preferencias_tabela(
                "consulta", self._consulta_scope()
            )
            prefs["visible_columns"] = selected
            prefs.pop("header_state", None)
            self.selection_service.set_value(
                self._preferencia_tabela_key("consulta", self._consulta_scope()), prefs
            )
            self.reload_table()
    def prev_page(self) -> None:
        if self.state.current_page > 1:
            self.state.current_page -= 1
            self.reload_table()
    def next_page(self) -> None:
        total_pages = max(
            1,
            (
                ((self.state.total_rows - 1) // self.state.page_size) + 1
                if self.state.total_rows
                else 1
            ),
        )
        if self.state.current_page < total_pages:
            self.state.current_page += 1
            self.reload_table()
    def _save_dialog(self, title: str, pattern: str) -> Path | None:
        filename, _ = QFileDialog.getSaveFileName(
            self, title, str(CONSULTAS_ROOT), pattern
        )
        return Path(filename) if filename else None
    def _filters_text(self) -> str:
        return " | ".join(
            f"{f.column} {f.operator} {f.value}".strip()
            for f in self.state.filters or []
        )
    @retry_on_io()
    def _dataset_for_export(self, mode: str) -> pl.DataFrame:
        if self.state.current_file is None:
            raise ValueError("Nenhum arquivo selecionado.")
        if mode == "full":
            return self.parquet_service.load_dataset(self.state.current_file)
        if mode == "filtered":
            return self.parquet_service.load_dataset(
                self.state.current_file, self.state.filters or []
            )
        if mode == "visible":
            return self.parquet_service.load_dataset(
                self.state.current_file,
                self.state.filters or [],
                self.state.visible_columns or [],
            )
        raise ValueError(f"Modo de exportacao nao suportado: {mode}")
    def export_excel(self, mode: str) -> None:
        try:
            df = self._dataset_for_export(mode)
            if mode != "visible":
                df = self._dataframe_colunas_visiveis(
                    self.table_view, self.table_model, df
                )
            target = self._save_dialog("Salvar Excel", "Excel (*.xlsx)")
            if not target:
                return
            self.export_service.export_excel(
                target,
                df,
                sheet_name=(
                    self.state.current_file.stem if self.state.current_file else "Dados"
                ),
            )
            self.show_info("Exportacao concluida", f"Arquivo gerado em:\n{target}")
        except Exception as exc:
            self.show_error("Falha na exportacao para Excel", str(exc))
    def export_docx(self) -> None:
        try:
            if self.state.current_file is None:
                raise ValueError("Nenhum arquivo selecionado.")
            df = self.parquet_service.load_dataset(
                self.state.current_file,
                self.state.filters or [],
                self.state.visible_columns or [],
            )
            target = self._save_dialog("Salvar relatorio Word", "Word (*.docx)")
            if not target:
                return
            self.export_service.export_docx(
                target,
                title="Relatorio Padronizado de AnAlise Fiscal",
                cnpj=self.state.current_cnpj or "",
                table_name=self.state.current_file.name,
                df=df,
                filters_text=self._filters_text(),
                visible_columns=self.state.visible_columns or [],
            )
            self.show_info("Relatorio gerado", f"Arquivo gerado em:\n{target}")
        except Exception as exc:
            self.show_error("Falha na exportacao para Word", str(exc))
    def export_txt_html(self) -> None:
        try:
            if self.state.current_file is None:
                raise ValueError("Nenhum arquivo selecionado.")
            df = self.parquet_service.load_dataset(
                self.state.current_file,
                self.state.filters or [],
                self.state.visible_columns or [],
            )
            html_report = self.export_service.build_html_report(
                title="Relatorio Padronizado de AnAlise Fiscal",
                cnpj=self.state.current_cnpj or "",
                table_name=self.state.current_file.name,
                df=df,
                filters_text=self._filters_text(),
                visible_columns=self.state.visible_columns or [],
            )
            target = self._save_dialog("Salvar TXT com HTML", "TXT (*.txt)")
            if not target:
                return
            self.export_service.export_txt_with_html(target, html_report)
            self.show_info("Relatorio HTML/TXT gerado", f"Arquivo gerado em:\n{target}")
        except Exception as exc:
            self.show_error("Falha na exportacao TXT/HTML", str(exc))
