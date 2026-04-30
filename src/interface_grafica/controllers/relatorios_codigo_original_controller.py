from __future__ import annotations

from pathlib import Path

import structlog

log = structlog.get_logger(__name__)

import polars as pl
from PySide6.QtWidgets import QFileDialog, QMessageBox

from interface_grafica.config import CNPJ_ROOT
from interface_grafica.controllers.workers import ServiceTaskWorker
from interface_grafica.utils.safe_slot import safe_slot


class RelatoriosCodigoOriginalControllerMixin:
    @safe_slot
    def atualizar_aba_codigo_original(self) -> None:
        cnpj = self.state.current_cnpj
        if not cnpj:
            self._atualizar_titulo_aba_codigo_original()
            return

        caminho = CNPJ_ROOT / cnpj / "analises" / "produtos" / f"aba_codigo_original_{cnpj}.parquet"
        if not caminho.exists():
            self.aba_codigo_original_model.set_dataframe(pl.DataFrame())
            self._aba_codigo_original_df = pl.DataFrame()
            self._aba_codigo_original_file_path = None
            self.lbl_aba_codigo_original_status.setText(
                "Arquivo ausente. Iniciando geracao automatica..."
            )
            self._atualizar_titulo_aba_codigo_original()
            self._reprocessar_codigo_original_auto(cnpj)
            return

        def _finalizar_carga(df: pl.DataFrame | None, uniques: dict | None = None) -> None:
            if df is None:
                self.aba_codigo_original_model.set_dataframe(pl.DataFrame())
                self._aba_codigo_original_df = pl.DataFrame()
                self._aba_codigo_original_file_path = None
                self.lbl_aba_codigo_original_status.setText(
                    "Tabela codigo_original nao encontrada."
                )
                self._atualizar_titulo_aba_codigo_original()
                return
            self._aba_codigo_original_df = df
            self._aba_codigo_original_file_path = caminho
            self._reset_table_resize_flag("aba_codigo_original")

            cod_atual = self.cod_original_filter_cod.currentText()
            if uniques and "Cod_item" in uniques:
                self._popular_combo_texto(
                    self.cod_original_filter_cod,
                    [str(c) for c in uniques["Cod_item"]],
                    cod_atual,
                    "",
                )

            ano_atual = self.cod_original_filter_ano.currentText()
            if uniques and "ano" in uniques:
                self._popular_combo_texto(
                    self.cod_original_filter_ano,
                    [str(a) for a in uniques["ano"]],
                    ano_atual,
                    "Todos",
                )

            self.aplicar_filtros_aba_codigo_original()
            self._atualizar_titulo_aba_codigo_original()

        self.lbl_aba_codigo_original_status.setText(
            "Carregando codigo original em segundo plano..."
        )
        self._carregar_dados_parquet_async(
            caminho,
            _finalizar_carga,
            "Carregando Codigo Original",
            unique_cols=["Cod_item", "ano"],
        )

    def _reprocessar_codigo_original_auto(self, cnpj: str) -> None:
        def task():
            try:
                from transformacao.calculos_codigo_original_pkg import (
                    gerar_calculos_codigo_original,
                )

                return bool(gerar_calculos_codigo_original(cnpj))
            except Exception as e:
                log.error("relatorios_codigo_original.gerar.falhou", error=str(e))
                return False

        def on_finished(success: bool) -> None:
            if success:
                self.atualizar_aba_codigo_original()
            else:
                self.lbl_aba_codigo_original_status.setText(
                    "Erro ao gerar codigo_original automaticamente."
                )

        worker = ServiceTaskWorker(task)
        worker.finished_ok.connect(on_finished)

        if not hasattr(self, "_active_load_workers") or not isinstance(
            self._active_load_workers, set
        ):
            self._active_load_workers = set()
        self._active_load_workers.add(worker)
        worker.finished.connect(lambda: self._active_load_workers.discard(worker))
        worker.finished.connect(worker.deleteLater)
        worker.start()

    def aplicar_filtros_aba_codigo_original(self) -> None:
        if self._aba_codigo_original_df.is_empty():
            return
        try:
            cod_val = self.cod_original_filter_cod.currentText().strip()
            desc_val = self.cod_original_filter_desc.text().strip().lower()
            ano_val = self.cod_original_filter_ano.currentText()
            mes_val = self.cod_original_filter_mes.currentText()
            texto_val = self.cod_original_filter_texto.text().strip().lower()
            num_col = self.cod_original_filter_num_col.currentText().strip()
            num_min = self.cod_original_filter_num_min.text().strip()
            num_max = self.cod_original_filter_num_max.text().strip()

            df = self._aba_codigo_original_df

            if cod_val:
                df = df.filter(pl.col("Cod_item").cast(pl.Utf8).str.contains(cod_val, literal=True))
            if desc_val:
                col_desc = "Descr_item" if "Descr_item" in df.columns else None
                if col_desc:
                    df = df.filter(
                        pl.col(col_desc)
                        .cast(pl.Utf8, strict=False)
                        .fill_null("")
                        .str.to_lowercase()
                        .str.contains(desc_val, literal=True)
                    )
            if ano_val != "Todos":
                df = df.filter(pl.col("ano").cast(pl.Utf8) == ano_val)
            if mes_val != "Todos":
                df = df.filter(pl.col("mes").cast(pl.Utf8) == mes_val)

            df = self._filtrar_intervalo_numerico(df, num_col, num_min, num_max)
            if texto_val:
                df = self._filtrar_texto_em_colunas(df, texto_val)

            self.aba_codigo_original_model.set_dataframe(df)
            self._resize_table_once(self.aba_codigo_original_table, "aba_codigo_original")
            if not self._aplicar_preferencias_tabela(
                "aba_codigo_original",
                self.aba_codigo_original_table,
                self.aba_codigo_original_model,
            ):
                self._aplicar_ordenacao_padrao(
                    self.aba_codigo_original_table,
                    self.aba_codigo_original_model,
                    ["ano", "mes", "Cod_item", "fonte"],
                )
            self.lbl_aba_codigo_original_status.setText(
                f"Exibindo {df.height:,} de {self._aba_codigo_original_df.height:,} linhas."
            )
            self.lbl_aba_codigo_original_filtros.setText(
                self._formatar_resumo_filtros(
                    [
                        ("Cod_item", cod_val),
                        ("descricao", desc_val),
                        ("ano", "" if ano_val == "Todos" else ano_val),
                        ("mes", "" if mes_val == "Todos" else mes_val),
                        (
                            num_col,
                            f"{num_min or ''}..{num_max or ''}" if num_min or num_max else "",
                        ),
                        ("texto", texto_val),
                    ]
                )
            )
            self._atualizar_titulo_aba_codigo_original(
                df.height, self._aba_codigo_original_df.height
            )
            self._salvar_preferencias_tabela(
                "aba_codigo_original",
                self.aba_codigo_original_table,
                self.aba_codigo_original_model,
            )
        except Exception as e:
            QMessageBox.warning(self, "Erro", f"Erro ao filtrar aba codigo_original: {e}")

    def limpar_filtros_aba_codigo_original(self) -> None:
        self.cod_original_filter_cod.setCurrentIndex(0)
        self.cod_original_filter_desc.clear()
        self.cod_original_filter_ano.setCurrentIndex(0)
        self.cod_original_filter_mes.setCurrentIndex(0)
        self.cod_original_filter_texto.clear()
        self.cod_original_filter_num_min.clear()
        self.cod_original_filter_num_max.clear()
        self.aplicar_filtros_aba_codigo_original()

    def exportar_aba_codigo_original_excel(self) -> None:
        df = self._dataframe_colunas_perfil(
            "aba_codigo_original",
            "aba_codigo_original",
            self.aba_codigo_original_model,
            self.aba_codigo_original_model.dataframe,
            perfil="Exportar",
        )
        if df.is_empty():
            return
        target = self._save_dialog("Exportar Codigo Original", "Excel (*.xlsx)")
        if not target:
            return
        try:
            self.export_service.export_excel(target, df, sheet_name="Cod Original")
        except Exception as e:
            self.show_error("Erro de exportacao", str(e))

    def _atualizar_titulo_aba_codigo_original(
        self, exibindo: int | None = None, total: int | None = None
    ) -> None:
        if not hasattr(self, "lbl_aba_codigo_original_titulo"):
            return
        cnpj = self.state.current_cnpj or "-"
        if exibindo is not None and total is not None:
            txt = f"Tabela: aba_codigo_original | CNPJ: {cnpj} | {exibindo:,}/{total:,} linhas"
        else:
            txt = f"Tabela: aba_codigo_original | CNPJ: {cnpj}"
        self.lbl_aba_codigo_original_titulo.setText(txt)
