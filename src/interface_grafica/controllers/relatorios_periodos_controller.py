from __future__ import annotations

from pathlib import Path

import polars as pl
from PySide6.QtWidgets import QFileDialog, QMessageBox

from interface_grafica.config import CNPJ_ROOT
from interface_grafica.controllers.workers import ServiceTaskWorker


class RelatoriosPeriodosControllerMixin:
    def atualizar_aba_periodos(self) -> None:
        cnpj = self.state.current_cnpj
        if not cnpj:
            self._atualizar_titulo_aba_periodos()
            return

        caminho = (
            CNPJ_ROOT / cnpj / "analises" / "produtos" / f"aba_periodos_{cnpj}.parquet"
        )
        if not caminho.exists():
            self.aba_periodos_model.set_dataframe(pl.DataFrame())
            self._aba_periodos_df = pl.DataFrame()
            self._aba_periodos_file_path = None
            self.lbl_aba_periodos_status.setText(
                "⏳ Arquivo ausente. Iniciando geração automática..."
            )
            self._atualizar_titulo_aba_periodos()
            self._reprocessar_periodos_auto(cnpj)
            return

        def _finalizar_carga_periodos(
            df: pl.DataFrame | None, uniques: dict | None = None
        ) -> None:
            if df is None:
                self.aba_periodos_model.set_dataframe(pl.DataFrame())
                self._aba_periodos_df = pl.DataFrame()
                self._aba_periodos_file_path = None
                self.lbl_aba_periodos_status.setText("Tabela periodos nao encontrada.")
                self._atualizar_titulo_aba_periodos()
                return
            self._aba_periodos_df = df
            self._aba_periodos_file_path = caminho
            self._reset_table_resize_flag("aba_periodos")

            id_atual = self.periodo_filter_id.currentText()
            if uniques and "id_agrupado" in uniques:
                self._popular_combo_texto(
                    self.periodo_filter_id,
                    [str(i) for i in uniques["id_agrupado"]],
                    id_atual,
                    "",
                )

            self.aplicar_filtros_aba_periodos()
            self.atualizar_aba_produtos_selecionados()
            self._atualizar_titulo_aba_periodos()

        self.lbl_aba_periodos_status.setText(
            "⏳ Carregando períodos em segundo plano..."
        )
        self._carregar_dados_parquet_async(
            caminho,
            _finalizar_carga_periodos,
            "Carregando Períodos",
            unique_cols=["id_agrupado"],
        )
    def _reprocessar_periodos_auto(self, cnpj: str) -> None:
        """Gera a aba_periodos automaticamente se estiver faltando."""

        def task():
            try:
                from transformacao.calculos_periodo_pkg import gerar_calculos_periodos

                return bool(gerar_calculos_periodos(cnpj))
            except Exception as e:
                print(f"Erro ao gerar períodos: {e}")
                return False

        def on_finished(success):
            if success:
                self.atualizar_aba_periodos()
            else:
                self.lbl_aba_periodos_status.setText(
                    "❌ Erro ao gerar períodos automaticamente."
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
    def aplicar_filtros_aba_periodos(self) -> None:
        if self._aba_periodos_df.is_empty():
            return

        filtros = []
        df_filtrado = self._aba_periodos_df

        id_val = self.periodo_filter_id.currentText().strip()
        if id_val:
            filtros.append(f"ID={id_val}")
            df_filtrado = df_filtrado.filter(
                pl.col("id_agrupado").cast(pl.Utf8).str.contains(id_val)
            )

        desc_val = self.periodo_filter_desc.text().strip().lower()
        if desc_val:
            filtros.append(f"Desc~={desc_val}")
            col_desc = (
                "descr_padrao"
                if "descr_padrao" in df_filtrado.columns
                else ("descricao" if "descricao" in df_filtrado.columns else None)
            )
            if col_desc:
                df_filtrado = df_filtrado.filter(
                    pl.col(col_desc)
                    .cast(pl.Utf8, strict=False)
                    .fill_null("")
                    .str.to_lowercase()
                    .str.contains(desc_val, literal=True)
                )

        texto_val = self.periodo_filter_texto.text().strip().lower()
        if texto_val:
            filtros.append(f"Busca='{texto_val}'")
            df_filtrado = self._filtrar_texto_em_colunas(df_filtrado, texto_val)

        num_col = self.periodo_filter_num_col.currentText().strip()
        v_min = self.periodo_filter_num_min.text().strip()
        v_max = self.periodo_filter_num_max.text().strip()
        if v_min or v_max:
            df_filtrado = self._filtrar_intervalo_numerico(
                df_filtrado, num_col, v_min, v_max
            )
            filtros.append(f"{num_col} entre [{v_min or '-inf'}, {v_max or '+inf'}]")

        self.aba_periodos_model.set_dataframe(df_filtrado)
        self.lbl_aba_periodos_filtros.setText(
            f"Filtros ativos: {', '.join(filtros) if filtros else 'nenhum'}"
        )
        self.lbl_aba_periodos_status.setText(
            f"Exibindo {df_filtrado.height:,} de {self._aba_periodos_df.height:,} registros."
        )
    def limpar_filtros_aba_periodos(self) -> None:
        self.periodo_filter_id.setCurrentText("")
        self.periodo_filter_desc.clear()
        self.periodo_filter_texto.clear()
        self.periodo_filter_num_min.clear()
        self.periodo_filter_num_max.clear()

        self.aba_periodos_model.set_dataframe(self._aba_periodos_df)
        self.lbl_aba_periodos_filtros.setText("Filtros ativos: nenhum")
        self.lbl_aba_periodos_status.setText(
            f"Sucesso! {self._aba_periodos_df.height} registros carregados."
        )
    def exportar_aba_periodos_excel(self) -> None:
        if self.aba_periodos_model.df_filtered.is_empty():
            QMessageBox.warning(self, "Aviso", "Não há dados filtrados para exportar.")
            return

        path, _ = QFileDialog.getSaveFileName(
            self,
            "Salvar Excel",
            f"aba_periodos_{self.cnpj_selecionado}.xlsx",
            "Excel Files (*.xlsx)",
        )
        if not path:
            return

        from interface_grafica.services.export_service import ExportService

        service = ExportService()
        try:
            service.export_excel(
                Path(path), self.aba_periodos_model.df_filtered, "Aba Periodos"
            )
            QMessageBox.information(
                self, "Sucesso", f"Arquivo exportado com sucesso: {path}"
            )
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Erro ao exportar: {e}")
    def atualizar_aba_anual(self) -> None:
        cnpj = self.state.current_cnpj
        if not cnpj:
            self._atualizar_titulo_aba_anual()
            return

        if not self._garantir_resumos_estoque_atualizados(cnpj):
            self._limpar_aba_resumo_estoque(
                "aba_anual", "Sincronizando tabela anual com a mov_estoque atual..."
            )
            return

        path = CNPJ_ROOT / cnpj / "analises" / "produtos" / f"aba_anual_{cnpj}.parquet"
        if not path.exists():
            self.aba_anual_model.set_dataframe(pl.DataFrame())
            self._aba_anual_df = pl.DataFrame()
            self.lbl_aba_anual_status.setText("Tabela Anual nao encontrada.")
            self._atualizar_titulo_aba_anual()
            self.atualizar_aba_produtos_selecionados()
            return

        def _finalizar_carga_anual(
            df: pl.DataFrame | None, uniques: dict | None = None
        ) -> None:
            if df is None:
                self.aba_anual_model.set_dataframe(pl.DataFrame())
                self._aba_anual_df = pl.DataFrame()
                self.lbl_aba_anual_status.setText("Tabela anual nao encontrada.")
                self._atualizar_titulo_aba_anual()
                return
            self._aba_anual_df = df
            self._aba_anual_file_path = path
            self._reset_table_resize_flag("aba_anual")

            id_atual = self.anual_filter_id.currentText()
            if uniques and "id_agrupado" in uniques:
                self._popular_combo_texto(
                    self.anual_filter_id,
                    [str(i) for i in uniques["id_agrupado"]],
                    id_atual,
                    "",
                )

            self.aplicar_filtros_aba_anual()
            self.atualizar_aba_produtos_selecionados()
            self.atualizar_aba_resumo_global()
            self._atualizar_titulo_aba_anual()

        self.lbl_aba_anual_status.setText(
            "⏳ Carregando tabela anual em segundo plano..."
        )
        self._carregar_dados_parquet_async(
            path,
            _finalizar_carga_anual,
            "Carregando Anual",
            unique_cols=["id_agrupado"],
        )
    def atualizar_aba_mensal(self) -> None:
        cnpj = self.state.current_cnpj
        if not cnpj:
            self._atualizar_titulo_aba_mensal()
            return

        if not self._garantir_resumos_estoque_atualizados(cnpj):
            self._limpar_aba_resumo_estoque(
                "aba_mensal", "Sincronizando tabela mensal com a mov_estoque atual..."
            )
            return

        path = CNPJ_ROOT / cnpj / "analises" / "produtos" / f"aba_mensal_{cnpj}.parquet"
        if not path.exists():
            self.aba_mensal_model.set_dataframe(pl.DataFrame())
            self._aba_mensal_df = pl.DataFrame()
            self.lbl_aba_mensal_status.setText("Tabela Mensal nao encontrada.")
            self._atualizar_titulo_aba_mensal()
            self.atualizar_aba_produtos_selecionados()
            return

        def _finalizar_carga_mensal(
            df: pl.DataFrame | None, uniques: dict | None = None
        ) -> None:
            if df is None:
                self.aba_mensal_model.set_dataframe(pl.DataFrame())
                self._aba_mensal_df = pl.DataFrame()
                self.lbl_aba_mensal_status.setText("Tabela mensal nao encontrada.")
                self._atualizar_titulo_aba_mensal()
                return
            self._aba_mensal_df = df
            self._aba_mensal_file_path = path
            self._reset_table_resize_flag("aba_mensal")

            id_atual = self.mensal_filter_id.currentText()
            if uniques and "id_agrupado" in uniques:
                self._popular_combo_texto(
                    self.mensal_filter_id,
                    [str(i) for i in uniques["id_agrupado"]],
                    id_atual,
                    "",
                )

            ano_atual = self.mensal_filter_ano.currentText()
            if uniques and "ano" in uniques:
                self._popular_combo_texto(
                    self.mensal_filter_ano,
                    [str(a) for a in uniques["ano"]],
                    ano_atual,
                    "Todos",
                )

            self.aplicar_filtros_aba_mensal()
            self.atualizar_aba_resumo_global()
            self._atualizar_titulo_aba_mensal()

        self.lbl_aba_mensal_status.setText(
            "⏳ Carregando tabela mensal em segundo plano..."
        )
        self._carregar_dados_parquet_async(
            path,
            _finalizar_carga_mensal,
            "Carregando Mensal",
            unique_cols=["id_agrupado", "ano"],
        )
    def aplicar_filtros_aba_mensal(self) -> None:
        if self._aba_mensal_df.is_empty():
            return
        try:
            id_agreg = self.mensal_filter_id.currentText().strip()
            desc = self.mensal_filter_desc.text().strip().lower()
            ano = self.mensal_filter_ano.currentText()
            mes = self.mensal_filter_mes.currentText()
            texto = self.mensal_filter_texto.text().strip().lower()
            num_col = self.mensal_filter_num_col.currentText().strip()
            num_min = self.mensal_filter_num_min.text().strip()
            num_max = self.mensal_filter_num_max.text().strip()

            df_filtrado = self._aba_mensal_df
            if id_agreg:
                df_filtrado = df_filtrado.filter(
                    pl.col("id_agregado").cast(pl.Utf8).str.contains(id_agreg)
                )
            if desc:
                df_filtrado = df_filtrado.filter(
                    pl.col("descr_padrao")
                    .cast(pl.Utf8, strict=False)
                    .fill_null("")
                    .str.to_lowercase()
                    .str.contains(desc, literal=True)
                )
            if ano != "Todos":
                df_filtrado = df_filtrado.filter(pl.col("ano").cast(pl.Utf8) == ano)
            if mes != "Todos":
                df_filtrado = df_filtrado.filter(pl.col("mes").cast(pl.Utf8) == mes)

            df_filtrado = self._filtrar_intervalo_numerico(
                df_filtrado, num_col, num_min, num_max
            )
            if texto:
                df_filtrado = self._filtrar_texto_em_colunas(df_filtrado, texto)

            self.aba_mensal_model.set_dataframe(df_filtrado)
            self._resize_table_once(self.aba_mensal_table, "aba_mensal")
            if not self._aplicar_preferencias_tabela(
                "aba_mensal", self.aba_mensal_table, self.aba_mensal_model
            ):
                self._aplicar_ordenacao_padrao(
                    self.aba_mensal_table,
                    self.aba_mensal_model,
                    ["ano", "mes", "id_agregado", "descr_padrao"],
                )
                self._aplicar_preset_aba_mensal()
            self.lbl_aba_mensal_status.setText(
                f"Exibindo {df_filtrado.height:,} de {self._aba_mensal_df.height:,} linhas."
            )
            self.lbl_aba_mensal_filtros.setText(
                self._formatar_resumo_filtros(
                    [
                        ("id_agregado", id_agreg),
                        ("descricao", desc),
                        ("ano", "" if ano == "Todos" else ano),
                        ("mes", "" if mes == "Todos" else mes),
                        (
                            num_col,
                            (
                                f"{num_min or ''}..{num_max or ''}"
                                if num_min or num_max
                                else ""
                            ),
                        ),
                        ("texto", texto),
                    ]
                )
            )
            self._atualizar_titulo_aba_mensal(
                df_filtrado.height, self._aba_mensal_df.height
            )
            self._salvar_preferencias_tabela(
                "aba_mensal", self.aba_mensal_table, self.aba_mensal_model
            )
        except Exception as e:
            QMessageBox.warning(self, "Erro", f"Erro ao filtrar aba mensal: {e}")
    def limpar_filtros_aba_mensal(self) -> None:
        self.mensal_filter_id.setCurrentIndex(0)
        self.mensal_filter_desc.clear()
        self.mensal_filter_ano.setCurrentIndex(0)
        self.mensal_filter_mes.setCurrentIndex(0)
        self.mensal_filter_texto.clear()
        self.mensal_filter_num_min.clear()
        self.mensal_filter_num_max.clear()
        self.aplicar_filtros_aba_mensal()
    def exportar_aba_mensal_excel_metodo(self) -> None:
        df = self._dataframe_colunas_perfil(
            "aba_mensal",
            "aba_mensal",
            self.aba_mensal_model,
            self.aba_mensal_model.dataframe,
            perfil="Exportar",
        )
        if df.is_empty():
            return
        target = self._save_dialog("Exportar Mensal", "Excel (*.xlsx)")
        if not target:
            return
        try:
            self.export_service.export_excel(target, df, sheet_name="Mensal")
        except Exception as e:
            self.show_error("Erro de exportacao", str(e))
    def exportar_aba_mensal_excel(self) -> None:
        self.exportar_aba_mensal_excel_metodo()
    def aplicar_filtros_aba_anual(self) -> None:
        if self._aba_anual_df.is_empty():
            return
        try:
            id_agreg = self.anual_filter_id.currentText().strip()
            desc = self.anual_filter_desc.text().strip().lower()
            ano = self.anual_filter_ano.currentText()
            texto = self.anual_filter_texto.text().strip().lower()
            num_col = self.anual_filter_num_col.currentText().strip()
            num_min = self.anual_filter_num_min.text().strip()
            num_max = self.anual_filter_num_max.text().strip()

            df_filtrado = self._aba_anual_df

            if self._filtro_cruzado_anuais_ids:
                df_filtrado = df_filtrado.filter(
                    pl.col("id_agregado").is_in(self._filtro_cruzado_anuais_ids)
                )

            if id_agreg:
                df_filtrado = df_filtrado.filter(
                    pl.col("id_agregado").cast(pl.Utf8).str.contains(id_agreg)
                )
            if desc:
                col_desc = (
                    "descr_padrao"
                    if "descr_padrao" in df_filtrado.columns
                    else ("descriCAo" if "descriCAo" in df_filtrado.columns else None)
                )
                if col_desc is not None:
                    df_filtrado = df_filtrado.filter(
                        pl.col(col_desc)
                        .cast(pl.Utf8, strict=False)
                        .fill_null("")
                        .str.to_lowercase()
                        .str.contains(desc, literal=True)
                    )
            if ano != "Todos":
                df_filtrado = df_filtrado.filter(pl.col("ano").cast(pl.Utf8) == ano)

            df_filtrado = self._filtrar_intervalo_numerico(
                df_filtrado, num_col, num_min, num_max
            )

            if texto:
                df_filtrado = self._filtrar_texto_em_colunas(df_filtrado, texto)

            self.aba_anual_model.set_dataframe(df_filtrado)
            self._resize_table_once(self.aba_anual_table, "aba_anual")
            if not self._aplicar_preferencias_tabela(
                "aba_anual", self.aba_anual_table, self.aba_anual_model
            ):
                self._aplicar_ordenacao_padrao(
                    self.aba_anual_table,
                    self.aba_anual_model,
                    ["ano", "id_agregado", "descr_padrao"],
                )
                self._aplicar_preset_aba_anual()
            self.lbl_aba_anual_status.setText(
                f"Exibindo {df_filtrado.height:,} de {self._aba_anual_df.height:,} linhas."
                + (" (FILTRO CRUZADO ATIVO)" if self._filtro_cruzado_anuais_ids else "")
            )
            self.lbl_aba_anual_filtros.setText(
                self._formatar_resumo_filtros(
                    [
                        ("id_agregado", id_agreg),
                        ("descricao", desc),
                        ("ano", "" if ano == "Todos" else ano),
                        (
                            num_col,
                            (
                                f"{num_min or ''}..{num_max or ''}"
                                if num_min or num_max
                                else ""
                            ),
                        ),
                        ("texto", texto),
                        (
                            "cruzado",
                            (
                                ",".join(self._filtro_cruzado_anuais_ids[:3])
                                + (
                                    "..."
                                    if len(self._filtro_cruzado_anuais_ids) > 3
                                    else ""
                                )
                                if self._filtro_cruzado_anuais_ids
                                else ""
                            ),
                        ),
                    ]
                )
            )
            self._atualizar_titulo_aba_anual(
                df_filtrado.height, self._aba_anual_df.height
            )
            self._salvar_preferencias_tabela(
                "aba_anual", self.aba_anual_table, self.aba_anual_model
            )
        except Exception as e:
            QMessageBox.warning(self, "Erro", f"Erro ao filtrar aba anual: {e}")
    def limpar_filtros_aba_anual(self) -> None:
        self.anual_filter_id.setCurrentIndex(0)
        self.anual_filter_desc.clear()
        self.anual_filter_ano.setCurrentIndex(0)
        self.anual_filter_texto.clear()
        self.anual_filter_num_min.clear()
        self.anual_filter_num_max.clear()
        self.aplicar_filtros_aba_anual()
    def filtrar_estoque_pela_selecao_anual(self) -> None:
        checked_ids = self.aba_anual_model.get_checked_rows()
        if not checked_ids:
            QMessageBox.information(self, "Aviso", "Nenhum produto selecionado.")
            return
        ids_unicos = list(
            set(
                [
                    str(r.get("id_agregado", ""))
                    for r in checked_ids
                    if r.get("id_agregado")
                ]
            )
        )
        self._filtro_cruzado_anuais_ids = ids_unicos
        self.aplicar_filtros_aba_anual()
        self.aplicar_filtros_mov_estoque()
    def limpar_filtro_cruzado_anual(self) -> None:
        self._filtro_cruzado_anuais_ids = []
        self.aba_anual_model.clear_checked()
        self.aplicar_filtros_aba_anual()
        self.aplicar_filtros_mov_estoque()
    def exportar_aba_anual_excel_metodo(self) -> None:
        df = self._dataframe_colunas_perfil(
            "aba_anual",
            "aba_anual",
            self.aba_anual_model,
            self.aba_anual_model.dataframe,
            perfil="Exportar",
        )
        if df.is_empty():
            return
        target = self._save_dialog("Exportar Anual", "Excel (*.xlsx)")
        if not target:
            return
        try:
            self.export_service.export_excel(target, df, sheet_name="Anual")
        except Exception as e:
            self.show_error("Erro de exportacao", str(e))
    def exportar_aba_anual_excel(self) -> None:
        self.exportar_aba_anual_excel_metodo()
