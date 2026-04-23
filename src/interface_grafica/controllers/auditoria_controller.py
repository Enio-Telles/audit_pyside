from __future__ import annotations

import polars as pl

from PySide6.QtCore import QDate, Qt
from PySide6.QtWidgets import QMessageBox

from interface_grafica.config import CNPJ_ROOT


class AuditoriaControllerMixin:
    def atualizar_aba_mov_estoque(self) -> None:
        cnpj = self.state.current_cnpj
        if not cnpj:
            self._atualizar_titulo_aba_mov_estoque()
            return

        path = (
            CNPJ_ROOT / cnpj / "analises" / "produtos" / f"mov_estoque_{cnpj}.parquet"
        )
        if not path.exists():
            self.mov_estoque_model.set_dataframe(pl.DataFrame())
            self._mov_estoque_df = pl.DataFrame()
            self.lbl_mov_estoque_status.setText(
                "Arquivo 'mov_estoque' nao encontrado para este CNPJ."
            )
            self._atualizar_titulo_aba_mov_estoque()
            self.atualizar_aba_produtos_selecionados()
            return

        def _finalizar_carga_estoque(
            df: pl.DataFrame | None, uniques: dict | None = None
        ) -> None:
            if df is None:
                self.mov_estoque_model.set_dataframe(pl.DataFrame())
                self._mov_estoque_df = pl.DataFrame()
                self._mov_estoque_file_path = None
                self.lbl_mov_estoque_status.setText(
                    "Arquivo 'mov_estoque' nao encontrado para este CNPJ."
                )
                self._atualizar_titulo_aba_mov_estoque()
                return
            self._mov_estoque_df = df
            self._mov_estoque_file_path = path
            self._reset_table_resize_flag("mov_estoque")

            # Popular combo de id_agrupado com dados pre-calculados no background
            id_atual = self.mov_filter_id.currentText()
            if uniques and "id_agrupado" in uniques:
                self._popular_combo_texto(
                    self.mov_filter_id,
                    [str(i) for i in uniques["id_agrupado"]],
                    id_atual,
                    "",
                )

            self.aplicar_filtros_mov_estoque()
            self.atualizar_aba_produtos_selecionados()
            self._atualizar_titulo_aba_mov_estoque()

        self.lbl_mov_estoque_status.setText(
            "⏳ Carregando dados de estoque em segundo plano..."
        )
        self._carregar_dados_parquet_async(
            path,
            _finalizar_carga_estoque,
            "Carregando Estoque",
            unique_cols=["id_agrupado"],
        )
    def aplicar_filtros_mov_estoque(self) -> None:
        if self._mov_estoque_df.is_empty():
            return
        try:
            id_agrup = self.mov_filter_id.currentText().strip()
            desc = self.mov_filter_desc.text().strip().lower()
            ncm = self.mov_filter_ncm.text().strip()
            tipo = self.mov_filter_tipo.currentText()
            texto = self.mov_filter_texto.text().strip().lower()
            data_col = self.mov_filter_data_col.currentText().strip()
            data_ini = self._valor_qdate_ativo(self.mov_filter_data_ini.date())
            data_fim = self._valor_qdate_ativo(self.mov_filter_data_fim.date())
            num_col = self.mov_filter_num_col.currentText().strip()
            num_min = self.mov_filter_num_min.text().strip()
            num_max = self.mov_filter_num_max.text().strip()

            df_filtrado = self._mov_estoque_df

            # Filtro Cruzado
            if self._filtro_cruzado_anuais_ids:
                df_filtrado = df_filtrado.filter(
                    pl.col("id_agrupado").is_in(self._filtro_cruzado_anuais_ids)
                )

            if id_agrup:
                df_filtrado = df_filtrado.filter(
                    pl.col("id_agrupado").cast(pl.Utf8).str.contains(id_agrup)
                )
            if desc:
                col_desc = (
                    "descr_padrao"
                    if "descr_padrao" in df_filtrado.columns
                    else "Descr_item"
                )
                df_filtrado = df_filtrado.filter(
                    pl.col(col_desc)
                    .cast(pl.Utf8, strict=False)
                    .fill_null("")
                    .str.to_lowercase()
                    .str.contains(desc, literal=True)
                )
            if ncm:
                col_ncm = "ncm_padrao" if "ncm_padrao" in df_filtrado.columns else "Ncm"
                df_filtrado = df_filtrado.filter(
                    pl.col(col_ncm)
                    .cast(pl.Utf8, strict=False)
                    .fill_null("")
                    .str.contains(ncm, literal=True)
                )
            if tipo == "Entradas":
                df_filtrado = df_filtrado.filter(
                    pl.col("Tipo_operacao")
                    .cast(pl.Utf8, strict=False)
                    .str.contains("ENTRADA", literal=True)
                )
            elif tipo == "Saidas":
                df_filtrado = df_filtrado.filter(
                    pl.col("Tipo_operacao")
                    .cast(pl.Utf8, strict=False)
                    .str.contains("SAIDA", literal=True)
                )

            df_filtrado = self._filtrar_intervalo_data(
                df_filtrado, data_col, data_ini, data_fim
            )
            df_filtrado = self._filtrar_intervalo_numerico(
                df_filtrado, num_col, num_min, num_max
            )

            if texto:
                df_filtrado = self._filtrar_texto_em_colunas(df_filtrado, texto)

            self.mov_estoque_model.set_dataframe(df_filtrado)
            self._resize_table_once(self.mov_estoque_table, "mov_estoque")
            if not self._aplicar_preferencias_tabela(
                "mov_estoque", self.mov_estoque_table, self.mov_estoque_model
            ):
                self._aplicar_ordenacao_padrao(
                    self.mov_estoque_table,
                    self.mov_estoque_model,
                    ["ordem_operacoes", "Dt_doc", "Dt_e_s", "id_agrupado"],
                )
                self._aplicar_preset_mov_estoque()
            if "ordem_operacoes" in self.mov_estoque_model.dataframe.columns:
                offset = (
                    1 if getattr(self.mov_estoque_model, "_checkable", False) else 0
                )
                idx_ordem = (
                    self.mov_estoque_model.dataframe.columns.index("ordem_operacoes")
                    + offset
                )
                self.mov_estoque_table.setColumnHidden(idx_ordem, False)
            self.lbl_mov_estoque_status.setText(
                f"Movimentacoes: {df_filtrado.height:,} de {self._mov_estoque_df.height:,} linhas."
                + (" (FILTRO CRUZADO ATIVO)" if self._filtro_cruzado_anuais_ids else "")
            )
            self.lbl_mov_estoque_filtros.setText(
                self._formatar_resumo_filtros(
                    [
                        ("id_agrupado", id_agrup),
                        ("descricao", desc),
                        ("ncm", ncm),
                        ("tipo", "" if tipo == "Todos" else tipo),
                        (
                            "data",
                            (
                                f"{data_ini.toString('dd/MM/yyyy') if data_ini else ''}..{data_fim.toString('dd/MM/yyyy') if data_fim else ''}"
                                if data_ini or data_fim
                                else ""
                            ),
                        ),
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
            self._atualizar_titulo_aba_mov_estoque(
                df_filtrado.height, self._mov_estoque_df.height
            )
            self._salvar_preferencias_tabela(
                "mov_estoque", self.mov_estoque_table, self.mov_estoque_model
            )
        except Exception as e:
            QMessageBox.warning(self, "Erro", f"Erro ao filtrar mov_estoque: {e}")
    def exportar_mov_estoque_excel(self) -> None:
        if self.mov_estoque_model.dataframe.is_empty():
            QMessageBox.information(
                self,
                "Exportacao",
                "Nao ha dados filtrados na mov_estoque para exportar.",
            )
            return
        target = self._save_dialog("Exportar Movimentacao de Estoque", "Excel (*.xlsx)")
        if not target:
            return
        try:
            df_to_export = self._dataframe_colunas_visiveis(
                self.mov_estoque_table,
                self.mov_estoque_model,
                self.mov_estoque_model.dataframe,
            )
            self.export_service.export_excel(
                target, df_to_export, sheet_name="Mov_Estoque"
            )
        except Exception as e:
            self.show_error("Erro de exportacao", str(e))
    def atualizar_aba_nfe_entrada(self) -> None:
        self._atualizar_estado_botao_nfe_entrada()
        cnpj = self.state.current_cnpj
        if not cnpj:
            self.nfe_entrada_model.set_dataframe(pl.DataFrame())
            self._nfe_entrada_df = pl.DataFrame()
            self._nfe_entrada_file_path = None
            self.lbl_nfe_entrada_status.setText(
                "Selecione um CPF/CNPJ para carregar as NFes/NFCes de entrada."
            )
            self._atualizar_titulo_aba_nfe_entrada()
            return

        path = (
            CNPJ_ROOT / cnpj / "analises" / "produtos" / f"nfe_entrada_{cnpj}.parquet"
        )
        if not path.exists():
            self.nfe_entrada_model.set_dataframe(pl.DataFrame())
            self._nfe_entrada_df = pl.DataFrame()
            self._nfe_entrada_file_path = None
            self._atualizar_titulo_aba_nfe_entrada()
            return

        def _finalizar_carga_nfe(
            df: pl.DataFrame | None, uniques: dict | None = None
        ) -> None:
            if df is None:
                self.nfe_entrada_model.set_dataframe(pl.DataFrame())
                self._nfe_entrada_df = pl.DataFrame()
                self._nfe_entrada_file_path = None
                self.lbl_nfe_entrada_status.setText(
                    "Tabela nfe_entrada nao encontrada para este CNPJ."
                )
                self._atualizar_titulo_aba_nfe_entrada()
                return
            self._nfe_entrada_df = df
            self._nfe_entrada_file_path = path
            self._reset_table_resize_flag("nfe_entrada")

            id_atual = self.nfe_entrada_filter_id.currentText()
            if uniques and "id_agrupado" in uniques:
                self._popular_combo_texto(
                    self.nfe_entrada_filter_id,
                    [str(i) for i in uniques["id_agrupado"]],
                    id_atual,
                    "",
                )

            self.aplicar_filtros_nfe_entrada()
            self._atualizar_titulo_aba_nfe_entrada()

        self.status.showMessage("⏳ Carregando NFe Entrada em segundo plano...")
        self._carregar_dados_parquet_async(
            path,
            _finalizar_carga_nfe,
            "Carregando NFe Entrada",
            unique_cols=["id_agrupado"],
        )
        self._atualizar_titulo_aba_nfe_entrada()
    def aplicar_filtros_nfe_entrada(self) -> None:
        if self._nfe_entrada_df.is_empty():
            return
        try:
            id_agrupado = self.nfe_entrada_filter_id.currentText().strip()
            desc = self.nfe_entrada_filter_desc.text().strip().lower()
            ncm = self.nfe_entrada_filter_ncm.text().strip()
            co_sefin = self.nfe_entrada_filter_sefin.text().strip()
            texto = self.nfe_entrada_filter_texto.text().strip().lower()
            data_ini = (
                None
                if self.nfe_entrada_filter_data_ini.date()
                == self.nfe_entrada_filter_data_ini.minimumDate()
                else self.nfe_entrada_filter_data_ini.date()
            )
            data_fim = (
                None
                if self.nfe_entrada_filter_data_fim.date()
                == self.nfe_entrada_filter_data_fim.minimumDate()
                else self.nfe_entrada_filter_data_fim.date()
            )

            df_filtrado = self._nfe_entrada_df
            if id_agrupado and "id_agrupado" in df_filtrado.columns:
                df_filtrado = df_filtrado.filter(
                    pl.col("id_agrupado")
                    .cast(pl.Utf8, strict=False)
                    .fill_null("")
                    .str.contains(id_agrupado, literal=True)
                )
            if desc:
                cols_desc = [
                    c
                    for c in ["descr_padrao", "prod_xprod"]
                    if c in df_filtrado.columns
                ]
                if cols_desc:
                    exprs = [
                        pl.col(col)
                        .cast(pl.Utf8, strict=False)
                        .fill_null("")
                        .str.to_lowercase()
                        .str.contains(desc, literal=True)
                        for col in cols_desc
                    ]
                    df_filtrado = df_filtrado.filter(pl.any_horizontal(exprs))
            if ncm and "prod_ncm" in df_filtrado.columns:
                df_filtrado = df_filtrado.filter(
                    pl.col("prod_ncm")
                    .cast(pl.Utf8, strict=False)
                    .fill_null("")
                    .str.contains(ncm, literal=True)
                )
            if co_sefin:
                cols_sefin = [
                    c
                    for c in ["co_sefin_agr", "co_sefin_inferido"]
                    if c in df_filtrado.columns
                ]
                if cols_sefin:
                    exprs = [
                        pl.col(col)
                        .cast(pl.Utf8, strict=False)
                        .fill_null("")
                        .str.contains(co_sefin, literal=True)
                        for col in cols_sefin
                    ]
                    df_filtrado = df_filtrado.filter(pl.any_horizontal(exprs))

            df_filtrado = self._filtrar_intervalo_data(
                df_filtrado, "data_classificacao", data_ini, data_fim
            )

            if texto:
                df_filtrado = self._filtrar_texto_em_colunas(df_filtrado, texto)

            self.nfe_entrada_model.set_dataframe(df_filtrado)
            self._resize_table_once(self.nfe_entrada_table, "nfe_entrada")
            if not self._aplicar_preferencias_tabela(
                "nfe_entrada", self.nfe_entrada_table, self.nfe_entrada_model
            ):
                self._aplicar_ordenacao_padrao(
                    self.nfe_entrada_table,
                    self.nfe_entrada_model,
                    ["data_classificacao", "nnf", "prod_nitem"],
                    Qt.DescendingOrder,
                )
                self._aplicar_preset_colunas(
                    self.nfe_entrada_table,
                    self.nfe_entrada_model.dataframe.columns,
                    self._obter_colunas_preset_perfil(
                        "auditoria",
                        self.nfe_entrada_model.dataframe.columns,
                        "nfe_entrada",
                    ),
                )
            self.lbl_nfe_entrada_status.setText(
                f"Exibindo {df_filtrado.height:,} de {self._nfe_entrada_df.height:,} itens de NFe/NFCe de entrada."
            )
            periodo = ""
            if data_ini is not None or data_fim is not None:
                periodo = f"{data_ini.toString('dd/MM/yyyy') if data_ini is not None else '...'} ate {data_fim.toString('dd/MM/yyyy') if data_fim is not None else '...'}"
            self.lbl_nfe_entrada_filtros.setText(
                self._formatar_resumo_filtros(
                    [
                        ("id_agrupado", id_agrupado),
                        ("descricao", desc),
                        ("ncm", ncm),
                        ("co_sefin", co_sefin),
                        ("periodo", periodo),
                        ("texto", texto),
                    ]
                )
            )
            self._salvar_preferencias_tabela(
                "nfe_entrada", self.nfe_entrada_table, self.nfe_entrada_model
            )
        except Exception as e:
            self.show_error("Erro", f"Erro ao filtrar NFe Entrada: {e}")
    def limpar_filtros_nfe_entrada(self) -> None:
        self.nfe_entrada_filter_id.setCurrentIndex(0)
        self.nfe_entrada_filter_desc.clear()
        self.nfe_entrada_filter_ncm.clear()
        self.nfe_entrada_filter_sefin.clear()
        self.nfe_entrada_filter_texto.clear()
        self.nfe_entrada_filter_data_ini.setDate(
            self.nfe_entrada_filter_data_ini.minimumDate()
        )
        self.nfe_entrada_filter_data_fim.setDate(
            self.nfe_entrada_filter_data_fim.minimumDate()
        )
        self.aplicar_filtros_nfe_entrada()
    def exportar_nfe_entrada_excel(self) -> None:
        df = self._dataframe_colunas_perfil(
            "nfe_entrada",
            "nfe_entrada",
            self.nfe_entrada_model,
            self.nfe_entrada_model.dataframe,
            perfil="Exportar",
        )
        if df.is_empty():
            self.show_info("Exportacao", "Nao ha dados de NFe Entrada para exportar.")
            return
        target = self._save_dialog("Exportar NFe Entrada", "Excel (*.xlsx)")
        if not target:
            return
        try:
            self.export_service.export_excel(target, df, sheet_name="NFe_Entrada")
        except Exception as e:
            self.show_error("Erro de exportacao", str(e))
