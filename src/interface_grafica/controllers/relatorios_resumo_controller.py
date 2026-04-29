from __future__ import annotations

from pathlib import Path

import polars as pl
from openpyxl import Workbook
from openpyxl.styles import Font as OpenPyxlFont
from PySide6.QtWidgets import QMessageBox

from interface_grafica.config import CNPJ_ROOT
from utilitarios.text import display_cell, is_year_column_name


class RelatoriosResumoControllerMixin:
    def _escrever_planilha_openpyxl(self, ws, df: pl.DataFrame) -> None:
        fonte_padrao = OpenPyxlFont(name="Arial", size=8)
        fonte_header = OpenPyxlFont(name="Arial", size=8, bold=True)
        formato_num = "#,##0.00"
        formato_inteiro = "#,##0"
        formato_ano = "0"
        formato_data = "dd/mm/yyyy"
        formato_data_hora = "dd/mm/yyyy hh:mm:ss"

        ws.append(list(df.columns))
        for cell in ws[1]:
            cell.font = fonte_header

        for row in df.iter_rows():
            linha_excel = []
            for coluna, valor in zip(df.columns, row, strict=False):
                if isinstance(valor, (int, float)) and not isinstance(valor, bool):
                    linha_excel.append(valor)
                else:
                    linha_excel.append(display_cell(valor, coluna))
            ws.append(linha_excel)

        for row in ws.iter_rows():
            for cell in row:
                if cell.row == 1:
                    cell.font = fonte_header
                else:
                    cell.font = fonte_padrao
                if cell.row == 1:
                    continue
                nome_coluna = str(ws.cell(row=1, column=cell.column).value or "")
                if (
                    is_year_column_name(nome_coluna)
                    and isinstance(cell.value, (int, float))
                    and not isinstance(cell.value, bool)
                ):
                    cell.number_format = formato_ano
                elif isinstance(cell.value, (int, float)) and not isinstance(
                    cell.value, bool
                ):
                    if float(cell.value).is_integer():
                        cell.number_format = formato_inteiro
                    else:
                        cell.number_format = formato_num
                elif hasattr(cell.value, "hour"):
                    cell.number_format = formato_data_hora
                elif hasattr(cell.value, "day") and hasattr(cell.value, "month"):
                    cell.number_format = formato_data

        ws.freeze_panes = "A2"
        if ws.max_row >= 1 and ws.max_column >= 1:
            ws.auto_filter.ref = ws.dimensions
    def _gerar_resumo_global(
        self,
        mensal: pl.DataFrame,
        anual: pl.DataFrame,
        anos_base: list[int] | None = None,
    ) -> pl.DataFrame:
        if anos_base is None:
            anos_base = []
            for df in (mensal, anual):
                if not df.is_empty() and "ano" in df.columns:
                    anos_base.extend(
                        df.get_column("ano")
                        .cast(pl.Int32, strict=False)
                        .drop_nulls()
                        .unique()
                        .sort()
                        .to_list()
                    )
            anos_base = sorted({int(ano) for ano in anos_base})

        competencias = [
            f"{ano:04d}-{mes:02d}" for ano in anos_base for mes in range(1, 13)
        ]
        if not competencias:
            return pl.DataFrame({"Ano/Mes": []}).with_columns(
                [
                    pl.lit(0.0).alias("ICMS_entr_desacob"),
                    pl.lit(0.0).alias("ICMS_saidas_desac"),
                    pl.lit(0.0).alias("ICMS_estoque_desac"),
                    pl.lit(0.0).alias("Total"),
                ]
            )

        base_competencias = pl.DataFrame({"Ano/Mes": competencias})

        if not mensal.is_empty():
            mensal_base = (
                mensal.select(
                    [
                        pl.concat_str(
                            [
                                pl.col("ano").cast(pl.Utf8, strict=False),
                                pl.lit("-"),
                                pl.col("mes").cast(pl.Utf8, strict=False).str.zfill(2),
                            ]
                        ).alias("Ano/Mes"),
                        pl.col("ICMS_entr_desacob")
                        .cast(pl.Float64, strict=False)
                        .fill_null(0.0)
                        .alias("ICMS_entr_desacob"),
                    ]
                )
                .group_by("Ano/Mes")
                .agg(pl.col("ICMS_entr_desacob").sum().alias("ICMS_entr_desacob"))
            )
        else:
            mensal_base = pl.DataFrame(
                schema={
                    "Ano/Mes": pl.Utf8,
                    "ICMS_entr_desacob": pl.Float64,
                }
            )

        if not anual.is_empty():
            anual_base = (
                anual.select(
                    [
                        pl.concat_str(
                            [
                                pl.col("ano").cast(pl.Utf8, strict=False),
                                pl.lit("-12"),
                            ]
                        ).alias("Ano/Mes"),
                        pl.col("ICMS_saidas_desac")
                        .cast(pl.Float64, strict=False)
                        .fill_null(0.0)
                        .alias("ICMS_saidas_desac"),
                        pl.col("ICMS_estoque_desac")
                        .cast(pl.Float64, strict=False)
                        .fill_null(0.0)
                        .alias("ICMS_estoque_desac"),
                    ]
                )
                .group_by("Ano/Mes")
                .agg(
                    [
                        pl.col("ICMS_saidas_desac").sum().alias("ICMS_saidas_desac"),
                        pl.col("ICMS_estoque_desac").sum().alias("ICMS_estoque_desac"),
                    ]
                )
            )
        else:
            anual_base = pl.DataFrame(
                schema={
                    "Ano/Mes": pl.Utf8,
                    "ICMS_saidas_desac": pl.Float64,
                    "ICMS_estoque_desac": pl.Float64,
                }
            )

        consolidado = (
            base_competencias.join(mensal_base, on="Ano/Mes", how="left")
            .join(anual_base, on="Ano/Mes", how="left")
            .with_columns(
                [
                    pl.col("ICMS_entr_desacob")
                    .cast(pl.Float64, strict=False)
                    .fill_null(0.0)
                    .round(2),
                    pl.col("ICMS_saidas_desac")
                    .cast(pl.Float64, strict=False)
                    .fill_null(0.0)
                    .round(2),
                    pl.col("ICMS_estoque_desac")
                    .cast(pl.Float64, strict=False)
                    .fill_null(0.0)
                    .round(2),
                ]
            )
            .with_columns(
                (
                    pl.col("ICMS_entr_desacob")
                    + pl.col("ICMS_saidas_desac")
                    + pl.col("ICMS_estoque_desac")
                )
                .round(2)
                .alias("Total")
            )
        )

        return consolidado.select(
            [
                "Ano/Mes",
                "ICMS_entr_desacob",
                "ICMS_saidas_desac",
                "ICMS_estoque_desac",
                "Total",
            ]
        ).sort(
            ["Ano/Mes"],
            descending=[False],
            nulls_last=True,
        )
    def atualizar_aba_resumo_global(self) -> None:
        cnpj = self.state.current_cnpj
        if not cnpj:
            return

        path = (
            CNPJ_ROOT
            / cnpj
            / "analises"
            / "produtos"
            / f"aba_resumo_global_{cnpj}.parquet"
        )

        def _finalizar_carga_resumo(
            df: pl.DataFrame | None, uniques: dict | None = None
        ) -> None:
            if df is None:
                self._resumo_global_df = pl.DataFrame()
                self.resumo_global_model.set_dataframe(pl.DataFrame())
                self.lbl_resumo_global_status.setText(
                    "Resumo global nao encontrado para este CNPJ."
                )
                return
            self._resumo_global_df = df
            self.resumo_global_model.set_dataframe(df)
            self.lbl_resumo_global_status.setText(
                f"Resumo global carregado (total {df.height} competencias)."
            )

        # Tentar carregar o arquivo do resumo global primeiro
        if path.exists():
            self.lbl_resumo_global_status.setText("⏳ Carregando resumo global...")
            self._carregar_dados_parquet_async(
                path, _finalizar_carga_resumo, "Carregando Resumo Global"
            )
            return

        # Se não existir resumo global, tentar calcular das abas mensal/anual
        if self._aba_mensal_df.is_empty() or self._aba_anual_df.is_empty():
            # Verificar se os arquivos de dependência existem antes de disparar
            path_mensal = (
                CNPJ_ROOT
                / cnpj
                / "analises"
                / "produtos"
                / f"aba_mensal_{cnpj}.parquet"
            )
            path_anual = (
                CNPJ_ROOT / cnpj / "analises" / "produtos" / f"aba_anual_{cnpj}.parquet"
            )

            needs_load = False
            if self._aba_mensal_df.is_empty() and path_mensal.exists():
                self.atualizar_aba_mensal()
                needs_load = True
            if self._aba_anual_df.is_empty() and path_anual.exists():
                self.atualizar_aba_anual()
                needs_load = True

            if needs_load:
                self.lbl_resumo_global_status.setText(
                    "⏳ Carregando dependências (Mensal/Anual)... Ao terminar, o resumo será atualizado."
                )
                return
            else:
                self.resumo_global_model.set_dataframe(pl.DataFrame())
                self._resumo_global_df = pl.DataFrame()
                self.lbl_resumo_global_status.setText(
                    "Aguarde o processamento das abas Mensal e Anual."
                )
                return

        # Se temos os dados em memória, consolidar em tempo real
        try:
            resumo = self._gerar_resumo_global(self._aba_mensal_df, self._aba_anual_df)
            self._resumo_global_df = resumo
            self.resumo_global_model.set_dataframe(resumo)
            self.lbl_resumo_global_status.setText(
                f"Resumo global consolidado ({resumo.height} competencias)."
            )
        except Exception as e:
            self.show_error(
                "Erro de consolidação", f"Erro ao calcular Resumo Global: {e}"
            )
    def exportar_resumo_global_excel(self) -> None:
        if self._resumo_global_df is None or self._resumo_global_df.is_empty():
            QMessageBox.information(
                self, "Exportacao", "Nao ha dados globais para exportar."
            )
            return
        target = self._save_dialog("Exportar Resumo Global", "Excel (*.xlsx)")
        if not target:
            return
        try:
            df_to_export = self._dataframe_colunas_perfil(
                "resumo_global",
                "resumo_global",
                self.resumo_global_model,
                self._resumo_global_df,
                perfil="Exportar",
            )
            wb = Workbook()
            ws = wb.active
            ws.title = "Resumo Global"
            self._escrever_planilha_openpyxl(ws, df_to_export)
            target.parent.mkdir(parents=True, exist_ok=True)
            wb.save(target)
            self.show_info("Exportacao concluida", f"Arquivo gerado em:\n{target}")
        except Exception as e:
            self.show_error("Erro de exportacao", str(e))
    def _montar_valores_consolidados_produtos_selecionados(
        self, ids: list[str]
    ) -> pl.DataFrame:
        resumo = self._filtrar_dataframe_por_ids(self._produtos_selecionados_df, ids)
        mensal = self._filtrar_dataframe_por_ids(
            self._produtos_selecionados_mensal_df, ids
        )
        anual = self._filtrar_dataframe_por_ids(
            self._produtos_selecionados_anual_df, ids
        )

        if resumo.is_empty():
            return pl.DataFrame(
                schema={
                    "Ano/Mes": pl.Utf8,
                    "ICMS_entr_desacob": pl.Float64,
                    "ICMS_saidas_desac": pl.Float64,
                    "ICMS_estoque_desac": pl.Float64,
                    "Total": pl.Float64,
                }
            )

        anos_base: list[int] = []
        ano_ini, ano_fim = self._intervalo_anos_produtos_selecionados()
        if ano_ini is not None and ano_fim is not None:
            anos_base = list(range(int(ano_ini), int(ano_fim) + 1))
        else:
            anos_base = None

        return self._gerar_resumo_global(mensal, anual, anos_base)
    def exportar_produtos_selecionados_excel(self) -> None:
        if self._produtos_selecionados_df.is_empty():
            QMessageBox.information(
                self, "Exportacao", "Nao ha dados consolidados para exportar."
            )
            return

        from interface_grafica.ui.dialogs import DialogoExportacaoEstoque

        dlg = DialogoExportacaoEstoque(self)
        if not dlg.exec():
            return
        dt_ini, dt_fim = dlg.obter_datas()

        target = self._save_dialog(
            "Exportar mov_estoque, mensal e anual", "Excel (*.xlsx)"
        )
        if not target:
            return
        try:
            ids = self._ids_produtos_selecionados_para_exportacao()
            if not ids:
                QMessageBox.information(
                    self,
                    "Exportacao",
                    "Marque pelo menos um id_agregado em Produtos selecionados.",
                )
                return

            df_mensal_export = self._filtrar_dataframe_produtos_selecionados_por_data(
                self._produtos_selecionados_mensal_df, dt_ini, dt_fim, "mensal"
            )
            df_anual_export = self._filtrar_dataframe_produtos_selecionados_por_data(
                self._produtos_selecionados_anual_df, dt_ini, dt_fim, "anual"
            )
            df_mov_export = self._filtrar_dataframe_produtos_selecionados_por_data(
                self._produtos_selecionados_mov_df, dt_ini, dt_fim, "mov_estoque"
            )

            mensal = self._dataframe_colunas_perfil(
                "aba_mensal",
                "aba_mensal",
                self.aba_mensal_model,
                self._filtrar_dataframe_por_ids(df_mensal_export, ids),
                perfil="Exportar",
            )
            anual = self._dataframe_colunas_perfil(
                "aba_anual",
                "aba_anual",
                self.aba_anual_model,
                self._filtrar_dataframe_por_ids(df_anual_export, ids),
                perfil="Exportar",
            )
            mov = self._dataframe_colunas_perfil(
                "mov_estoque",
                "mov_estoque",
                self.mov_estoque_model,
                self._filtrar_dataframe_por_ids(df_mov_export, ids),
                perfil="Exportar",
            )
            periodos = self._dataframe_colunas_perfil(
                "aba_periodos",
                "aba_periodos",
                self.aba_periodos_model,
                self._filtrar_dataframe_por_ids(
                    self._produtos_selecionados_periodos_df, ids
                ),
                perfil="Exportar",
            )
            valores_consolidados = (
                self._montar_valores_consolidados_produtos_selecionados(ids)
            )

            produtos_selecionados_tabela = self._dataframe_colunas_perfil(
                "produtos_selecionados",
                "produtos_selecionados",
                self.produtos_selecionados_model,
                self._filtrar_dataframe_por_ids(self._produtos_selecionados_df, ids),
                perfil="Exportar",
            )

            wb = Workbook()
            ws_produtos = wb.active
            ws_produtos.title = "Produtos_Selecionados"
            self._escrever_planilha_openpyxl(ws_produtos, produtos_selecionados_tabela)

            self._escrever_planilha_openpyxl(wb.create_sheet("Mov_Estoque"), mov)
            self._escrever_planilha_openpyxl(wb.create_sheet("Mensal"), mensal)
            self._escrever_planilha_openpyxl(wb.create_sheet("Anual"), anual)
            self._escrever_planilha_openpyxl(wb.create_sheet("Periodos"), periodos)
            self._escrever_planilha_openpyxl(
                wb.create_sheet("ICMS_devido"), valores_consolidados
            )
            target.parent.mkdir(parents=True, exist_ok=True)
            wb.save(target)
            self.show_info("Exportacao concluida", f"Arquivo gerado em:\n{target}")
        except Exception as e:
            self.show_error("Erro de exportacao", str(e))
