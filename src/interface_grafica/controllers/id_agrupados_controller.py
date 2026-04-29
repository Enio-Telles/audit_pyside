from __future__ import annotations

import polars as pl
from openpyxl import Workbook
from PySide6.QtCore import Qt

from interface_grafica.config import CNPJ_ROOT


def _adicionar_qtd_descricoes(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty() or "lista_descricoes" not in df.columns:
        return df

    df = df.with_columns(
        pl.col("lista_descricoes")
        .cast(pl.List(pl.Utf8), strict=False)
        .list.eval(pl.element().filter(pl.element().is_not_null() & (pl.element() != "")))
        .list.len()
        .cast(pl.Int64)
        .alias("qtd_descricoes")
    )
    colunas = list(df.columns)
    if "qtd_descricoes" in colunas and "lista_descricoes" in colunas:
        colunas.remove("qtd_descricoes")
        colunas.insert(colunas.index("lista_descricoes") + 1, "qtd_descricoes")
        df = df.select(colunas)
    return df


class IdAgrupadosControllerMixin:
    def atualizar_aba_id_agrupados(self) -> None:
        cnpj = self.state.current_cnpj
        if not cnpj:
            self.id_agrupados_model.set_dataframe(pl.DataFrame())
            self._id_agrupados_df = pl.DataFrame()
            self.lbl_id_agrupados_status.setText(
                "Selecione um CPF/CNPJ para carregar os id_agrupados."
            )
            self._atualizar_titulo_aba_id_agrupados()
            return

        path = (
            CNPJ_ROOT / cnpj / "analises" / "produtos" / f"id_agrupados_{cnpj}.parquet"
        )
        if not path.exists():
            self.id_agrupados_model.set_dataframe(pl.DataFrame())
            self._id_agrupados_df = pl.DataFrame()
            self.lbl_id_agrupados_status.setText(
                "Arquivo 'id_agrupados' nao encontrado para este CPF/CNPJ."
            )
            self._atualizar_titulo_aba_id_agrupados()
            return

        def _finalizar_carga_id_agrupados(
            df: pl.DataFrame | None, uniques: dict | None = None
        ) -> None:
            if df is None:
                self.id_agrupados_model.set_dataframe(pl.DataFrame())
                self._id_agrupados_df = pl.DataFrame()
                self.lbl_id_agrupados_status.setText(
                    "Tabela id_agrupados nao encontrada para este CNPJ."
                )
                self._atualizar_titulo_aba_id_agrupados()
                return
            df = _adicionar_qtd_descricoes(df)
            self._id_agrupados_df = df
            self._id_agrupados_file_path = path
            self._reset_table_resize_flag("id_agrupados")

            id_atual = self.id_agrupados_filter_id.currentText()
            if uniques and "id_agrupado" in uniques:
                self._popular_combo_texto(
                    self.id_agrupados_filter_id,
                    [str(i) for i in uniques["id_agrupado"]],
                    id_atual,
                    "",
                )

            self.aplicar_filtros_id_agrupados()
            self._atualizar_titulo_aba_id_agrupados()

        self.lbl_id_agrupados_status.setText(
            "⏳ Carregando id_agrupados em segundo plano..."
        )
        self._carregar_dados_parquet_async(
            path,
            _finalizar_carga_id_agrupados,
            "Carregando ID Agrupados",
            unique_cols=["id_agrupado"],
        )
    def aplicar_filtros_id_agrupados(self) -> None:
        if self._id_agrupados_df.is_empty():
            return
        try:
            id_agrupado = self.id_agrupados_filter_id.currentText().strip()
            texto = self.id_agrupados_filter_texto.text().strip().lower()

            df_filtrado = self._id_agrupados_df
            if id_agrupado and "id_agrupado" in df_filtrado.columns:
                df_filtrado = df_filtrado.filter(
                    pl.col("id_agrupado")
                    .cast(pl.Utf8, strict=False)
                    .fill_null("")
                    .str.contains(id_agrupado, literal=True)
                )
            if texto:
                exprs = []
                for col in df_filtrado.columns:
                    dtype = df_filtrado.schema.get(col)
                    if dtype in [pl.Utf8, pl.Categorical]:
                        exprs.append(
                            pl.col(col)
                            .cast(pl.Utf8, strict=False)
                            .fill_null("")
                            .str.to_lowercase()
                            .str.contains(texto, literal=True)
                        )
                    elif isinstance(dtype, pl.List):
                        exprs.append(
                            pl.col(col)
                            .list.join(" | ", ignore_nulls=True)
                            .cast(pl.Utf8, strict=False)
                            .fill_null("")
                            .str.to_lowercase()
                            .str.contains(texto, literal=True)
                        )
                if exprs:
                    df_filtrado = df_filtrado.filter(pl.any_horizontal(exprs))

            self.id_agrupados_model.set_dataframe(df_filtrado)
            self._resize_table_once(self.id_agrupados_table, "id_agrupados")
            if not self._aplicar_preferencias_tabela(
                "id_agrupados", self.id_agrupados_table, self.id_agrupados_model
            ):
                self._aplicar_ordenacao_padrao(
                    self.id_agrupados_table,
                    self.id_agrupados_model,
                    ["id_agrupado"],
                    Qt.AscendingOrder,
                )
                self._aplicar_preset_colunas(
                    self.id_agrupados_table,
                    self.id_agrupados_model.dataframe.columns,
                    self._obter_colunas_preset_perfil(
                        "auditoria",
                        self.id_agrupados_model.dataframe.columns,
                        "id_agrupados",
                    ),
                )
            self.lbl_id_agrupados_status.setText(
                f"Exibindo {df_filtrado.height:,} de {self._id_agrupados_df.height:,} grupos consolidados."
            )
            self.lbl_id_agrupados_filtros.setText(
                self._formatar_resumo_filtros(
                    [
                        ("id_agrupado", id_agrupado),
                        ("texto", texto),
                    ]
                )
            )
            self._salvar_preferencias_tabela(
                "id_agrupados", self.id_agrupados_table, self.id_agrupados_model
            )
            self._atualizar_titulo_aba_id_agrupados(
                df_filtrado.height, self._id_agrupados_df.height
            )
        except Exception as e:
            self.show_error("Erro", f"Erro ao filtrar id_agrupados: {e}")
    def limpar_filtros_id_agrupados(self) -> None:
        self.id_agrupados_filter_id.setCurrentIndex(0)
        self.id_agrupados_filter_texto.clear()
        self.aplicar_filtros_id_agrupados()
    def exportar_id_agrupados_excel(self) -> None:
        df = self._dataframe_colunas_perfil(
            "id_agrupados",
            "id_agrupados",
            self.id_agrupados_model,
            self.id_agrupados_model.dataframe,
            perfil="Exportar",
        )
        if df.is_empty():
            self.show_info("Exportacao", "Nao ha dados de id_agrupados para exportar.")
            return
        target = self._save_dialog("Exportar id_agrupados", "Excel (*.xlsx)")
        if not target:
            return
        try:
            wb = Workbook()
            ws_id_agrupados = wb.active
            ws_id_agrupados.title = "id_agrupados"
            self._escrever_planilha_openpyxl(ws_id_agrupados, df)

            df_produtos_sel = self._dataframe_colunas_visiveis(
                self.produtos_sel_table,
                self.produtos_selecionados_model,
            )
            if not df_produtos_sel.is_empty():
                self._escrever_planilha_openpyxl(
                    wb.create_sheet("produtos_selecionados"),
                    df_produtos_sel,
                )

            target.parent.mkdir(parents=True, exist_ok=True)
            wb.save(target)
            self.show_info("Exportacao concluida", f"Arquivo gerado em:\n{target}")
        except Exception as e:
            self.show_error("Erro de exportacao", str(e))
