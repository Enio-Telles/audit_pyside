from __future__ import annotations

from datetime import date

import polars as pl
from PySide6.QtCore import QDate
from PySide6.QtWidgets import QMessageBox

from interface_grafica.config import CNPJ_ROOT
from interface_grafica.controllers.workers import ServiceTaskWorker


class RelatoriosProdutosControllerMixin:
    def atualizar_aba_produtos_selecionados(self) -> None:
        cnpj = self.state.current_cnpj
        if not cnpj:
            self.produtos_selecionados_model.set_dataframe(pl.DataFrame())
            self._produtos_selecionados_df = pl.DataFrame()
            self._produtos_selecionados_mov_df = pl.DataFrame()
            self._produtos_selecionados_mensal_df = pl.DataFrame()
            self._produtos_selecionados_anual_df = pl.DataFrame()
            self.lbl_produtos_sel_status.setText(
                "Selecione um CNPJ para consolidar os produtos analisados."
            )
            self.lbl_produtos_sel_resumo.setText(
                "Recorte atual: mov_estoque 0 | mensal 0 | anual 0"
            )
            self._atualizar_titulo_aba_produtos_selecionados()
            return

        path = (
            CNPJ_ROOT / cnpj / "analises" / "produtos" / f"aba_produtos_selecionados_{cnpj}.parquet"
        )

        def _finalizar_carga_produtos_sel(
            df: pl.DataFrame | None, uniques: dict | None = None
        ) -> None:
            if df is None:
                self.produtos_selecionados_model.set_dataframe(pl.DataFrame())
                self._produtos_selecionados_df = pl.DataFrame()
                self.lbl_produtos_sel_status.setText(
                    "Tabela de produtos selecionados nao encontrada para este CNPJ."
                )
                self._atualizar_titulo_aba_produtos_selecionados()
                return
            self._produtos_selecionados_df = df
            self._reset_table_resize_flag("produtos_selecionados")

            id_atual = self.produtos_sel_filter_id.currentText()
            if uniques and "id_agregado" in uniques:
                self._popular_combo_texto(
                    self.produtos_sel_filter_id,
                    [str(i) for i in uniques["id_agregado"]],
                    id_atual,
                    "",
                )

            anos = self._anos_disponiveis_produtos_selecionados()
            anos_texto = [str(a) for a in anos]
            self._popular_combo_texto(
                self.produtos_sel_filter_ano_ini,
                anos_texto,
                self.produtos_sel_filter_ano_ini.currentText(),
                "Todos",
            )
            self._popular_combo_texto(
                self.produtos_sel_filter_ano_fim,
                anos_texto,
                self.produtos_sel_filter_ano_fim.currentText(),
                "Todos",
            )

            self.aplicar_filtros_produtos_selecionados()
            self._atualizar_titulo_aba_produtos_selecionados()

        if path.exists():
            self.lbl_id_agrupados_status.setText(
                "⏳ Carregando base de produtos selecionados em segundo plano..."
            )
            self._carregar_dados_parquet_async(
                path,
                _finalizar_carga_produtos_sel,
                "Carregando Produtos Selecionados",
                unique_cols=["id_agregado"],
            )
            return

        # Fallback para consolidar em tempo real (também em background para não travar)
        def _worker_consolidar():
            df = self._coletar_base_produtos_selecionados()
            # Extração de IDs únicos igual ao async loader
            ids = (
                df.get_column("id_agrupado")
                .cast(pl.Utf8, strict=False)
                .drop_nulls()
                .unique()
                .sort()
                .to_list()
                if "id_agrupado" in df.columns
                else []
            )
            return {"df": df, "uniques": {"id_agregado": ids}}

        self.lbl_produtos_sel_status.setText(
            "⏳ Consolidando produtos em tempo real no background..."
        )
        worker = ServiceTaskWorker(_worker_consolidar)
        worker.finished_ok.connect(
            lambda res: _finalizar_carga_produtos_sel(res["df"], res["uniques"])
        )
        worker.failed.connect(
            lambda err: self.show_error(
                "Erro de leitura", f"Falha ao consolidar produtos selecionados: {err}"
            )
        )

        if not hasattr(self, "_active_load_workers") or not isinstance(
            self._active_load_workers, set
        ):
            self._active_load_workers = set()
        self._active_load_workers.add(worker)
        worker.finished.connect(lambda: self._active_load_workers.discard(worker))
        worker.finished.connect(worker.deleteLater)
        worker.start()

    def _coletar_base_produtos_selecionados(self) -> pl.DataFrame:
        bases: list[pl.DataFrame] = []
        if not self._aba_mensal_df.is_empty() and {
            "id_agregado",
            "descr_padrao",
        }.issubset(set(self._aba_mensal_df.columns)):
            bases.append(self._aba_mensal_df.select(["id_agregado", "descr_padrao"]))
        if not self._aba_anual_df.is_empty() and {
            "id_agregado",
            "descr_padrao",
        }.issubset(set(self._aba_anual_df.columns)):
            bases.append(self._aba_anual_df.select(["id_agregado", "descr_padrao"]))
        if not self._mov_estoque_df.is_empty():
            col_id = (
                "id_agregado"
                if "id_agregado" in self._mov_estoque_df.columns
                else ("id_agrupado" if "id_agrupado" in self._mov_estoque_df.columns else None)
            )
            col_desc = "descr_padrao" if "descr_padrao" in self._mov_estoque_df.columns else None
            if col_id and col_desc:
                bases.append(
                    self._mov_estoque_df.select(
                        [
                            pl.col(col_id).cast(pl.Utf8, strict=False).alias("id_agregado"),
                            pl.col(col_desc).cast(pl.Utf8, strict=False).alias("descr_padrao"),
                        ]
                    )
                )
        if not bases:
            return pl.DataFrame(
                {"id_agregado": [], "descr_padrao": []},
                schema={"id_agregado": pl.Utf8, "descr_padrao": pl.Utf8},
            )
        return (
            pl.concat(bases, how="vertical_relaxed")
            .unique(subset=["id_agregado"])
            .sort("id_agregado")
        )

    def _anos_disponiveis_produtos_selecionados(self) -> list[int]:
        anos: set[int] = set()
        for df in (self._aba_mensal_df, self._aba_anual_df):
            if not df.is_empty() and "ano" in df.columns:
                try:
                    anos.update(
                        int(a) for a in df.get_column("ano").drop_nulls().unique().to_list()
                    )
                except Exception:
                    pass
        if not anos and not self._mov_estoque_df.is_empty():
            for col in ("Dt_e_s", "Dt_doc"):
                if col in self._mov_estoque_df.columns:
                    try:
                        serie = (
                            self._mov_estoque_df.get_column(col)
                            .cast(pl.Date, strict=False)
                            .drop_nulls()
                            .dt.year()
                            .unique()
                            .to_list()
                        )
                        anos.update(int(a) for a in serie if a is not None)
                    except Exception:
                        pass
        return sorted(anos)

    def _intervalo_anos_produtos_selecionados(self) -> tuple[int | None, int | None]:
        ano_ini_txt = self.produtos_sel_filter_ano_ini.currentText().strip()
        ano_fim_txt = self.produtos_sel_filter_ano_fim.currentText().strip()
        ano_ini = int(ano_ini_txt) if ano_ini_txt and ano_ini_txt != "Todos" else None
        ano_fim = int(ano_fim_txt) if ano_fim_txt and ano_fim_txt != "Todos" else None
        if ano_ini is not None and ano_fim is not None and ano_ini > ano_fim:
            ano_ini, ano_fim = ano_fim, ano_ini
        return ano_ini, ano_fim

    def _intervalo_datas_produtos_selecionados(
        self,
    ) -> tuple[QDate | None, QDate | None]:
        data_ini = self._valor_qdate_ativo(self.produtos_sel_filter_data_ini.date())
        data_fim = self._valor_qdate_ativo(self.produtos_sel_filter_data_fim.date())
        if data_ini is not None and data_fim is not None and data_ini > data_fim:
            data_ini, data_fim = data_fim, data_ini
        return data_ini, data_fim

    def _filtrar_dataframe_por_ids(self, df: pl.DataFrame, ids: list[str]) -> pl.DataFrame:
        if df.is_empty() or not ids:
            return df
        if "id_agregado" in df.columns:
            return df.filter(pl.col("id_agregado").cast(pl.Utf8, strict=False).is_in(ids))
        if "id_agrupado" in df.columns:
            return df.filter(pl.col("id_agrupado").cast(pl.Utf8, strict=False).is_in(ids))
        return df

    def _filtrar_dataframe_por_ano(
        self, df: pl.DataFrame, ano_ini: int | None, ano_fim: int | None
    ) -> pl.DataFrame:
        if df.is_empty() or (ano_ini is None and ano_fim is None):
            return df
        if "ano" in df.columns:
            ano_expr = pl.col("ano").cast(pl.Int32, strict=False)
            if ano_ini is not None:
                df = df.filter(ano_expr >= ano_ini)
            if ano_fim is not None:
                df = df.filter(ano_expr <= ano_fim)
            return df
        data_col = None
        for col in ("Dt_e_s", "Dt_doc"):
            if col in df.columns:
                data_col = col
                break
        if data_col is None:
            return df
        ano_expr = pl.col(data_col).cast(pl.Date, strict=False).dt.year()
        if ano_ini is not None:
            df = df.filter(ano_expr >= ano_ini)
        if ano_fim is not None:
            df = df.filter(ano_expr <= ano_fim)
        return df

    def _filtrar_dataframe_produtos_selecionados_por_data(
        self,
        df: pl.DataFrame,
        data_ini: QDate | None,
        data_fim: QDate | None,
        tipo_base: str,
    ) -> pl.DataFrame:
        if df.is_empty() or (data_ini is None and data_fim is None):
            return df

        if tipo_base == "mensal" and {"ano", "mes"}.issubset(set(df.columns)):
            df_tmp = df.with_columns(
                pl.concat_str(
                    [
                        pl.col("ano").cast(pl.Int32, strict=False).cast(pl.Utf8),
                        pl.lit("-"),
                        pl.col("mes").cast(pl.Int32, strict=False).cast(pl.Utf8).str.zfill(2),
                        pl.lit("-01"),
                    ]
                )
                .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                .alias("__data_ref_filtro__")
            )
            df_tmp = self._filtrar_intervalo_data(df_tmp, "__data_ref_filtro__", data_ini, data_fim)
            return df_tmp.drop("__data_ref_filtro__", strict=False)

        if tipo_base == "anual" and "ano" in df.columns:
            df_tmp = df.with_columns(
                pl.concat_str(
                    [
                        pl.col("ano").cast(pl.Int32, strict=False).cast(pl.Utf8),
                        pl.lit("-12-31"),
                    ]
                )
                .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                .alias("__data_ref_filtro__")
            )
            df_tmp = self._filtrar_intervalo_data(df_tmp, "__data_ref_filtro__", data_ini, data_fim)
            return df_tmp.drop("__data_ref_filtro__", strict=False)

        return self._filtrar_intervalo_data(
            df, "Dt_e_s" if "Dt_e_s" in df.columns else "Dt_doc", data_ini, data_fim
        )

    def _ids_produtos_selecionados_para_exportacao(self) -> list[str]:
        checked = self.produtos_selecionados_model.get_checked_rows()
        ids = [str(r.get("id_agregado") or "").strip() for r in checked if r.get("id_agregado")]
        if ids:
            return sorted(set(ids))
        return []

    def aplicar_filtros_produtos_selecionados(self) -> None:
        try:
            base = self._coletar_base_produtos_selecionados()
            if base.is_empty():
                self.produtos_selecionados_model.set_dataframe(pl.DataFrame())
                self._produtos_selecionados_df = pl.DataFrame()
                self._produtos_selecionados_mov_df = pl.DataFrame()
                self._produtos_selecionados_mensal_df = pl.DataFrame()
                self._produtos_selecionados_anual_df = pl.DataFrame()
                self._produtos_selecionados_periodos_df = pl.DataFrame()
                self.lbl_produtos_sel_status.setText(
                    "Nenhum dado de estoque/mensal/anual foi encontrado para consolidacao."
                )
                self.lbl_produtos_sel_resumo.setText(
                    "Recorte atual: mov_estoque 0 | mensal 0 | anual 0 | periodos 0"
                )
                self._atualizar_titulo_aba_produtos_selecionados(0, 0)
                return

            id_agregado = self.produtos_sel_filter_id.currentText().strip()
            desc = self.produtos_sel_filter_desc.text().strip().lower()
            texto = self.produtos_sel_filter_texto.text().strip().lower()
            ano_ini, ano_fim = self._intervalo_anos_produtos_selecionados()
            data_ini, data_fim = self._intervalo_datas_produtos_selecionados()

            df_produtos = base
            if id_agregado:
                df_produtos = df_produtos.filter(
                    pl.col("id_agregado")
                    .cast(pl.Utf8, strict=False)
                    .fill_null("")
                    .str.contains(id_agregado, literal=True)
                )
            if desc:
                df_produtos = df_produtos.filter(
                    pl.col("descr_padrao")
                    .cast(pl.Utf8, strict=False)
                    .fill_null("")
                    .str.to_lowercase()
                    .str.contains(desc, literal=True)
                )
            if texto:
                df_produtos = self._filtrar_texto_em_colunas(df_produtos, texto)

            ids_filtrados = (
                df_produtos.get_column("id_agregado")
                .cast(pl.Utf8, strict=False)
                .drop_nulls()
                .unique()
                .sort()
                .to_list()
                if "id_agregado" in df_produtos.columns
                else []
            )

            df_mensal = self._filtrar_dataframe_por_ids(self._aba_mensal_df, ids_filtrados)
            df_mensal = self._filtrar_dataframe_por_ano(df_mensal, ano_ini, ano_fim)
            df_mensal = self._filtrar_dataframe_produtos_selecionados_por_data(
                df_mensal, data_ini, data_fim, "mensal"
            )
            if not df_mensal.is_empty() and {"id_agregado", "descr_padrao"}.issubset(
                set(df_mensal.columns)
            ):
                agg_mensal = [
                    pl.col("ICMS_entr_desacob")
                    .cast(pl.Float64, strict=False)
                    .fill_null(0.0)
                    .sum()
                    .alias("total_ICMS_entr_desacob"),
                ]
                if "ICMS_entr_desacob_periodo" in df_mensal.columns:
                    agg_mensal.append(
                        pl.col("ICMS_entr_desacob_periodo")
                        .cast(pl.Float64, strict=False)
                        .fill_null(0.0)
                        .sum()
                        .alias("total_ICMS_entr_desacob_periodo")
                    )
                else:
                    agg_mensal.append(pl.lit(0.0).alias("total_ICMS_entr_desacob_periodo"))
                resumo_mensal = df_mensal.group_by(["id_agregado", "descr_padrao"]).agg(agg_mensal)
            else:
                resumo_mensal = pl.DataFrame(
                    schema={
                        "id_agregado": pl.Utf8,
                        "descr_padrao": pl.Utf8,
                        "total_ICMS_entr_desacob": pl.Float64,
                        "total_ICMS_entr_desacob_periodo": pl.Float64,
                    }
                )

            df_anual = self._filtrar_dataframe_por_ids(self._aba_anual_df, ids_filtrados)
            df_anual = self._filtrar_dataframe_por_ano(df_anual, ano_ini, ano_fim)
            df_anual = self._filtrar_dataframe_produtos_selecionados_por_data(
                df_anual, data_ini, data_fim, "anual"
            )
            if not df_anual.is_empty() and {"id_agregado", "descr_padrao"}.issubset(
                set(df_anual.columns)
            ):
                resumo_anual = df_anual.group_by(["id_agregado", "descr_padrao"]).agg(
                    [
                        pl.col("ICMS_saidas_desac")
                        .cast(pl.Float64, strict=False)
                        .fill_null(0)
                        .sum()
                        .alias("total_ICMS_saidas_desac"),
                        pl.col("ICMS_estoque_desac")
                        .cast(pl.Float64, strict=False)
                        .fill_null(0)
                        .sum()
                        .alias("total_ICMS_estoque_desac"),
                    ]
                )
            else:
                resumo_anual = pl.DataFrame(
                    schema={
                        "id_agregado": pl.Utf8,
                        "descr_padrao": pl.Utf8,
                        "total_ICMS_saidas_desac": pl.Float64,
                        "total_ICMS_estoque_desac": pl.Float64,
                    }
                )

            df_periodos = self._filtrar_dataframe_por_ids(self._aba_periodos_df, ids_filtrados)
            _tem_col_saida_per = "ICMS_saidas_desac_periodo" in df_periodos.columns
            _tem_col_estoque_per = "ICMS_estoque_desac_periodo" in df_periodos.columns
            if (
                not df_periodos.is_empty()
                and {"id_agregado", "descr_padrao"}.issubset(set(df_periodos.columns))
                and (_tem_col_saida_per or _tem_col_estoque_per)
            ):
                aggs_periodo = []
                if _tem_col_saida_per:
                    aggs_periodo.append(
                        pl.col("ICMS_saidas_desac_periodo")
                        .cast(pl.Float64, strict=False)
                        .fill_null(0.0)
                        .sum()
                        .alias("total_ICMS_saidas_desac_periodo")
                    )
                else:
                    aggs_periodo.append(pl.lit(0.0).alias("total_ICMS_saidas_desac_periodo"))
                if _tem_col_estoque_per:
                    aggs_periodo.append(
                        pl.col("ICMS_estoque_desac_periodo")
                        .cast(pl.Float64, strict=False)
                        .fill_null(0.0)
                        .sum()
                        .alias("total_ICMS_estoque_desac_periodo")
                    )
                else:
                    aggs_periodo.append(pl.lit(0.0).alias("total_ICMS_estoque_desac_periodo"))
                resumo_periodos = df_periodos.group_by(["id_agregado", "descr_padrao"]).agg(
                    aggs_periodo
                )
            else:
                resumo_periodos = pl.DataFrame(
                    schema={
                        "id_agregado": pl.Utf8,
                        "descr_padrao": pl.Utf8,
                        "total_ICMS_saidas_desac_periodo": pl.Float64,
                        "total_ICMS_estoque_desac_periodo": pl.Float64,
                    }
                )

            resumo = (
                df_produtos.join(resumo_mensal, on=["id_agregado", "descr_padrao"], how="left")
                .join(resumo_anual, on=["id_agregado", "descr_padrao"], how="left")
                .join(resumo_periodos, on=["id_agregado", "descr_padrao"], how="left")
                .with_columns(
                    [
                        pl.col("total_ICMS_entr_desacob")
                        .cast(pl.Float64, strict=False)
                        .fill_null(0)
                        .round(2),
                        pl.col("total_ICMS_saidas_desac")
                        .cast(pl.Float64, strict=False)
                        .fill_null(0)
                        .round(2),
                        pl.col("total_ICMS_estoque_desac")
                        .cast(pl.Float64, strict=False)
                        .fill_null(0)
                        .round(2),
                        pl.col("total_ICMS_entr_desacob_periodo")
                        .cast(pl.Float64, strict=False)
                        .fill_null(0)
                        .round(2),
                        pl.col("total_ICMS_saidas_desac_periodo")
                        .cast(pl.Float64, strict=False)
                        .fill_null(0)
                        .round(2),
                        pl.col("total_ICMS_estoque_desac_periodo")
                        .cast(pl.Float64, strict=False)
                        .fill_null(0)
                        .round(2),
                        (
                            pl.col("total_ICMS_entr_desacob")
                            .cast(pl.Float64, strict=False)
                            .fill_null(0)
                            + pl.col("total_ICMS_saidas_desac")
                            .cast(pl.Float64, strict=False)
                            .fill_null(0)
                            + pl.col("total_ICMS_estoque_desac")
                            .cast(pl.Float64, strict=False)
                            .fill_null(0)
                        )
                        .round(2)
                        .alias("total_ICMS_total"),
                        (
                            pl.col("total_ICMS_entr_desacob_periodo")
                            .cast(pl.Float64, strict=False)
                            .fill_null(0)
                            + pl.col("total_ICMS_saidas_desac_periodo")
                            .cast(pl.Float64, strict=False)
                            .fill_null(0)
                            + pl.col("total_ICMS_estoque_desac_periodo")
                            .cast(pl.Float64, strict=False)
                            .fill_null(0)
                        )
                        .round(2)
                        .alias("total_ICMS_total_periodo"),
                    ]
                )
                .sort(["descr_padrao", "id_agregado"], nulls_last=True)
            )

            self._produtos_selecionados_df = resumo
            self._produtos_selecionados_mensal_df = df_mensal
            self._produtos_selecionados_anual_df = df_anual
            self._produtos_selecionados_mov_df = self._filtrar_dataframe_por_ano(
                self._filtrar_dataframe_por_ids(self._mov_estoque_df, ids_filtrados),
                ano_ini,
                ano_fim,
            )
            self._produtos_selecionados_mov_df = (
                self._filtrar_dataframe_produtos_selecionados_por_data(
                    self._produtos_selecionados_mov_df,
                    data_ini,
                    data_fim,
                    "mov",
                )
            )
            self._produtos_selecionados_periodos_df = df_periodos

            self.produtos_selecionados_model.set_dataframe(resumo)
            if (
                self.state.current_cnpj
                and self._produtos_sel_preselecionado_cnpj != self.state.current_cnpj
            ):
                top_ids = (
                    (
                        resumo.sort(
                            ["total_ICMS_total", "id_agregado"],
                            descending=[True, False],
                            nulls_last=True,
                        )
                        .head(20)
                        .get_column("id_agregado")
                        .cast(pl.Utf8, strict=False)
                        .drop_nulls()
                        .to_list()
                    )
                    if "id_agregado" in resumo.columns
                    else []
                )
                self.produtos_selecionados_model.set_checked_keys(
                    {(str(item_id),) for item_id in top_ids if item_id is not None}
                )
                self._produtos_sel_preselecionado_cnpj = self.state.current_cnpj
            self._resize_table_once(self.produtos_sel_table, "produtos_selecionados")
            self._aplicar_preferencias_tabela(
                "produtos_selecionados",
                self.produtos_sel_table,
                self.produtos_selecionados_model,
            )

            self.lbl_produtos_sel_status.setText(
                f"Exibindo {resumo.height:,} produtos consolidados para o periodo selecionado."
            )
            periodo = ""
            if data_ini is not None or data_fim is not None:
                periodo = f"{data_ini.toString('dd/MM/yyyy') if data_ini is not None else '...'} ate {data_fim.toString('dd/MM/yyyy') if data_fim is not None else '...'}"
            elif ano_ini is not None or ano_fim is not None:
                periodo = f"{ano_ini or '...'} ate {ano_fim or '...'}"
            self.lbl_produtos_sel_filtros.setText(
                self._formatar_resumo_filtros(
                    [
                        ("id_agregado", id_agregado),
                        ("descricao", desc),
                        ("periodo", periodo),
                        ("texto", texto),
                    ]
                )
            )
            self.lbl_produtos_sel_resumo.setText(
                f"Recorte atual: mov_estoque {self._produtos_selecionados_mov_df.height:,} | mensal {self._produtos_selecionados_mensal_df.height:,} | anual {self._produtos_selecionados_anual_df.height:,} | periodos {self._produtos_selecionados_periodos_df.height:,}"
            )
            self._atualizar_titulo_aba_produtos_selecionados(resumo.height, base.height)
            self._salvar_preferencias_tabela(
                "produtos_selecionados",
                self.produtos_sel_table,
                self.produtos_selecionados_model,
            )
        except Exception as e:
            QMessageBox.warning(self, "Erro", f"Erro ao consolidar produtos selecionados: {e}")

    def limpar_vistos_produtos_selecionados(self) -> None:
        self.produtos_selecionados_model.clear_checked()
        self._salvar_preferencias_tabela(
            "produtos_selecionados",
            self.produtos_sel_table,
            self.produtos_selecionados_model,
        )
        self.status.showMessage("Vistos limpos.")

    def selecionar_top20_icms_produtos_selecionados(self) -> None:
        self._selecionar_top_n_icms_produtos(col="total_ICMS_total", n=20)

    def selecionar_top20_icms_periodo_produtos_selecionados(self) -> None:
        self._selecionar_top_n_icms_produtos(col="total_ICMS_total_periodo", n=20)

    def _selecionar_top_n_icms_produtos(self, col: str, n: int) -> None:
        df = self.produtos_selecionados_model.get_dataframe()
        if df.is_empty() or col not in df.columns:
            self.status.showMessage(f"Coluna '{col}' nao disponivel.")
            return
        top_ids = df.sort(col, descending=True).head(n).get_column("id_agregado").to_list()
        ids_set = set(top_ids)
        model = self.produtos_selecionados_model
        model.uncheck_all()
        for row in range(model.rowCount()):
            row_data = model.row_as_dict(row)
            if row_data.get("id_agregado") in ids_set:
                from PySide6.QtCore import Qt

                idx = model.index(row, 0)
                model.setData(idx, Qt.CheckState.Checked, Qt.ItemDataRole.CheckStateRole)
        self.status.showMessage(f"Top {n} por '{col}' marcados.")

    def limpar_filtros_produtos_selecionados(self) -> None:
        self.produtos_sel_filter_id.setCurrentIndex(0)
        self.produtos_sel_filter_desc.clear()
        self.produtos_sel_filter_ano_ini.setCurrentIndex(0)
        self.produtos_sel_filter_ano_fim.setCurrentIndex(0)
        self.produtos_sel_filter_data_ini.setDate(self.produtos_sel_filter_data_ini.minimumDate())
        self.produtos_sel_filter_data_fim.setDate(self.produtos_sel_filter_data_fim.minimumDate())
        self.produtos_sel_filter_texto.clear()
        self.aplicar_filtros_produtos_selecionados()
