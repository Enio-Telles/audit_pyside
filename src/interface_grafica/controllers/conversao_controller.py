from __future__ import annotations

from pathlib import Path

import polars as pl
from PySide6.QtWidgets import QFileDialog, QMessageBox

from interface_grafica.config import CNPJ_ROOT
from interface_grafica.controllers.workers import ServiceTaskWorker
from interface_grafica.utils.safe_slot import safe_slot
from interface_grafica.utils.validators import validate_cnpj, validate_path_exists


class ConversaoControllerMixin:
    def _on_conversion_selection_changed(self, selected, deselected) -> None:
        indexes = self.conversion_table.selectionModel().selectedIndexes()
        if not indexes or self._conversion_df_full.is_empty():
            self.lbl_produto_sel.setText("Nenhum produto selecionado")
            self.combo_unid_ref.clear()
            self.combo_unid_ref.setEnabled(False)
            self.btn_apply_unid_ref.setEnabled(False)
            self._current_selected_id_produto = None
            return

        row = indexes[0].row()
        df = self.conversion_model.dataframe
        if row < 0 or row >= df.height:
            return

        id_prod = df.item(row, df.columns.index("id_produtos"))
        descr = df.item(row, df.columns.index("descr_padrao"))

        self.lbl_produto_sel.setText(f"{id_prod} - {descr}")
        self._current_selected_id_produto = id_prod

        # Obter unidades unicas originais vinculadas a este ID
        try:
            unidades_s = (
                self._conversion_df_full.filter(pl.col("id_produtos") == id_prod)
                .get_column("unid")
                .drop_nulls()
                .cast(pl.Utf8)
            )
            unidades = (
                unidades_s.unique().to_list() if not unidades_s.is_empty() else []
            )
        except Exception:
            unidades = []

        self.combo_unid_ref.clear()
        if unidades:
            self.combo_unid_ref.addItems(sorted(unidades))
            self.combo_unid_ref.setEnabled(True)
            self.btn_apply_unid_ref.setEnabled(True)
        else:
            self.combo_unid_ref.setEnabled(False)
            self.btn_apply_unid_ref.setEnabled(False)
    def _apply_unid_ref_to_all(self) -> None:
        id_prod = getattr(self, "_current_selected_id_produto", None)
        nova_unid = self.combo_unid_ref.currentText()
        if not id_prod or not nova_unid or self._conversion_df_full.is_empty():
            return

        # Determinar o preco medio da nova unidade de referencia
        df_prod = self._conversion_df_full.filter(pl.col("id_produtos") == id_prod)
        row_ref = df_prod.filter(pl.col("unid") == nova_unid)

        novo_preco_ref = None
        if not row_ref.is_empty():
            val = row_ref.get_column("preco_medio")[0]
            if val is not None:
                try:
                    novo_preco_ref = float(val)
                except Exception:
                    pass

        # Atualizar unid_ref para as linhas do produto
        self._conversion_df_full = self._conversion_df_full.with_columns(
            pl.when(pl.col("id_produtos") == id_prod)
            .then(pl.lit(nova_unid))
            .otherwise(pl.col("unid_ref"))
            .alias("unid_ref")
        )
        self._conversion_df_full = self._conversion_df_full.with_columns(
            [
                pl.when(pl.col("id_produtos") == id_prod)
                .then(pl.lit(True))
                .otherwise(
                    pl.col("unid_ref_manual")
                    .cast(pl.Boolean, strict=False)
                    .fill_null(False)
                )
                .alias("unid_ref_manual"),
                pl.when(pl.col("id_produtos") == id_prod)
                .then(pl.lit(False))
                .otherwise(
                    pl.col("fator_manual")
                    .cast(pl.Boolean, strict=False)
                    .fill_null(False)
                )
                .alias("fator_manual"),
            ]
        )

        # Recalcular fatores de conversao das unidades relativas ao novo preco alvo
        if novo_preco_ref is not None and novo_preco_ref > 0:
            self._conversion_df_full = self._conversion_df_full.with_columns(
                pl.when(pl.col("id_produtos") == id_prod)
                .then(
                    pl.when(pl.col("preco_medio").is_not_null())
                    .then(pl.col("preco_medio").cast(pl.Float64) / novo_preco_ref)
                    .otherwise(1.0)
                )
                .otherwise(pl.col("fator"))
                .alias("fator")
            )
        else:
            # Caso a nova unidade selecionada nao tenha preco medio valido, forcamos fator 1.0 para todo o produto
            self._conversion_df_full = self._conversion_df_full.with_columns(
                pl.when(pl.col("id_produtos") == id_prod)
                .then(pl.lit(1.0))
                .otherwise(pl.col("fator"))
                .alias("fator")
            )

        # Salvar as alteracoes matematicas
        if self._conversion_file_path:
            (
                self._preparar_dataframe_para_salvar_conversao(
                    self._conversion_df_full
                ).write_parquet(self._conversion_file_path)
            )

        self.status.showMessage(
            f"Unidade {nova_unid} e fatores recalculados aplicados para {id_prod}."
        )
        self.atualizar_aba_conversao()
        self._marcar_recalculo_conversao_pendente(
            "Clique em 'Recalcular fatores' ou mude de tela."
        )
    def recalcular_derivados_conversao(self, show_popup: bool = True) -> None:
        cnpj = self.state.current_cnpj
        if not cnpj:
            self.show_error(
                "CNPJ nao selecionado", "Selecione um CNPJ antes de recalcular."
            )
            return
        if self._recalculando_conversao:
            return
        if not self._conversion_recalc_pending and show_popup:
            self.status.showMessage("Nao ha recalculo pendente na aba Conversao.")
            return

        self._recalculando_conversao = True

        def _on_success(ok) -> None:
            self._recalculando_conversao = False
            resumo = self.servico_agregacao.resumo_tempos()
            if ok:
                self._limpar_recalculo_conversao_pendente()
                self.atualizar_aba_mov_estoque()
                self.atualizar_aba_mensal()
                self.atualizar_aba_anual()
                self.atualizar_aba_nfe_entrada()
                self.atualizar_aba_id_agrupados()
                self.refresh_file_tree(cnpj)
                self.status.showMessage(
                    "Conversao aplicada; mov_estoque, mensal e anual recalculadas."
                    + (f" {resumo}" if resumo else "")
                )
                if show_popup:
                    self.show_info(
                        "Conversao aplicada",
                        "Fatores salvos; mov_estoque, mensal e anual foram recalculadas."
                        + (f"\n\nTempos: {resumo}" if resumo else ""),
                    )
            else:
                self.status.showMessage("Falha ao recalcular derivados da conversao.")
                if show_popup:
                    self.show_error(
                        "Falha no recalculo",
                        "Nao foi possivel recalcular mov_estoque, mensal e anual a partir da conversao.",
                    )

        def _on_failure(mensagem: str) -> None:
            self._recalculando_conversao = False
            self.status.showMessage(
                f"Erro ao recalcular derivados da conversao: {mensagem}"
            )
            if show_popup:
                self.show_error("Falha no recalculo", mensagem)

        iniciado = self._executar_em_worker(
            self.servico_agregacao.recalcular_mov_estoque,
            cnpj,
            mensagem_inicial="Recalculando mov_estoque, mensal e anual...",
            on_success=_on_success,
            on_failure=_on_failure,
        )
        if not iniciado:
            self._recalculando_conversao = False
    def _enriquecer_dataframe_conversao(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df
        if not {"id_agrupado", "unid", "unid_ref"}.issubset(set(df.columns)):
            return df

        df_base = df.drop(["preco_medio_ref", "fator_calculado"], strict=False)
        ref_price = (
            df_base.filter(
                pl.col("unid").cast(pl.Utf8, strict=False)
                == pl.col("unid_ref").cast(pl.Utf8, strict=False)
            )
            .group_by("id_agrupado")
            .agg(
                pl.col("preco_medio")
                .cast(pl.Float64, strict=False)
                .drop_nulls()
                .mean()
                .alias("preco_medio_ref")
            )
        )
        df_enriquecido = df_base.join(
            ref_price, on="id_agrupado", how="left"
        ).with_columns(
            pl.when(pl.col("preco_medio_ref").cast(pl.Float64, strict=False) > 0)
            .then(
                pl.col("preco_medio").cast(pl.Float64, strict=False)
                / pl.col("preco_medio_ref").cast(pl.Float64, strict=False)
            )
            .otherwise(1.0)
            .alias("fator_calculado")
        )

        colunas = list(df_enriquecido.columns)
        if "fator" in colunas:
            novas = [c for c in ["preco_medio_ref", "fator_calculado"] if c in colunas]
            for nome in novas:
                colunas.remove(nome)
            idx_fator = colunas.index("fator")
            for deslocamento, nome in enumerate(novas, start=1):
                colunas.insert(idx_fator + deslocamento, nome)
            df_enriquecido = df_enriquecido.select(colunas)

        return df_enriquecido
    def _montar_descricoes_exibicao_por_grupo(
        self, df_descricoes: pl.DataFrame
    ) -> pl.DataFrame:
        """
        Normaliza a lista exibida na aba de conversao sem alterar a
        fonte canonica persistida em parquet.
        """
        if df_descricoes.is_empty() or not {"id_agrupado", "descricao_item"}.issubset(
            set(df_descricoes.columns)
        ):
            return pl.DataFrame()

        return (
            df_descricoes.select(
                [
                    pl.col("id_agrupado").cast(pl.Utf8, strict=False),
                    pl.col("descricao_item").cast(pl.Utf8, strict=False),
                ]
            )
            .with_columns(
                pl.col("descricao_item")
                .fill_null("")
                .str.strip_chars()
                .alias("descricao_item")
            )
            .filter(pl.col("descricao_item") != "")
            .unique(subset=["id_agrupado", "descricao_item"])
            .sort(["id_agrupado", "descricao_item"], nulls_last=True)
            .group_by("id_agrupado")
            .agg(pl.col("descricao_item").alias("__lista_descricoes"))
            .with_columns(
                pl.col("__lista_descricoes")
                .list.join(" | ")
                .alias("lista_descricoes_produto")
            )
            .select(["id_agrupado", "lista_descricoes_produto"])
        )
    def _carregar_descr_padrao_canonico_conversao(self, cnpj: str) -> pl.DataFrame:
        """
        Garante que a aba use o descr_padrao atual do agrupamento.
        """
        arquivos_canonicos = [
            CNPJ_ROOT
            / cnpj
            / "analises"
            / "produtos"
            / f"produtos_agrupados_{cnpj}.parquet",
            CNPJ_ROOT / cnpj / "analises" / "produtos" / f"id_agrupados_{cnpj}.parquet",
            CNPJ_ROOT
            / cnpj
            / "analises"
            / "produtos"
            / f"produtos_final_{cnpj}.parquet",
        ]

        for caminho in arquivos_canonicos:
            if not caminho.exists():
                continue
            try:
                df_origem = self._carregar_dataset_ui(
                    caminho, columns=["id_agrupado", "descr_padrao"]
                )
            except Exception:
                continue

            if df_origem.is_empty() or not {"id_agrupado", "descr_padrao"}.issubset(
                set(df_origem.columns)
            ):
                continue

            return (
                df_origem.select(
                    [
                        pl.col("id_agrupado").cast(pl.Utf8, strict=False),
                        pl.col("descr_padrao")
                        .cast(pl.Utf8, strict=False)
                        .fill_null("")
                        .str.strip_chars()
                        .alias("descr_padrao_canonico"),
                    ]
                )
                .filter(pl.col("descr_padrao_canonico") != "")
                .unique(subset=["id_agrupado"], keep="first")
            )

        return pl.DataFrame()
    def _carregar_descricoes_canonicas_conversao(self, cnpj: str) -> pl.DataFrame:
        """
        Prefere a lista consolidada do ETL; so recorre ao fallback via
        produtos_final quando o schema antigo ainda nao foi regenerado.
        """
        arquivos_canonicos = [
            CNPJ_ROOT
            / cnpj
            / "analises"
            / "produtos"
            / f"produtos_agrupados_{cnpj}.parquet",
            CNPJ_ROOT / cnpj / "analises" / "produtos" / f"id_agrupados_{cnpj}.parquet",
        ]

        for caminho in arquivos_canonicos:
            if not caminho.exists():
                continue
            try:
                df_origem = self._carregar_dataset_ui(
                    caminho, columns=["id_agrupado", "lista_descricoes"]
                )
            except Exception:
                continue

            if df_origem.is_empty() or not {"id_agrupado", "lista_descricoes"}.issubset(
                set(df_origem.columns)
            ):
                continue

            df_descricoes = self._montar_descricoes_exibicao_por_grupo(
                df_origem.select(
                    [
                        pl.col("id_agrupado").cast(pl.Utf8, strict=False),
                        pl.col("lista_descricoes")
                        .cast(pl.List(pl.Utf8), strict=False)
                        .alias("descricao_item"),
                    ]
                ).explode("descricao_item")
            )
            if not df_descricoes.is_empty():
                return df_descricoes

        return pl.DataFrame()
    def _reconstruir_descricoes_conversao_via_produtos_final(
        self, cnpj: str
    ) -> pl.DataFrame:
        """Fallback legado para CNPJs ainda nao regenerados."""
        arq_prod_final = (
            CNPJ_ROOT
            / cnpj
            / "analises"
            / "produtos"
            / f"produtos_final_{cnpj}.parquet"
        )
        if not arq_prod_final.exists():
            return pl.DataFrame()

        try:
            df_prod = self._carregar_dataset_ui(
                arq_prod_final,
                columns=[
                    "id_agrupado",
                    "descricao",
                    "descricao_final",
                    "lista_desc_compl",
                ],
            )
        except Exception:
            return pl.DataFrame()

        if df_prod.is_empty() or "id_agrupado" not in df_prod.columns:
            return pl.DataFrame()

        partes_descricoes: list[pl.DataFrame] = []
        if "descricao" in df_prod.columns:
            partes_descricoes.append(
                df_prod.select(
                    [
                        pl.col("id_agrupado").cast(pl.Utf8, strict=False),
                        pl.col("descricao")
                        .cast(pl.Utf8, strict=False)
                        .alias("descricao_item"),
                    ]
                )
            )
        if "descricao_final" in df_prod.columns:
            partes_descricoes.append(
                df_prod.select(
                    [
                        pl.col("id_agrupado").cast(pl.Utf8, strict=False),
                        pl.col("descricao_final")
                        .cast(pl.Utf8, strict=False)
                        .alias("descricao_item"),
                    ]
                )
            )
        if "lista_desc_compl" in df_prod.columns:
            partes_descricoes.append(
                df_prod.select(
                    [
                        pl.col("id_agrupado").cast(pl.Utf8, strict=False),
                        pl.col("lista_desc_compl")
                        .cast(pl.List(pl.Utf8), strict=False)
                        .alias("descricao_item"),
                    ]
                ).explode("descricao_item")
            )

        if not partes_descricoes:
            return pl.DataFrame()

        return self._montar_descricoes_exibicao_por_grupo(
            pl.concat(partes_descricoes, how="vertical_relaxed")
        )
    def _preparar_dataframe_para_salvar_conversao(
        self, df: pl.DataFrame
    ) -> pl.DataFrame:
        """
        Remove colunas derivadas da UI antes de persistir fatores_conversao.
        """
        return df.drop(
            [
                "__row_id__",
                "preco_medio_ref",
                "fator_calculado",
                "lista_descricoes_produto",
                "descr_padrao_canonico",
            ],
            strict=False,
        )
    def _enriquecer_descricoes_conversao(
        self, cnpj: str, df: pl.DataFrame
    ) -> pl.DataFrame:
        if df.is_empty() or "id_agrupado" not in df.columns:
            return df

        df_descr_padrao = self._carregar_descr_padrao_canonico_conversao(cnpj)
        df_descricoes_base = self._carregar_descricoes_canonicas_conversao(cnpj)
        if df_descricoes_base.is_empty():
            df_descricoes_base = (
                self._reconstruir_descricoes_conversao_via_produtos_final(cnpj)
            )
        if df_descricoes_base.is_empty() and df_descr_padrao.is_empty():
            return df

        df_out = df.drop(
            ["lista_descricoes_produto", "descr_padrao_canonico"], strict=False
        )
        if not df_descr_padrao.is_empty():
            df_out = df_out.join(
                df_descr_padrao, on="id_agrupado", how="left"
            ).with_columns(
                pl.coalesce(
                    [pl.col("descr_padrao_canonico"), pl.col("descr_padrao")]
                ).alias("descr_padrao")
            )
        if not df_descricoes_base.is_empty():
            df_out = df_out.join(
                df_descricoes_base, on="id_agrupado", how="left"
            ).with_columns(
                pl.col("lista_descricoes_produto")
                .cast(pl.Utf8, strict=False)
                .fill_null("")
                .alias("lista_descricoes_produto")
            )
        else:
            df_out = df_out.with_columns(pl.lit("").alias("lista_descricoes_produto"))

        colunas = list(df_out.columns)
        if "descr_padrao" in colunas and "lista_descricoes_produto" in colunas:
            colunas.remove("lista_descricoes_produto")
            idx_descr = colunas.index("descr_padrao")
            colunas.insert(idx_descr + 1, "lista_descricoes_produto")
            df_out = df_out.select(colunas)
        return df_out
    def atualizar_aba_conversao(self) -> None:
        """Carrega os fatores de conversao do CNPJ atual."""
        cnpj = self.state.current_cnpj
        if not cnpj:
            self._atualizar_titulo_aba_conversao()
            return

        pasta_produtos = CNPJ_ROOT / cnpj / "analises" / "produtos"
        arq_conversao = pasta_produtos / f"fatores_conversao_{cnpj}.parquet"

        if not arq_conversao.exists():
            self.conversion_model.set_dataframe(pl.DataFrame())
            self._conversion_df_full = pl.DataFrame()
            self._conversion_file_path = None
            self._atualizar_titulo_aba_conversao()
            return

        def _processar_conversao():
            df = self._carregar_dataset_ui(arq_conversao)
            if "fator_manual" not in df.columns:
                df = df.with_columns(pl.lit(False).alias("fator_manual"))
            if "unid_ref_manual" not in df.columns:
                df = df.with_columns(pl.lit(False).alias("unid_ref_manual"))
            df = self._enriquecer_dataframe_conversao(df)
            df = self._enriquecer_descricoes_conversao(cnpj, df)
            df = df.with_row_index("__row_id__")
            return df

        def _finalizar_carga_conversao(df: pl.DataFrame | None) -> None:
            if df is None:
                self._conversion_df_full = pl.DataFrame()
                self.conversion_model.set_dataframe(pl.DataFrame())
                self.lbl_conversion_status.setText(
                    "Tabela de conversao nao encontrada para este CNPJ."
                )
                return
            self._conversion_df_full = df
            self._conversion_file_path = arq_conversao
            self._limpar_recalculo_conversao_pendente()
            self._reset_table_resize_flag("conversao")
            id_atual = self.conv_filter_id.currentText()
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
            self._popular_combo_texto(
                self.conv_filter_id, [str(i) for i in ids], id_atual, ""
            )
            self.aplicar_filtros_conversao()
            self._atualizar_titulo_aba_conversao()

        self.status.showMessage("⏳ Carregando fatores de conversão...")
        worker = ServiceTaskWorker(_processar_conversao)
        worker.finished_ok.connect(_finalizar_carga_conversao)
        worker.failed.connect(
            lambda msg: self.show_error(
                "Erro de Carregamento", f"Falha ao carregar conversão: {msg}"
            )
        )
        worker.start()
        if not hasattr(self, "_active_load_workers") or not isinstance(
            self._active_load_workers, set
        ):
            self._active_load_workers = set()
        self._active_load_workers.add(worker)
        worker.finished.connect(lambda: self._active_load_workers.discard(worker))
        worker.finished.connect(worker.deleteLater)
    def aplicar_filtros_conversao(self) -> None:
        if self._conversion_df_full.is_empty():
            return

        try:
            total_bruto = self._conversion_df_full.height
            df_vis = self._conversion_df_full

            mostrar_unidade_unica = getattr(self, "chk_show_single_unit", None)
            mostrar_unidade_unica = bool(
                mostrar_unidade_unica and mostrar_unidade_unica.isChecked()
            )
            if (not mostrar_unidade_unica) and {"id_produtos", "unid"}.issubset(
                set(df_vis.columns)
            ):
                df_multi_unid = (
                    df_vis.group_by("id_produtos")
                    .agg(
                        pl.col("unid")
                        .cast(pl.Utf8, strict=False)
                        .drop_nulls()
                        .n_unique()
                        .alias("qtd_unid")
                    )
                    .filter(pl.col("qtd_unid") > 1)
                    .select("id_produtos")
                )
                if df_multi_unid.height > 0:
                    df_vis = df_vis.join(df_multi_unid, on="id_produtos", how="inner")
                else:
                    df_vis = pl.DataFrame(schema=df_vis.schema)

            id_agrupado = self.conv_filter_id.currentText().strip()
            descr = self.conv_filter_desc.text().strip().lower()

            if id_agrupado and "id_agrupado" in df_vis.columns:
                df_vis = df_vis.filter(
                    pl.col("id_agrupado")
                    .cast(pl.Utf8, strict=False)
                    .fill_null("")
                    .str.contains(id_agrupado, literal=True)
                )
            if descr and "descr_padrao" in df_vis.columns:
                df_vis = df_vis.filter(
                    pl.col("descr_padrao")
                    .cast(pl.Utf8, strict=False)
                    .fill_null("")
                    .str.to_lowercase()
                    .str.contains(descr, literal=True)
                )

            self._updating_conversion_model = True
            self.conversion_model.set_dataframe(df_vis)
            self._updating_conversion_model = False
            self._resize_table_once(self.conversion_table, "conversao")
            self._aplicar_preferencias_tabela(
                "conversao", self.conversion_table, self.conversion_model
            )
            for col_oculta in ["__row_id__", "fator_manual", "unid_ref_manual"]:
                if col_oculta in self.conversion_model.dataframe.columns:
                    col_idx = self.conversion_model.dataframe.columns.index(col_oculta)
                    self.conversion_table.setColumnHidden(col_idx, True)
            self._salvar_preferencias_tabela(
                "conversao", self.conversion_table, self.conversion_model
            )
            self._atualizar_titulo_aba_conversao(df_vis.height, total_bruto)
        except Exception as e:
            self._updating_conversion_model = False
            QMessageBox.warning(
                self, "Erro", f"Erro ao filtrar fatores de conversao: {e}"
            )
    def _on_conversion_model_changed(self, top_left, bottom_right, _roles) -> None:
        if self._updating_conversion_model:
            return
        if self._conversion_file_path is None or self._conversion_df_full.is_empty():
            return

        df_vis = self.conversion_model.dataframe
        if df_vis.is_empty() or "__row_id__" not in df_vis.columns:
            return

        col_ini = top_left.column()
        col_fim = bottom_right.column()
        touched_cols = set(df_vis.columns[col_ini : col_fim + 1])
        if not ("fator" in touched_cols or "unid_ref" in touched_cols):
            return

        row_ini = max(0, top_left.row())
        row_fim = min(df_vis.height - 1, bottom_right.row())

        updates_row_id = []
        updates_fator = []
        updates_unid_ref = []
        updates_fator_manual = []
        updates_unid_ref_manual = []

        for r in range(row_ini, row_fim + 1):
            row_id = df_vis.item(r, df_vis.columns.index("__row_id__"))

            # Fator
            try:
                fator = df_vis.item(r, df_vis.columns.index("fator"))
                fator_val = None if fator is None else float(fator)
            except Exception:
                fator_val = None

            # Unidade de Referencia
            try:
                unid_ref = df_vis.item(r, df_vis.columns.index("unid_ref"))
                unid_ref_val = None if unid_ref is None else str(unid_ref).strip()
            except Exception:
                unid_ref_val = None

            updates_row_id.append(int(row_id))
            updates_fator.append(fator_val)
            updates_unid_ref.append(unid_ref_val)
            updates_fator_manual.append("fator" in touched_cols)
            updates_unid_ref_manual.append("unid_ref" in touched_cols)

        if not updates_row_id:
            return

        df_updates = pl.DataFrame(
            {
                "__row_id__": updates_row_id,
                "fator_editado": updates_fator,
                "unid_ref_editado": updates_unid_ref,
                "fator_manual_editado": updates_fator_manual,
                "unid_ref_manual_editado": updates_unid_ref_manual,
            }
        )

        self._conversion_df_full = (
            self._conversion_df_full.join(df_updates, on="__row_id__", how="left")
            .with_columns(
                [
                    pl.coalesce([pl.col("fator_editado"), pl.col("fator")]).alias(
                        "fator"
                    ),
                    pl.coalesce([pl.col("unid_ref_editado"), pl.col("unid_ref")]).alias(
                        "unid_ref"
                    ),
                    pl.when(pl.col("fator_manual_editado").fill_null(False))
                    .then(pl.lit(True))
                    .otherwise(
                        pl.col("fator_manual")
                        .cast(pl.Boolean, strict=False)
                        .fill_null(False)
                    )
                    .alias("fator_manual"),
                    pl.when(pl.col("unid_ref_manual_editado").fill_null(False))
                    .then(pl.lit(True))
                    .otherwise(
                        pl.col("unid_ref_manual")
                        .cast(pl.Boolean, strict=False)
                        .fill_null(False)
                    )
                    .alias("unid_ref_manual"),
                ]
            )
            .drop(
                [
                    "fator_editado",
                    "unid_ref_editado",
                    "fator_manual_editado",
                    "unid_ref_manual_editado",
                ]
            )
        )
        self._conversion_df_full = self._enriquecer_dataframe_conversao(
            self._conversion_df_full
        )

        (
            self._preparar_dataframe_para_salvar_conversao(
                self._conversion_df_full
            ).write_parquet(self._conversion_file_path)
        )
        self.status.showMessage(
            "Fator e/ou unidade de referencia atualizados e salvos."
        )
        self._marcar_recalculo_conversao_pendente(
            "Clique em 'Recalcular fatores' ou mude de tela."
        )
    def _atualizar_titulo_aba_conversao(
        self, visiveis: int | None = None, total: int | None = None
    ) -> None:
        if not hasattr(self, "tabs") or not hasattr(self, "tab_conversao"):
            return
        idx = self.tabs.indexOf(self.tab_conversao)
        if idx < 0:
            return
        if visiveis is None or total is None:
            self.tabs.setTabText(idx, "Conversao")
            return
        self.tabs.setTabText(idx, f"Conversao ({visiveis}/{total})")
    def exportar_conversao_excel(self) -> None:
        """Exporta os fatores de conversao para Excel para edicao."""
        df = self._dataframe_colunas_visiveis(
            self.conversion_table, self.conversion_model
        )
        df = df.drop(
            [
                "__row_id__",
                "preco_medio_ref",
                "fator_calculado",
                "fator_manual",
                "unid_ref_manual",
            ],
            strict=False,
        )
        if df.is_empty():
            QMessageBox.information(self, "Aviso", "Nao hA dados para exportar.")
            return

        path, _ = QFileDialog.getSaveFileName(
            self,
            "Salvar Excel",
            f"fator_conversao_{self.state.current_cnpj}.xlsx",
            "Excel (*.xlsx)",
        )
        if not path:
            return

        try:
            df.write_excel(path)
            QMessageBox.information(
                self, "Sucesso", f"Arquivo salvo com sucesso:\n{path}"
            )
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Erro ao exportar: {e}")
    @safe_slot
    def importar_conversao_excel(self) -> None:
        """Importa fatores de conversao do Excel, sobrescrevendo o Parquet."""
        cnpj = self.state.current_cnpj
        if not cnpj:
            return
        try:
            cnpj = validate_cnpj(cnpj)
        except ValueError as exc:
            QMessageBox.warning(self, "CNPJ invalido", str(exc))
            return

        path, _ = QFileDialog.getOpenFileName(self, "Abrir Excel", "", "Excel (*.xlsx)")
        if not path:
            return
        try:
            path = validate_path_exists(path)
        except ValueError as exc:
            QMessageBox.warning(self, "Arquivo invalido", str(exc))
            return

        try:
            df_excel = pl.read_excel(path)
            # Validacao conforme documentacao: id_produtos, descr_padrao, unid, unid_ref, fator
            mapping = {
                "id_produtos": "id_produtos",
                "descr_padrao": "descr_padrao",
                "unid": "unid",
                "unid_ref": "unid_ref",
                "fator": "fator",
            }
            cols_obrigatorias = list(mapping.keys())
            if not all(c in df_excel.columns for c in cols_obrigatorias):
                raise ValueError(f"O Excel deve conter as colunas: {cols_obrigatorias}")

            pasta_produtos = CNPJ_ROOT / cnpj / "analises" / "produtos"
            nome_saida = f"fatores_conversao_{cnpj}.parquet"

            # Renomeia para colunas internas e garante tipos
            df_imp = df_excel.select(cols_obrigatorias).rename(
                {c: mapping[c] for c in cols_obrigatorias}
            )
            df_imp = df_imp.with_columns(
                [
                    pl.col("fator").cast(pl.Float64),
                    pl.lit(True).alias("fator_manual"),
                    pl.lit(True).alias("unid_ref_manual"),
                ]
            )

            df_imp.write_parquet(pasta_produtos / nome_saida)
            self.atualizar_aba_conversao()
            self._marcar_recalculo_conversao_pendente(
                "Clique em 'Recalcular fatores' ou mude de tela."
            )
            QMessageBox.information(
                self, "Sucesso", "Fatores de conversao importados com sucesso."
            )
        except Exception as e:
            QMessageBox.critical(self, "Erro", f"Erro ao importar: {e}")
