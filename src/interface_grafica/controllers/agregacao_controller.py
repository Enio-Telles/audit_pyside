from __future__ import annotations

import re
from pathlib import Path

import polars as pl
from PySide6.QtWidgets import QInputDialog, QMessageBox

from interface_grafica.services.parquet_service import FilterCondition
from utilitarios.text import remove_accents


class AgregacaoControllerMixin:
    def open_editable_aggregation_table(self) -> None:
        if not self.state.current_cnpj:
            self.show_error("CNPJ nao selecionado", "Selecione um CNPJ na lista.")
            return
        try:
            target = self.servico_agregacao.carregar_tabela_editavel(
                self.state.current_cnpj
            )
            self._aggregation_file_path = target
            self._aggregation_filters = []
            self._aggregation_results_filters = []
            self._aggregation_relational_mode = None
            self._aggregation_results_relational_mode = None
            self._load_aggregation_table()
            self.recarregar_historico_agregacao(self.state.current_cnpj)
        except Exception as exc:
            self.show_error("Falha ao abrir tabela editAvel", str(exc))
            return

        self.tabs.setCurrentIndex(
            2
        )  # Switch to Agregacao tab (0-indexed: Consulta, SQL, Agregacao, Logs)
    def _abrir_tabela_agrupada(self) -> None:
        self.open_editable_aggregation_table()
    def _desfazer_agregacao(self) -> None:
        self.aggregation_table_model.clear_checked()
        self.results_table_model.clear_checked()
        self.status.showMessage("Selecao de agregacao limpa.")
    def _obter_ids_agrupados_para_reversao(self) -> list[str]:
        rows = self.results_table_model.get_checked_rows()
        if not rows:
            selecao = self.results_table.selectionModel()
            if selecao is not None:
                df = self.results_table_model.get_dataframe()
                rows = [
                    df.row(index.row(), named=True)
                    for index in selecao.selectedRows()
                    if 0 <= index.row() < df.height
                ]

        ids: list[str] = []
        vistos: set[str] = set()
        for row in rows:
            valor = str(row.get("id_agrupado") or "").strip()
            if not valor or valor in vistos:
                continue
            vistos.add(valor)
            ids.append(valor)
        return ids
    def reverter_agregacao(self) -> None:
        if not self.state.current_cnpj:
            self.show_error(
                "CNPJ nao selecionado",
                "Selecione um CNPJ antes de reverter agrupamentos.",
            )
            return

        ids_reversao = self._obter_ids_agrupados_para_reversao()
        if not ids_reversao:
            self.show_error(
                "Selecao insuficiente",
                "Marque ou selecione na tabela inferior o agrupamento que deve ser revertido.",
            )
            return

        mensagem = (
            "Isso vai restaurar os grupos de origem do(s) agrupamento(s) selecionado(s):\n"
            + "\n".join(ids_reversao)
            + "\n\nProsseguir?"
        )
        if (
            QMessageBox.question(self, "Reverter agrupamento", mensagem)
            != QMessageBox.StandardButton.Yes
        ):
            return

        try:
            resultados = [
                self.servico_agregacao.reverter_agrupamento(
                    self.state.current_cnpj, id_agrupado
                )
                for id_agrupado in ids_reversao
            ]
            self.atualizar_tabelas_agregacao()
            self.recarregar_historico_agregacao(self.state.current_cnpj)
            self.atualizar_aba_id_agrupados()
            self.refresh_logs()

            total_restaurado = sum(
                int(item.get("qtd_grupos_restaurados", 0)) for item in resultados
            )
            self.show_info(
                "Agrupamento revertido",
                f"Foram restaurados {total_restaurado} grupos a partir de {len(ids_reversao)} agrupamento(s).",
            )
        except Exception as exc:
            self.show_error("Erro ao reverter agrupamento", str(exc))
    def reverter_mapa_manual_ui(self) -> None:
        cnpj = self._obter_cnpj_valido()
        if cnpj is None:
            return

        try:
            snapshots = self.servico_agregacao.listar_snapshots_mapa_manual(cnpj)
        except Exception as exc:
            self.show_error("Erro", f"Falha ao listar snapshots: {exc}")
            return

        if not snapshots:
            self.show_error(
                "Nenhum snapshot", "Nao ha snapshots disponiveis para este CNPJ."
            )
            return

        items = [Path(p).name for p in snapshots]
        name, ok = QInputDialog.getItem(
            self,
            "Restaurar snapshot do mapa manual",
            "Escolha snapshot:",
            items,
            0,
            False,
        )
        if not ok or not name:
            return

        if (
            QMessageBox.question(
                self,
                "Confirmar restauração",
                f"Restaurar snapshot {name}?",
            )
            != QMessageBox.StandardButton.Yes
        ):
            return

        try:
            restored = self.servico_agregacao.reverter_mapa_manual(
                cnpj, snapshot_name=name
            )
        except Exception as exc:
            self.show_error("Erro", f"Falha ao restaurar snapshot: {exc}")
            return

        if not restored:
            self.show_error(
                "Falha", "Nao foi possivel restaurar o snapshot selecionado."
            )
            return

        def _on_success(resultado) -> None:
            ok = bool(resultado)
            if ok:
                self.atualizar_tabelas_agregacao()
                self.recarregar_historico_agregacao(cnpj)
                self.refresh_logs()
                self.show_info(
                    "Restaurado", f"Snapshot {name} restaurado e reprocessado."
                )
            else:
                self.show_error(
                    "Restaurado", "Snapshot restaurado, mas reprocessamento falhou."
                )

        def _on_failure(mensagem: str) -> None:
            self.show_error("Erro no reprocessamento", mensagem)

        started = self._executar_em_worker(
            self.servico_agregacao.recalcular_produtos_final,
            cnpj,
            mensagem_inicial="Reprocessando produtos_final...",
            on_success=_on_success,
            on_failure=_on_failure,
        )

        if not started:
            # Worker ocupado; still refresh UI state minimally
            self.atualizar_tabelas_agregacao()
            self.recarregar_historico_agregacao(cnpj)
            self.refresh_logs()
            self.show_info(
                "Restaurado",
                f"Snapshot {name} restaurado. Reprocessamento nao iniciado (worker ocupado).",
            )
    def _load_aggregation_table(self) -> None:
        cnpj = self.state.current_cnpj
        if not cnpj:
            return
        if self._aggregation_file_path is None:
            return
        self._aggregation_file_path = self.servico_agregacao.carregar_tabela_editavel(
            cnpj
        )
        df = self.parquet_service.load_dataset(
            self._aggregation_file_path, self._aggregation_filters or []
        )
        df = self._aplicar_modo_relacional_agregacao_df(
            df, self._aggregation_relational_mode
        )
        self.aggregation_table_model.set_dataframe(df)
        self._resize_table_once(self.aggregation_table_view, "agregacao_top")
        if not self._aplicar_preferencias_tabela(
            "agregacao_top", self.aggregation_table, self.aggregation_table_model
        ):
            self._aplicar_perfil_agregacao(
                "agregacao_top",
                self.aggregation_table,
                self.aggregation_table_model,
                self.top_profile.currentText(),
            )
    def execute_aggregation(self) -> None:
        if not self.state.current_cnpj:
            self.show_error(
                "CNPJ nao selecionado", "Selecione um CNPJ antes de agregar."
            )
            return

        rows_top = self.aggregation_table_model.get_checked_rows()
        rows_bottom = self.results_table_model.get_checked_rows()

        # Merge and de-duplicate
        combined = []
        seen = set()
        for r in rows_top + rows_bottom:
            key = str(r.get("id_agrupado") or "").strip()
            if not key:
                key = str(r.get("chave_produto") or "").strip()
            if not key:
                key = str(r.get("chave_item") or "").strip()
            if not key:
                key = (
                    str(r.get("descr_padrao") or r.get("descricao") or "")
                    .strip()
                    .upper()
                )
            if key not in seen:
                seen.add(key)
                combined.append(r)

        if len(combined) < 2:
            self.show_error(
                "Selecao insuficiente",
                "Marque pelo menos duas linhas com 'Visto' (pode ser em ambas as tabelas) para agregar.",
            )
            return

        try:
            # Novo: Passar lista de IDs agrupados para o servico
            ids_selecionados = [
                str(r.get("id_agrupado") or "")
                for r in combined
                if r.get("id_agrupado")
            ]

            if len(ids_selecionados) < 2:
                self.show_error(
                    "Selecao insuficiente",
                    "Nao foi possivel identificar IDs unicos para os grupos selecionados.",
                )
                return

            self.servico_agregacao.agregar_linhas(
                cnpj=self.state.current_cnpj,
                ids_agrupados_selecionados=ids_selecionados,
            )
            # Update the tables to reflect the changes
            self.atualizar_tabelas_agregacao()
            self.recarregar_historico_agregacao(self.state.current_cnpj)
            self.refresh_logs()

            self.show_info(
                "Agregacao concluida",
                f"As {len(combined)} descricoes foram unificadas.",
            )
        except Exception as e:
            import traceback

            from utilitarios.perf_monitor import registrar_evento_performance

            registrar_evento_performance(
                "main_window.agregacao_erro",
                contexto={"erro": str(e), "traceback": traceback.format_exc()},
                status="error",
            )
            self.show_error(
                "Erro na agregacao",
                "Ocorreu um erro interno ao agregar. Consulte os logs internos para mais detalhes.",
            )

            # Clear checks and reload top table
            self.aggregation_table_model.clear_checked()
            self.results_table_model.clear_checked()
            self.open_editable_aggregation_table()
    def apply_quick_filters(self) -> None:
        idx = self.tabs.currentIndex()
        if idx == 1:  # Consulta
            fields = {
                "descricao_normalizada": self.qf_norm.text().strip(),
                "descricao": self.qf_desc.text().strip(),
                "ncm_padrao": self.qf_ncm.text().strip(),
                "cest_padrao": self.qf_cest.text().strip(),
            }
        elif idx == 3:  # Agregacao (Index 3 is "Agregacao", Index 2 is "SQL")
            fields = {
                "descricao_normalizada": self.aqf_norm.text().strip(),
                "descricao": self.aqf_desc.text().strip(),
                "ncm_padrao": self.aqf_ncm.text().strip(),
                "cest_padrao": self.aqf_cest.text().strip(),
            }
        else:
            return

        def split_terms(value: str) -> list[str]:
            texto = (value or "").strip()
            if not texto:
                return []
            # Permite buscar varios trechos no mesmo campo.
            # Ex.: "buch 18", "buch;18" ou "buch, 18".
            partes = re.split(r"[;,]+|\s{2,}", texto)
            if len(partes) == 1 and " " in texto:
                partes = texto.split()
            return [p.strip() for p in partes if p and p.strip()]

        # Mapas de colunas equivalentes por tipo de filtro rapido.
        # Inclui colunas usadas na aba de Agregacao (ex.: descr_padrao).
        alternatives = {
            "ncm_padrao": ["ncm_padrao", "NCM_padrao", "lista_ncm", "ncm_final", "ncm"],
            "cest_padrao": [
                "cest_padrao",
                "CEST_padrao",
                "lista_cest",
                "cest_final",
                "cest",
            ],
            "descricao_normalizada": [
                "descricao_normalizada",
                "descricao",
                "descr_norm",
                "descr_padrao",
                "descricao_final",
            ],
            "descricao": [
                "descricao",
                "lista_descricoes",
                "lista_desc_compl",
                "lista_itens_agrupados",
                "descr",
                "descr_padrao",
                "descricao_final",
            ],
        }

        # Remove filtros rapidos antigos (inclusive quando ficaram com nome de coluna "alias").
        quick_target_cols = set(fields.keys())
        for key in fields.keys():
            quick_target_cols.update(alternatives.get(key, []))

        # Na aba de Agregacao, o filtro rapido deve ser deterministico:
        # substitui totalmente os filtros anteriores para evitar "filtros ocultos".
        if idx == 3:
            new_filters = []
        else:
            new_filters = [
                f
                for f in (self.state.filters or [])
                if f.column not in quick_target_cols
            ]

        available_columns = self.state.all_columns or []
        if idx == 3 and self._aggregation_file_path is not None:
            try:
                available_columns = self.parquet_service.get_schema(
                    self._aggregation_file_path
                )
            except Exception:
                available_columns = list(self.aggregation_table_model.dataframe.columns)

        for col, val in fields.items():
            termos = split_terms(val)
            if termos:
                # Need to be flexible with column names as they might differ across files
                # We'll use the one present in the schema
                actual_col = col
                if available_columns:
                    # Match case-sensitive in alias map first.
                    if col in alternatives:
                        for alt in alternatives[col]:
                            if alt in available_columns:
                                actual_col = alt
                                break

                    # Fallback: match case/acento-insensitive
                    if actual_col not in available_columns:
                        target_clean = remove_accents(col).lower()
                        for c in available_columns:
                            if remove_accents(c).lower() == target_clean:
                                actual_col = c
                                break

                # Usa operador ASCII para evitar problemas de encoding no caminho UI -> servico.
                # Cada termo vira um filtro proprio; como os filtros sao encadeados,
                # a busca exige que todos os trechos estejam presentes.
                for termo in termos:
                    new_filters.append(
                        FilterCondition(
                            column=actual_col, operator="contem", value=termo
                        )
                    )

        if idx == 3:
            self._aggregation_filters = new_filters
            self._load_aggregation_table()
        else:
            self.state.filters = new_filters
            self.state.current_page = 1
            self.reload_table(update_main_view=True)
    def apply_aggregation_results_filters(self) -> None:
        if self.tabs.currentIndex() != 3:
            return

        fields = {
            "descricao_normalizada": self.bqf_norm.text().strip(),
            "descricao": self.bqf_desc.text().strip(),
            "ncm_padrao": self.bqf_ncm.text().strip(),
            "cest_padrao": self.bqf_cest.text().strip(),
        }

        def split_terms(value: str) -> list[str]:
            texto = (value or "").strip()
            if not texto:
                return []
            partes = re.split(r"[;,]+|\s{2,}", texto)
            if len(partes) == 1 and " " in texto:
                partes = texto.split()
            return [p.strip() for p in partes if p and p.strip()]

        alternatives = {
            "ncm_padrao": ["ncm_padrao", "NCM_padrao", "lista_ncm", "ncm_final", "ncm"],
            "cest_padrao": [
                "cest_padrao",
                "CEST_padrao",
                "lista_cest",
                "cest_final",
                "cest",
            ],
            "descricao_normalizada": [
                "descricao_normalizada",
                "descricao",
                "descr_norm",
                "descr_padrao",
                "descricao_final",
            ],
            "descricao": [
                "descricao",
                "lista_descricoes",
                "lista_desc_compl",
                "lista_itens_agrupados",
                "descr",
                "descr_padrao",
                "descricao_final",
            ],
        }

        new_filters: list[FilterCondition] = []
        cnpj = self.state.current_cnpj
        if not cnpj:
            self._aggregation_results_filters = []
            self.recarregar_historico_agregacao("")
            return

        path = self.servico_agregacao.caminho_tabela_agregadas(cnpj)
        available_columns = []
        if path.exists():
            try:
                available_columns = self.parquet_service.get_schema(path)
            except Exception:
                available_columns = list(self.results_table_model.dataframe.columns)

        for col, val in fields.items():
            termos = split_terms(val)
            if not termos:
                continue

            actual_col = col
            if available_columns:
                if col in alternatives:
                    for alt in alternatives[col]:
                        if alt in available_columns:
                            actual_col = alt
                            break
                if actual_col not in available_columns:
                    target_clean = remove_accents(col).lower()
                    for c in available_columns:
                        if remove_accents(c).lower() == target_clean:
                            actual_col = c
                            break

            for termo in termos:
                new_filters.append(
                    FilterCondition(column=actual_col, operator="contem", value=termo)
                )

        self._aggregation_results_filters = new_filters
        self.recarregar_historico_agregacao(cnpj)
    def _obter_linha_selecionada_tabela(
        self, table: QTableView, model: PolarsTableModel
    ) -> dict | None:
        df = model.get_dataframe()
        if df.is_empty():
            return None

        indice = table.currentIndex()
        if not indice.isValid():
            indices = (
                table.selectionModel().selectedIndexes()
                if table.selectionModel()
                else []
            )
            if not indices:
                return None
            indice = indices[0]

        linha = indice.row()
        if linha < 0 or linha >= df.height:
            return None
        return df.row(linha, named=True)
    def _resolver_coluna_agregacao(
        self, aliases: list[str], available_columns: list[str]
    ) -> str | None:
        for alias in aliases:
            if alias in available_columns:
                return alias

        normalizadas = {remove_accents(col).lower(): col for col in available_columns}
        for alias in aliases:
            chave = remove_accents(alias).lower()
            if chave in normalizadas:
                return normalizadas[chave]
        return None
    def _aplicar_modo_relacional_agregacao_df(
        self, df: pl.DataFrame, modo: str | None
    ) -> pl.DataFrame:
        if df.is_empty() or not modo:
            return df

        aliases_map = {
            "ncm": ["ncm_padrao", "NCM_padrao", "lista_ncm", "ncm_final", "ncm"],
            "cest": ["cest_padrao", "CEST_padrao", "lista_cest", "cest_final", "cest"],
            "gtin": ["gtin_padrao", "GTIN_padrao", "gtin", "cod_barra", "cod_barras"],
        }

        available_columns = list(df.columns)
        col_ncm = self._resolver_coluna_agregacao(aliases_map["ncm"], available_columns)
        col_cest = self._resolver_coluna_agregacao(
            aliases_map["cest"], available_columns
        )
        if not col_ncm or not col_cest:
            return df

        chaves: list[tuple[str, str]] = [("ncm", col_ncm), ("cest", col_cest)]
        if modo == "ncm_cest_gtin":
            col_gtin = self._resolver_coluna_agregacao(
                aliases_map["gtin"], available_columns
            )
            if not col_gtin:
                return df.head(0)
            chaves.append(("gtin", col_gtin))

        temporarias = [f"__rel_{nome}" for nome, _col in chaves]
        df_rel = df.with_row_index("__row_pos").with_columns(
            [
                pl.col(col)
                .cast(pl.Utf8, strict=False)
                .fill_null("")
                .str.strip_chars()
                .alias(f"__rel_{nome}")
                for nome, col in chaves
            ]
        )

        for coluna_tmp in temporarias:
            df_rel = df_rel.filter(pl.col(coluna_tmp) != "")

        if df_rel.is_empty():
            return df.head(0)

        df_repetidos = (
            df_rel.group_by(temporarias)
            .agg(pl.len().alias("__match_count"))
            .filter(pl.col("__match_count") >= 2)
        )

        if df_repetidos.is_empty():
            return df.head(0)

        return (
            df_rel.join(df_repetidos, on=temporarias, how="inner")
            .sort("__row_pos")
            .drop(["__row_pos", "__match_count", *temporarias], strict=False)
        )
    def _aplicar_filtro_relacional_agregacao(
        self, destino: str, include_gtin: bool
    ) -> None:
        if self.tabs.currentIndex() != 3:
            return

        modo = "ncm_cest_gtin" if include_gtin else "ncm_cest"

        if destino == "top":
            if self._aggregation_file_path is None:
                self.show_error(
                    "Tabela indisponivel",
                    "Abra a tabela de agregacao antes de aplicar o filtro.",
                )
                return
            self._aggregation_relational_mode = modo
            self._load_aggregation_table()
        else:
            cnpj = self.state.current_cnpj
            if not cnpj:
                self.show_error(
                    "CNPJ nao selecionado",
                    "Selecione um CNPJ antes de aplicar o filtro.",
                )
                return
            self._aggregation_results_relational_mode = modo
            self.recarregar_historico_agregacao(cnpj)

        rotulo = "NCM+CEST+GTIN iguais" if include_gtin else "NCM+CEST iguais"
        self.status.showMessage(f"Filtro relacional ativo: {rotulo}.")
    def clear_top_aggregation_filters(self) -> None:
        for widget in [self.aqf_norm, self.aqf_desc, self.aqf_ncm, self.aqf_cest]:
            widget.clear()
        self._aggregation_filters = []
        self._aggregation_relational_mode = None
        self._load_aggregation_table()
    def clear_bottom_aggregation_filters(self) -> None:
        for widget in [self.bqf_norm, self.bqf_desc, self.bqf_ncm, self.bqf_cest]:
            widget.clear()
        self._aggregation_results_filters = []
        self._aggregation_results_relational_mode = None
        cnpj = self.state.current_cnpj or ""
        self.recarregar_historico_agregacao(cnpj)
    def reprocessar_agregacao(self) -> None:
        cnpj = self.state.current_cnpj
        if not cnpj:
            return

        ret = QMessageBox.question(
            self,
            "Reprocessar",
            "Isso vai reprocessar a agregacao inteira: padroes, totais, produtos_final, tabelas _agr, precos medios, fatores de conversao, c170_xml, c176_xml, mov_estoque, mensal, anual e Produtos selecionados.\nProsseguir?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if ret == QMessageBox.StandardButton.No:
            return

        def _on_success(ok) -> None:
            self.status.showMessage("Pronto.")
            if ok:
                self.atualizar_tabelas_agregacao()
                self.recarregar_historico_agregacao(cnpj)
                self.atualizar_aba_conversao()
                self.atualizar_aba_mov_estoque()
                self.atualizar_aba_mensal()
                self.atualizar_aba_anual()
                self.atualizar_aba_nfe_entrada()
                self.atualizar_aba_id_agrupados()
                self.atualizar_aba_produtos_selecionados()
                self.refresh_file_tree(cnpj)
                if self.state.current_file is not None:
                    self.load_current_file(reset_columns=False)
                resumo = self.servico_agregacao.resumo_tempos()
                QMessageBox.information(
                    self,
                    "Sucesso",
                    "Reprocessamento concluido com sucesso."
                    + (f"\n\nTempos: {resumo}" if resumo else ""),
                )
            else:
                QMessageBox.warning(
                    self, "Aviso", "Nao foi possivel concluir o reprocessamento."
                )

        def _on_failure(mensagem: str) -> None:
            self.status.showMessage("Pronto.")
            QMessageBox.critical(self, "Erro", f"Erro ao reprocessar: {mensagem}")

        self._executar_em_worker(
            self.servico_agregacao.reprocessar_agregacao,
            cnpj,
            mensagem_inicial="Reprocessando agregacao, precos medios, fatores e tabelas derivadas...",
            on_success=_on_success,
            on_failure=_on_failure,
        )
    def recalcular_padroes_agregacao(self) -> None:
        """Invoca o servico para recalcular todos os padroes do CNPJ atual."""
        cnpj = self.state.current_cnpj
        if not cnpj:
            return

        self.reprocessar_agregacao()
    def recalcular_totais_agregacao(self) -> None:
        self.reprocessar_agregacao()
    def refazer_tabelas_agr_agregacao(self) -> None:
        self.reprocessar_agregacao()
    def refazer_fontes_produtos_agregacao(self) -> None:
        """Alias legado para refazer_tabelas_agr_agregacao."""
        self.refazer_tabelas_agr_agregacao()
    def recarregar_historico_agregacao(self, cnpj: str) -> None:
        """Carrega a tabela de descricoes agregadas no painel inferior."""
        try:
            path = self.servico_agregacao.carregar_tabela_editavel(cnpj)
            if path.exists():
                df_agregadas = self.parquet_service.load_dataset(
                    path, self._aggregation_results_filters or []
                )
                df_agregadas = self._aplicar_modo_relacional_agregacao_df(
                    df_agregadas, self._aggregation_results_relational_mode
                )
            else:
                df_agregadas = pl.DataFrame()
            self.results_table_model.set_dataframe(df_agregadas)
            self._resize_table_once(self.results_table_view, "agregacao_bottom")
            if not self._aplicar_preferencias_tabela(
                "agregacao_bottom", self.results_table, self.results_table_model
            ):
                self._aplicar_perfil_agregacao(
                    "agregacao_bottom",
                    self.results_table,
                    self.results_table_model,
                    self.bottom_profile.currentText(),
                )
        except Exception:
            self.results_table_model.set_dataframe(pl.DataFrame())
    def atualizar_tabelas_agregacao(self) -> None:
        """Atualiza os modelos das tabelas de agregacao."""
        cnpj = self.state.current_cnpj
        if not cnpj:
            return
        try:
            self._aggregation_file_path = (
                self.servico_agregacao.carregar_tabela_editavel(cnpj)
            )
            if self._aggregation_file_path.exists():
                self._load_aggregation_table()
        except FileNotFoundError as e:
            self.status.showMessage(f"Aviso: {e!s}")
            print(f"Aviso Agregacao: {e}")
        except Exception as e:
            self.show_error("Erro ao carregar agregacao", str(e))
