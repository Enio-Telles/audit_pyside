from __future__ import annotations

from PySide6.QtCore import Qt


class MainWindowSignalWiringCoreMixin:
    def _schedule_mov_filters(self) -> None:
        self._schedule_debounced("mov_filters", self.aplicar_filtros_mov_estoque)

    def _schedule_anual_filters(self) -> None:
        self._schedule_debounced("anual_filters", self.aplicar_filtros_aba_anual)

    def _schedule_mensal_filters(self) -> None:
        self._schedule_debounced("mensal_filters", self.aplicar_filtros_aba_mensal)

    def _schedule_periodos_filters(self) -> None:
        self._schedule_debounced("periodos_filters", self.aplicar_filtros_aba_periodos)

    def _schedule_nfe_entrada_filters(self) -> None:
        self._schedule_debounced("nfe_entrada_filters", self.aplicar_filtros_nfe_entrada)

    def _schedule_produtos_sel_filters(self) -> None:
        self._schedule_debounced("produtos_sel_filters", self.aplicar_filtros_produtos_selecionados)

    def _schedule_id_agrupados_filters(self) -> None:
        self._schedule_debounced("id_agrupados_filters", self.aplicar_filtros_id_agrupados)

    def _schedule_conversao_filters(self) -> None:
        self._schedule_debounced("conversao_filters", self.aplicar_filtros_conversao)

    def _schedule_consulta_quick_filters(self) -> None:
        self._schedule_debounced("consulta_quick_filters", self.apply_quick_filters)

    def _schedule_agregacao_bottom_filters(self) -> None:
        self._schedule_debounced("agregacao_bottom_filters", self.apply_aggregation_results_filters)

    def _schedule_sql_search(self) -> None:
        self._schedule_debounced("sql_result_search", self._filter_sql_results)

    def _connect_base_signals(self) -> None:
        self.btn_refresh_cnpjs.clicked.connect(self.refresh_cnpjs)
        self.btn_run_pipeline.clicked.connect(self.run_pipeline_for_input)
        self.btn_extrair_brutas.clicked.connect(self.extrair_tabelas_brutas)
        self.btn_processamento.clicked.connect(self.executar_processamento)
        self.btn_apagar_dados.clicked.connect(self.apagar_dados_cnpj)
        self.btn_apagar_cnpj.clicked.connect(self.apagar_cnpj_completo)
        self.btn_limpar_tudo.clicked.connect(self.limpar_tudo)
        self.btn_refresh_logs.clicked.connect(self.refresh_logs)
        self.cnpj_list.itemSelectionChanged.connect(self.on_cnpj_selected)
        self.file_tree.itemClicked.connect(self.on_file_activated)
        self.file_tree.itemDoubleClicked.connect(self.on_file_activated)
        self.btn_open_cnpj_folder.clicked.connect(self.open_cnpj_folder)
        self.btn_toggle_panel.toggled.connect(self._toggle_left_panel)
        self.tabs.currentChanged.connect(self._on_main_tab_changed)

    def _connect_consulta_agregacao_signals(self) -> None:
        self.btn_add_filter.clicked.connect(self.add_filter_from_form)
        self.btn_clear_filters.clicked.connect(self.clear_filters)
        self.btn_remove_filter.clicked.connect(self.remove_selected_filter)
        self.btn_choose_columns.clicked.connect(self.choose_columns)
        self.btn_apply_consulta_profile.clicked.connect(self._aplicar_perfil_consulta)
        self.btn_save_consulta_profile.clicked.connect(
            lambda: self._salvar_perfil_tabela_com_dialogo(
                "consulta",
                self.table_view,
                self.table_model,
                self.consulta_profile,
                ["Padrao", "Auditoria", "Estoque", "Custos"],
                self._consulta_scope(),
            )
        )
        self.btn_consulta_destacar.clicked.connect(
            lambda: self._destacar_tabela("consulta")
        )
        self.table_view.horizontalHeader().customContextMenuRequested.connect(
            lambda pos: self._abrir_menu_colunas_tabela(
                "consulta", self.table_view, pos, scope=self._consulta_scope()
            )
        )
        self.table_view.horizontalHeader().sectionMoved.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "consulta",
                self.table_view,
                self.table_model,
                scope=self._consulta_scope(),
            )
        )
        self.table_view.horizontalHeader().sectionResized.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "consulta",
                self.table_view,
                self.table_model,
                scope=self._consulta_scope(),
            )
        )
        self.btn_prev_page.clicked.connect(self.prev_page)
        self.btn_next_page.clicked.connect(self.next_page)

        self.btn_mov_estoque_prev_page.clicked.connect(self._prev_page_mov_estoque)
        self.btn_mov_estoque_next_page.clicked.connect(self._next_page_mov_estoque)
        self.btn_aba_mensal_prev_page.clicked.connect(self._prev_page_aba_mensal)
        self.btn_aba_mensal_next_page.clicked.connect(self._next_page_aba_mensal)
        self.btn_aba_anual_prev_page.clicked.connect(self._prev_page_aba_anual)
        self.btn_aba_anual_next_page.clicked.connect(self._next_page_aba_anual)

        self.btn_export_excel_full.clicked.connect(lambda: self.export_excel("full"))
        self.btn_export_excel_filtered.clicked.connect(
            lambda: self.export_excel("filtered")
        )
        self.btn_export_excel_visible.clicked.connect(
            lambda: self.export_excel("visible")
        )
        self.btn_export_docx.clicked.connect(self.export_docx)
        self.btn_export_html_txt.clicked.connect(self.export_txt_html)

        self.btn_open_editable_table.clicked.connect(
            self.open_editable_aggregation_table
        )
        self.btn_execute_aggregation.clicked.connect(self.execute_aggregation)
        self.btn_reprocessar_agregacao.clicked.connect(self.reprocessar_agregacao)
        self.btn_clear_top_agg_filters.clicked.connect(
            self.clear_top_aggregation_filters
        )
        self.btn_clear_bottom_agg_filters.clicked.connect(
            self.clear_bottom_aggregation_filters
        )
        self.btn_top_match_ncm_cest.clicked.connect(
            lambda: self._aplicar_filtro_relacional_agregacao("top", include_gtin=False)
        )
        self.btn_top_match_ncm_cest_gtin.clicked.connect(
            lambda: self._aplicar_filtro_relacional_agregacao("top", include_gtin=True)
        )
        self.btn_apply_top_profile.clicked.connect(
            lambda: self._aplicar_perfil_agregacao(
                "agregacao_top",
                self.aggregation_table,
                self.aggregation_table_model,
                self.top_profile.currentText(),
            )
        )
        self.btn_save_top_profile.clicked.connect(
            lambda: self._salvar_perfil_tabela_com_dialogo(
                "agregacao_top",
                self.aggregation_table,
                self.aggregation_table_model,
                self.top_profile,
                ["Padrao", "Auditoria", "Estoque", "Custos"],
            )
        )
        self.btn_top_colunas.clicked.connect(
            lambda: self._abrir_menu_colunas_tabela(
                "agregacao_top", self.aggregation_table
            )
        )
        self.btn_top_destacar.clicked.connect(
            lambda: self._destacar_tabela("agregacao_top")
        )
        self.aggregation_table.horizontalHeader().customContextMenuRequested.connect(
            lambda pos: self._abrir_menu_colunas_tabela(
                "agregacao_top", self.aggregation_table, pos
            )
        )
        self.aggregation_table.horizontalHeader().sectionMoved.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "agregacao_top", self.aggregation_table, self.aggregation_table_model
            )
        )
        self.aggregation_table.horizontalHeader().sectionResized.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "agregacao_top", self.aggregation_table, self.aggregation_table_model
            )
        )
        self.aggregation_table.horizontalHeader().sortIndicatorChanged.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "agregacao_top", self.aggregation_table, self.aggregation_table_model
            )
        )
        self.btn_apply_bottom_profile.clicked.connect(
            lambda: self._aplicar_perfil_agregacao(
                "agregacao_bottom",
                self.results_table,
                self.results_table_model,
                self.bottom_profile.currentText(),
            )
        )
        self.btn_save_bottom_profile.clicked.connect(
            lambda: self._salvar_perfil_tabela_com_dialogo(
                "agregacao_bottom",
                self.results_table,
                self.results_table_model,
                self.bottom_profile,
                ["Padrao", "Auditoria", "Estoque", "Custos"],
            )
        )
        self.btn_bottom_colunas.clicked.connect(
            lambda: self._abrir_menu_colunas_tabela(
                "agregacao_bottom", self.results_table
            )
        )
        self.results_table.horizontalHeader().customContextMenuRequested.connect(
            lambda pos: self._abrir_menu_colunas_tabela(
                "agregacao_bottom", self.results_table, pos
            )
        )
        self.results_table.horizontalHeader().sectionMoved.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "agregacao_bottom", self.results_table, self.results_table_model
            )
        )
        self.results_table.horizontalHeader().sectionResized.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "agregacao_bottom", self.results_table, self.results_table_model
            )
        )
        self.results_table.horizontalHeader().sortIndicatorChanged.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "agregacao_bottom", self.results_table, self.results_table_model
            )
        )
        self.btn_bottom_match_ncm_cest.clicked.connect(
            lambda: self._aplicar_filtro_relacional_agregacao(
                "bottom", include_gtin=False
            )
        )
        self.btn_bottom_match_ncm_cest_gtin.clicked.connect(
            lambda: self._aplicar_filtro_relacional_agregacao(
                "bottom", include_gtin=True
            )
        )
        self.btn_bottom_destacar.clicked.connect(
            lambda: self._destacar_tabela("agregacao_bottom")
        )
        for tabela, contexto in [
            (self.aggregation_table, "agregacao_top"),
            (self.results_table, "agregacao_bottom"),
            (self.sql_result_table, "sql_result"),
            (self.conversion_table, "conversao"),
            (self.aba_mensal_table, "aba_mensal"),
            (self.aba_anual_table, "aba_anual"),
            (self.nfe_entrada_table, "nfe_entrada"),
            (self.produtos_sel_table, "produtos_selecionados"),
            (self.id_agrupados_table, "id_agrupados"),
            (self.aba_codigo_original_table, "aba_codigo_original"),
        ]:
            tabela.setContextMenuPolicy(Qt.CustomContextMenu)
            tabela.customContextMenuRequested.connect(
                lambda pos, t=tabela, ctx=contexto: self._abrir_menu_contexto_celula(
                    ctx, t, pos
                )
            )

        for qf in [
            self.qf_norm,
            self.qf_desc,
            self.qf_ncm,
            self.qf_cest,
            self.aqf_norm,
            self.aqf_desc,
            self.aqf_ncm,
            self.aqf_cest,
        ]:
            qf.returnPressed.connect(self.apply_quick_filters)
            qf.textChanged.connect(lambda _value: self._schedule_consulta_quick_filters())
        for qf in [self.bqf_norm, self.bqf_desc, self.bqf_ncm, self.bqf_cest]:
            qf.returnPressed.connect(self.apply_aggregation_results_filters)
            qf.textChanged.connect(lambda _value: self._schedule_agregacao_bottom_filters())

    def _connect_sql_conversao_signals(self) -> None:
        # --- Consulta SQL tab ---
        self.sql_combo.currentIndexChanged.connect(self._on_sql_selected)
        self.btn_sql_execute.clicked.connect(self._execute_sql_query)
        self.btn_sql_export.clicked.connect(self._export_sql_results)
        self.btn_sql_destacar.clicked.connect(
            lambda: self._destacar_tabela("sql_result")
        )
        self.sql_result_search.returnPressed.connect(self._filter_sql_results)
        self.sql_result_search.textChanged.connect(lambda _value: self._schedule_sql_search())
        self.btn_sql_prev.clicked.connect(self._sql_prev_page)
        self.btn_sql_next.clicked.connect(self._sql_next_page)

        # --- Conversao tab ---
        self.btn_refresh_conversao.clicked.connect(self.atualizar_aba_conversao)
        self.chk_show_single_unit.stateChanged.connect(
            lambda _state: self.atualizar_aba_conversao()
        )
        self.btn_export_conversao.clicked.connect(self.exportar_conversao_excel)
        self.btn_import_conversao.clicked.connect(self.importar_conversao_excel)
        self.btn_conversao_destacar.clicked.connect(
            lambda: self._destacar_tabela("conversao")
        )
        self.btn_recalcular_fatores.clicked.connect(
            self.recalcular_derivados_conversao
        )
        self.btn_apply_conversao_profile.clicked.connect(
            lambda: self._aplicar_perfil_tabela(
                "conversao",
                self.conversion_table,
                self.conversion_model,
                self.conversao_profile.currentText(),
                "conversao",
            )
        )
        self.btn_save_conversao_profile.clicked.connect(
            lambda: self._salvar_perfil_tabela_com_dialogo(
                "conversao",
                self.conversion_table,
                self.conversion_model,
                self.conversao_profile,
                ["Padrao", "Auditoria", "Estoque", "Custos"],
            )
        )
        self.btn_conversao_colunas.clicked.connect(
            lambda: self._abrir_menu_colunas_tabela("conversao", self.conversion_table)
        )
        self.conversion_table.horizontalHeader().customContextMenuRequested.connect(
            lambda pos: self._abrir_menu_colunas_tabela(
                "conversao", self.conversion_table, pos
            )
        )
        self.conversion_table.horizontalHeader().sectionMoved.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "conversao", self.conversion_table, self.conversion_model
            )
        )
        self.conversion_table.horizontalHeader().sectionResized.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "conversao", self.conversion_table, self.conversion_model
            )
        )
        self.conversion_table.horizontalHeader().sortIndicatorChanged.connect(
            lambda _index, _order: self._salvar_preferencias_tabela(
                "conversao", self.conversion_table, self.conversion_model
            )
        )
        self.conv_filter_id.currentTextChanged.connect(lambda _value: self._schedule_conversao_filters())
        self.conv_filter_desc.textChanged.connect(lambda _value: self._schedule_conversao_filters())
        self.conversion_model.dataChanged.connect(self._on_conversion_model_changed)

        self.conversion_table.selectionModel().selectionChanged.connect(
            self._on_conversion_selection_changed
        )
        self.btn_apply_unid_ref.clicked.connect(self._apply_unid_ref_to_all)

    def _connect_signals(self) -> None:
        self._connect_base_signals()
        self._connect_relatorios_signals()
        self._connect_consulta_agregacao_signals()
        self._connect_sql_conversao_signals()
