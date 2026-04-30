from __future__ import annotations


class MainWindowSignalWiringRelatoriosMixin:
    def _connect_relatorios_signals(self) -> None:
        # --- Estoque Tab signals ---
        self.mov_filter_id.currentTextChanged.connect(lambda _value: self._schedule_mov_filters())
        self.mov_filter_desc.textChanged.connect(lambda _value: self._schedule_mov_filters())
        self.mov_filter_ncm.textChanged.connect(lambda _value: self._schedule_mov_filters())
        self.mov_filter_tipo.currentIndexChanged.connect(lambda _index: self._schedule_mov_filters())
        self.mov_filter_texto.textChanged.connect(lambda _value: self._schedule_mov_filters())
        self.mov_filter_data_col.currentIndexChanged.connect(
            lambda _index: self._schedule_mov_filters()
        )
        self.mov_filter_data_ini.dateChanged.connect(lambda _date: self._schedule_mov_filters())
        self.mov_filter_data_fim.dateChanged.connect(lambda _date: self._schedule_mov_filters())
        self.mov_filter_num_col.currentIndexChanged.connect(
            lambda _index: self._schedule_mov_filters()
        )
        self.mov_filter_num_min.textChanged.connect(lambda _value: self._schedule_mov_filters())
        self.mov_filter_num_max.textChanged.connect(lambda _value: self._schedule_mov_filters())
        self.btn_mov_profile.clicked.connect(
            lambda: self._aplicar_perfil_tabela(
                "mov_estoque",
                self.mov_estoque_table,
                self.mov_estoque_model,
                self.mov_profile.currentText(),
                "mov_estoque",
            )
        )
        self.btn_mov_save_profile.clicked.connect(
            lambda: self._salvar_perfil_tabela_com_dialogo(
                "mov_estoque",
                self.mov_estoque_table,
                self.mov_estoque_model,
                self.mov_profile,
                [
                    "Exportar",
                    "Padrao",
                    "Auditoria",
                    "Auditoria Fiscal",
                    "Estoque",
                    "Custos",
                ],
            )
        )
        self.btn_mov_colunas.clicked.connect(
            lambda: self._abrir_menu_colunas_tabela(
                "mov_estoque", self.mov_estoque_table
            )
        )
        self.btn_mov_destacar.clicked.connect(
            lambda: self._destacar_tabela("mov_estoque")
        )
        self.mov_estoque_table.horizontalHeader().customContextMenuRequested.connect(
            lambda pos: self._abrir_menu_colunas_tabela(
                "mov_estoque", self.mov_estoque_table, pos
            )
        )
        self.mov_estoque_table.horizontalHeader().sectionMoved.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "mov_estoque", self.mov_estoque_table, self.mov_estoque_model
            )
        )
        self.mov_estoque_table.horizontalHeader().sectionResized.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "mov_estoque", self.mov_estoque_table, self.mov_estoque_model
            )
        )
        self.mov_estoque_table.horizontalHeader().sortIndicatorChanged.connect(
            lambda _index, _order: self._salvar_preferencias_tabela(
                "mov_estoque", self.mov_estoque_table, self.mov_estoque_model
            )
        )

        self.btn_export_mov_estoque.clicked.connect(self.exportar_mov_estoque_excel)
        self.btn_refresh_aba_anual.clicked.connect(self.atualizar_aba_anual)
        self.btn_apply_aba_anual_filters.clicked.connect(self.aplicar_filtros_aba_anual)
        self.btn_clear_aba_anual_filters.clicked.connect(self.limpar_filtros_aba_anual)
        self.btn_filtrar_estoque_anual.clicked.connect(
            self.filtrar_estoque_pela_selecao_anual
        )
        self.btn_limpar_filtro_cruzado.clicked.connect(self.limpar_filtro_cruzado_anual)
        self.btn_export_aba_anual.clicked.connect(self.exportar_aba_anual_excel)

        self.btn_refresh_aba_periodos.clicked.connect(self.atualizar_aba_periodos)
        self.btn_apply_aba_periodos_filters.clicked.connect(
            self.aplicar_filtros_aba_periodos
        )
        self.btn_clear_aba_periodos_filters.clicked.connect(
            self.limpar_filtros_aba_periodos
        )
        self.btn_export_aba_periodos.clicked.connect(self.exportar_aba_periodos_excel)
        self.periodo_filter_id.currentTextChanged.connect(
            lambda _value: self._schedule_periodos_filters()
        )
        self.periodo_filter_desc.textChanged.connect(lambda _value: self._schedule_periodos_filters())
        self.periodo_filter_texto.textChanged.connect(
            lambda _value: self._schedule_periodos_filters()
        )
        self.btn_periodo_profile.clicked.connect(
            lambda: self._aplicar_perfil_tabela(
                "aba_periodos",
                self.aba_periodos_table,
                self.aba_periodos_model,
                self.periodo_profile.currentText(),
                "aba_periodos",
            )
        )
        self.btn_periodo_save_profile.clicked.connect(
            lambda: self._salvar_perfil_tabela_com_dialogo(
                "aba_periodos",
                self.aba_periodos_table,
                self.aba_periodos_model,
                self.periodo_profile,
                ["Exportar", "Padrao", "Auditoria", "Estoque", "Custos"],
            )
        )
        self.btn_periodo_colunas.clicked.connect(
            lambda: self._abrir_menu_colunas_tabela(
                "aba_periodos", self.aba_periodos_table
            )
        )
        self.btn_destacar_aba_periodos.clicked.connect(
            lambda: self._destacar_tabela("aba_periodos")
        )
        self.aba_periodos_table.horizontalHeader().customContextMenuRequested.connect(
            lambda pos: self._abrir_menu_colunas_tabela(
                "aba_periodos", self.aba_periodos_table, pos
            )
        )
        self.aba_periodos_table.horizontalHeader().sectionMoved.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "aba_periodos", self.aba_periodos_table, self.aba_periodos_model
            )
        )
        self.aba_periodos_table.horizontalHeader().sectionResized.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "aba_periodos", self.aba_periodos_table, self.aba_periodos_model
            )
        )
        self.aba_periodos_table.horizontalHeader().sortIndicatorChanged.connect(
            lambda _index, _order: self._salvar_preferencias_tabela(
                "aba_periodos", self.aba_periodos_table, self.aba_periodos_model
            )
        )
        self.anual_filter_id.currentTextChanged.connect(lambda _value: self._schedule_anual_filters())
        self.anual_filter_desc.textChanged.connect(lambda _value: self._schedule_anual_filters())
        self.anual_filter_ano.currentIndexChanged.connect(
            lambda _index: self._schedule_anual_filters()
        )
        self.anual_filter_texto.textChanged.connect(lambda _value: self._schedule_anual_filters())
        self.anual_filter_num_col.currentIndexChanged.connect(
            lambda _index: self._schedule_anual_filters()
        )
        self.anual_filter_num_min.textChanged.connect(lambda _value: self._schedule_anual_filters())
        self.anual_filter_num_max.textChanged.connect(lambda _value: self._schedule_anual_filters())
        self.btn_anual_profile.clicked.connect(
            lambda: self._aplicar_perfil_tabela(
                "aba_anual",
                self.aba_anual_table,
                self.aba_anual_model,
                self.anual_profile.currentText(),
                "aba_anual",
            )
        )
        self.btn_anual_save_profile.clicked.connect(
            lambda: self._salvar_perfil_tabela_com_dialogo(
                "aba_anual",
                self.aba_anual_table,
                self.aba_anual_model,
                self.anual_profile,
                ["Exportar", "Padrao", "Auditoria", "Estoque", "Custos"],
            )
        )
        self.btn_anual_colunas.clicked.connect(
            lambda: self._abrir_menu_colunas_tabela("aba_anual", self.aba_anual_table)
        )
        self.btn_destacar_aba_anual.clicked.connect(
            lambda: self._destacar_tabela("aba_anual")
        )
        self.aba_anual_table.horizontalHeader().customContextMenuRequested.connect(
            lambda pos: self._abrir_menu_colunas_tabela(
                "aba_anual", self.aba_anual_table, pos
            )
        )
        self.aba_anual_table.horizontalHeader().sectionMoved.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "aba_anual", self.aba_anual_table, self.aba_anual_model
            )
        )
        self.aba_anual_table.horizontalHeader().sectionResized.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "aba_anual", self.aba_anual_table, self.aba_anual_model
            )
        )
        self.aba_anual_table.horizontalHeader().sortIndicatorChanged.connect(
            lambda _index, _order: self._salvar_preferencias_tabela(
                "aba_anual", self.aba_anual_table, self.aba_anual_model
            )
        )

        self.btn_refresh_resumo_global.clicked.connect(self.atualizar_aba_resumo_global)
        self.btn_export_resumo_global.clicked.connect(self.exportar_resumo_global_excel)
        self.chk_resumo_global_so_selecionados.toggled.connect(
            lambda _: self.atualizar_aba_resumo_global()
        )
        self.cmb_resumo_global_ano_ini.currentIndexChanged.connect(
            lambda _: self.atualizar_aba_resumo_global()
        )
        self.cmb_resumo_global_ano_fim.currentIndexChanged.connect(
            lambda _: self.atualizar_aba_resumo_global()
        )

        self.btn_refresh_aba_mensal.clicked.connect(self.atualizar_aba_mensal)
        self.btn_apply_aba_mensal_filters.clicked.connect(
            self.aplicar_filtros_aba_mensal
        )
        self.btn_clear_aba_mensal_filters.clicked.connect(
            self.limpar_filtros_aba_mensal
        )
        self.btn_export_aba_mensal.clicked.connect(self.exportar_aba_mensal_excel)
        self.mensal_filter_num_col.currentIndexChanged.connect(
            lambda _index: self._schedule_mensal_filters()
        )
        self.mensal_filter_num_min.textChanged.connect(lambda _value: self._schedule_mensal_filters())
        self.mensal_filter_num_max.textChanged.connect(lambda _value: self._schedule_mensal_filters())
        self.mensal_filter_id.currentTextChanged.connect(
            lambda _value: self._schedule_mensal_filters()
        )
        self.mensal_filter_desc.textChanged.connect(lambda _value: self._schedule_mensal_filters())
        self.mensal_filter_ano.currentIndexChanged.connect(
            lambda _index: self._schedule_mensal_filters()
        )
        self.mensal_filter_mes.currentIndexChanged.connect(
            lambda _index: self._schedule_mensal_filters()
        )
        self.mensal_filter_texto.textChanged.connect(lambda _value: self._schedule_mensal_filters())
        self.btn_mensal_profile.clicked.connect(
            lambda: self._aplicar_perfil_tabela(
                "aba_mensal",
                self.aba_mensal_table,
                self.aba_mensal_model,
                self.mensal_profile.currentText(),
                "aba_mensal",
            )
        )
        self.btn_mensal_save_profile.clicked.connect(
            lambda: self._salvar_perfil_tabela_com_dialogo(
                "aba_mensal",
                self.aba_mensal_table,
                self.aba_mensal_model,
                self.mensal_profile,
                ["Exportar", "Padrao", "Auditoria", "Estoque", "Custos"],
            )
        )
        self.btn_mensal_colunas.clicked.connect(
            lambda: self._abrir_menu_colunas_tabela("aba_mensal", self.aba_mensal_table)
        )
        self.btn_destacar_aba_mensal.clicked.connect(
            lambda: self._destacar_tabela("aba_mensal")
        )
        self.aba_mensal_table.horizontalHeader().customContextMenuRequested.connect(
            lambda pos: self._abrir_menu_colunas_tabela(
                "aba_mensal", self.aba_mensal_table, pos
            )
        )
        self.aba_mensal_table.horizontalHeader().sectionMoved.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "aba_mensal", self.aba_mensal_table, self.aba_mensal_model
            )
        )
        self.aba_mensal_table.horizontalHeader().sectionResized.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "aba_mensal", self.aba_mensal_table, self.aba_mensal_model
            )
        )
        self.aba_mensal_table.horizontalHeader().sortIndicatorChanged.connect(
            lambda _index, _order: self._salvar_preferencias_tabela(
                "aba_mensal", self.aba_mensal_table, self.aba_mensal_model
            )
        )

        self.btn_extract_nfe_entrada.clicked.connect(self.extrair_dados_nfe_entrada)
        self.btn_refresh_nfe_entrada.clicked.connect(self.atualizar_aba_nfe_entrada)
        self.btn_apply_nfe_entrada_filters.clicked.connect(
            self.aplicar_filtros_nfe_entrada
        )
        self.btn_clear_nfe_entrada_filters.clicked.connect(
            self.limpar_filtros_nfe_entrada
        )
        self.btn_nfe_entrada_profile.clicked.connect(
            lambda: self._aplicar_perfil_tabela(
                "nfe_entrada",
                self.nfe_entrada_table,
                self.nfe_entrada_model,
                self.nfe_entrada_profile.currentText(),
                "nfe_entrada",
            )
        )
        self.btn_nfe_entrada_save_profile.clicked.connect(
            lambda: self._salvar_perfil_tabela_com_dialogo(
                "nfe_entrada",
                self.nfe_entrada_table,
                self.nfe_entrada_model,
                self.nfe_entrada_profile,
                ["Padrao", "Auditoria", "Estoque", "Custos"],
            )
        )
        self.btn_nfe_entrada_colunas.clicked.connect(
            lambda: self._abrir_menu_colunas_tabela(
                "nfe_entrada", self.nfe_entrada_table
            )
        )
        self.btn_nfe_entrada_destacar.clicked.connect(
            lambda: self._destacar_tabela("nfe_entrada")
        )
        self.btn_export_nfe_entrada.clicked.connect(self.exportar_nfe_entrada_excel)
        self.nfe_entrada_filter_id.currentTextChanged.connect(
            lambda _value: self._schedule_nfe_entrada_filters()
        )
        self.nfe_entrada_filter_desc.textChanged.connect(
            lambda _value: self._schedule_nfe_entrada_filters()
        )
        self.nfe_entrada_filter_ncm.textChanged.connect(
            lambda _value: self._schedule_nfe_entrada_filters()
        )
        self.nfe_entrada_filter_sefin.textChanged.connect(
            lambda _value: self._schedule_nfe_entrada_filters()
        )
        self.nfe_entrada_filter_texto.textChanged.connect(
            lambda _value: self._schedule_nfe_entrada_filters()
        )
        self.nfe_entrada_filter_data_ini.dateChanged.connect(
            lambda _date: self._schedule_nfe_entrada_filters()
        )
        self.nfe_entrada_filter_data_fim.dateChanged.connect(
            lambda _date: self._schedule_nfe_entrada_filters()
        )
        self.nfe_entrada_table.horizontalHeader().customContextMenuRequested.connect(
            lambda pos: self._abrir_menu_colunas_tabela(
                "nfe_entrada", self.nfe_entrada_table, pos
            )
        )
        self.nfe_entrada_table.horizontalHeader().sectionMoved.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "nfe_entrada", self.nfe_entrada_table, self.nfe_entrada_model
            )
        )
        self.nfe_entrada_table.horizontalHeader().sectionResized.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "nfe_entrada", self.nfe_entrada_table, self.nfe_entrada_model
            )
        )
        self.nfe_entrada_table.horizontalHeader().sortIndicatorChanged.connect(
            lambda _index, _order: self._salvar_preferencias_tabela(
                "nfe_entrada", self.nfe_entrada_table, self.nfe_entrada_model
            )
        )

        self.btn_refresh_id_agrupados.clicked.connect(self.atualizar_aba_id_agrupados)
        self.btn_apply_id_agrupados_filters.clicked.connect(
            self.aplicar_filtros_id_agrupados
        )
        self.btn_clear_id_agrupados_filters.clicked.connect(
            self.limpar_filtros_id_agrupados
        )
        self.btn_id_agrupados_profile.clicked.connect(
            lambda: self._aplicar_perfil_tabela(
                "id_agrupados",
                self.id_agrupados_table,
                self.id_agrupados_model,
                self.id_agrupados_profile.currentText(),
                "id_agrupados",
            )
        )
        self.btn_id_agrupados_save_profile.clicked.connect(
            lambda: self._salvar_perfil_tabela_com_dialogo(
                "id_agrupados",
                self.id_agrupados_table,
                self.id_agrupados_model,
                self.id_agrupados_profile,
                ["Padrao", "Auditoria", "Estoque", "Custos"],
            )
        )
        self.btn_id_agrupados_colunas.clicked.connect(
            lambda: self._abrir_menu_colunas_tabela(
                "id_agrupados", self.id_agrupados_table
            )
        )
        self.btn_destacar_id_agrupados.clicked.connect(
            lambda: self._destacar_tabela("id_agrupados")
        )
        self.btn_export_id_agrupados.clicked.connect(self.exportar_id_agrupados_excel)
        self.id_agrupados_filter_id.currentTextChanged.connect(
            lambda _value: self._schedule_id_agrupados_filters()
        )
        self.id_agrupados_filter_texto.textChanged.connect(
            lambda _value: self._schedule_id_agrupados_filters()
        )
        self.id_agrupados_table.horizontalHeader().customContextMenuRequested.connect(
            lambda pos: self._abrir_menu_colunas_tabela(
                "id_agrupados", self.id_agrupados_table, pos
            )
        )
        self.id_agrupados_table.horizontalHeader().sectionMoved.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "id_agrupados", self.id_agrupados_table, self.id_agrupados_model
            )
        )
        self.id_agrupados_table.horizontalHeader().sectionResized.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "id_agrupados", self.id_agrupados_table, self.id_agrupados_model
            )
        )
        self.id_agrupados_table.horizontalHeader().sortIndicatorChanged.connect(
            lambda _index, _order: self._salvar_preferencias_tabela(
                "id_agrupados", self.id_agrupados_table, self.id_agrupados_model
            )
        )

        self.btn_refresh_aba_codigo_original.clicked.connect(
            self.atualizar_aba_codigo_original
        )
        self.btn_apply_aba_codigo_original_filters.clicked.connect(
            self.aplicar_filtros_aba_codigo_original
        )
        self.btn_clear_aba_codigo_original_filters.clicked.connect(
            self.limpar_filtros_aba_codigo_original
        )
        self.btn_export_aba_codigo_original.clicked.connect(
            self.exportar_aba_codigo_original_excel
        )
        self.btn_cod_original_profile.clicked.connect(
            lambda: self._aplicar_perfil_tabela(
                "aba_codigo_original",
                self.aba_codigo_original_table,
                self.aba_codigo_original_model,
                self.cod_original_profile.currentText(),
                "aba_codigo_original",
            )
        )
        self.btn_cod_original_save_profile.clicked.connect(
            lambda: self._salvar_perfil_tabela_com_dialogo(
                "aba_codigo_original",
                self.aba_codigo_original_table,
                self.aba_codigo_original_model,
                self.cod_original_profile,
                ["Exportar", "Padrao", "Auditoria", "Estoque", "Custos"],
            )
        )
        self.btn_cod_original_colunas.clicked.connect(
            lambda: self._abrir_menu_colunas_tabela(
                "aba_codigo_original", self.aba_codigo_original_table
            )
        )
        self.btn_destacar_aba_codigo_original.clicked.connect(
            lambda: self._destacar_tabela("aba_codigo_original")
        )
        self.cod_original_filter_cod.currentTextChanged.connect(
            lambda _v: self._schedule_debounced(
                "cod_original_filters", self.aplicar_filtros_aba_codigo_original
            )
        )
        self.cod_original_filter_desc.textChanged.connect(
            lambda _v: self._schedule_debounced(
                "cod_original_filters", self.aplicar_filtros_aba_codigo_original
            )
        )
        self.cod_original_filter_ano.currentIndexChanged.connect(
            lambda _i: self._schedule_debounced(
                "cod_original_filters", self.aplicar_filtros_aba_codigo_original
            )
        )
        self.cod_original_filter_mes.currentIndexChanged.connect(
            lambda _i: self._schedule_debounced(
                "cod_original_filters", self.aplicar_filtros_aba_codigo_original
            )
        )
        self.cod_original_filter_texto.textChanged.connect(
            lambda _v: self._schedule_debounced(
                "cod_original_filters", self.aplicar_filtros_aba_codigo_original
            )
        )
        self.cod_original_filter_num_col.currentIndexChanged.connect(
            lambda _i: self._schedule_debounced(
                "cod_original_filters", self.aplicar_filtros_aba_codigo_original
            )
        )
        self.cod_original_filter_num_min.textChanged.connect(
            lambda _v: self._schedule_debounced(
                "cod_original_filters", self.aplicar_filtros_aba_codigo_original
            )
        )
        self.cod_original_filter_num_max.textChanged.connect(
            lambda _v: self._schedule_debounced(
                "cod_original_filters", self.aplicar_filtros_aba_codigo_original
            )
        )
        self.aba_codigo_original_table.horizontalHeader().customContextMenuRequested.connect(
            lambda pos: self._abrir_menu_colunas_tabela(
                "aba_codigo_original", self.aba_codigo_original_table, pos
            )
        )
        self.aba_codigo_original_table.horizontalHeader().sectionMoved.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "aba_codigo_original", self.aba_codigo_original_table, self.aba_codigo_original_model
            )
        )
        self.aba_codigo_original_table.horizontalHeader().sectionResized.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "aba_codigo_original", self.aba_codigo_original_table, self.aba_codigo_original_model
            )
        )
        self.aba_codigo_original_table.horizontalHeader().sortIndicatorChanged.connect(
            lambda _index, _order: self._salvar_preferencias_tabela(
                "aba_codigo_original", self.aba_codigo_original_table, self.aba_codigo_original_model
            )
        )

        self.btn_refresh_produtos_sel.clicked.connect(
            self.atualizar_aba_produtos_selecionados
        )
        self.btn_apply_produtos_sel_filters.clicked.connect(
            self.aplicar_filtros_produtos_selecionados
        )
        self.btn_clear_produtos_sel_filters.clicked.connect(
            self.limpar_filtros_produtos_selecionados
        )
        self.btn_limpar_vistos_produtos_sel.clicked.connect(
            self.limpar_vistos_produtos_selecionados
        )
        self.btn_top20_icms_produtos_sel.clicked.connect(
            self.selecionar_top20_icms_produtos_selecionados
        )
        self.btn_top20_icms_periodo_produtos_sel.clicked.connect(
            self.selecionar_top20_icms_periodo_produtos_selecionados
        )
        self.btn_produtos_sel_profile.clicked.connect(
            lambda: self._aplicar_perfil_tabela(
                "produtos_selecionados",
                self.produtos_sel_table,
                self.produtos_selecionados_model,
                self.produtos_sel_profile.currentText(),
                "produtos_selecionados",
            )
        )
        self.btn_produtos_sel_save_profile.clicked.connect(
            lambda: self._salvar_perfil_tabela_com_dialogo(
                "produtos_selecionados",
                self.produtos_sel_table,
                self.produtos_selecionados_model,
                self.produtos_sel_profile,
                ["Padrao", "Auditoria", "Estoque", "Custos"],
            )
        )
        self.btn_colunas_produtos_sel.clicked.connect(
            lambda: self._abrir_menu_colunas_tabela(
                "produtos_selecionados", self.produtos_sel_table
            )
        )
        self.btn_destacar_produtos_sel.clicked.connect(
            lambda: self._destacar_tabela("produtos_selecionados")
        )
        self.btn_export_produtos_sel.clicked.connect(
            self.exportar_produtos_selecionados_excel
        )
        self.produtos_sel_filter_id.currentTextChanged.connect(
            lambda _value: self._schedule_produtos_sel_filters()
        )
        self.produtos_sel_filter_desc.textChanged.connect(
            lambda _value: self._schedule_produtos_sel_filters()
        )
        self.produtos_sel_filter_ano_ini.currentIndexChanged.connect(
            lambda _index: self._schedule_produtos_sel_filters()
        )
        self.produtos_sel_filter_ano_fim.currentIndexChanged.connect(
            lambda _index: self._schedule_produtos_sel_filters()
        )
        self.produtos_sel_filter_data_ini.dateChanged.connect(
            lambda _date: self._schedule_produtos_sel_filters()
        )
        self.produtos_sel_filter_data_fim.dateChanged.connect(
            lambda _date: self._schedule_produtos_sel_filters()
        )
        self.produtos_sel_filter_texto.textChanged.connect(
            lambda _value: self._schedule_produtos_sel_filters()
        )
        self.produtos_sel_table.horizontalHeader().customContextMenuRequested.connect(
            lambda pos: self._abrir_menu_colunas_tabela(
                "produtos_selecionados", self.produtos_sel_table, pos
            )
        )
        self.produtos_sel_table.horizontalHeader().sectionMoved.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "produtos_selecionados",
                self.produtos_sel_table,
                self.produtos_selecionados_model,
            )
        )
        self.produtos_sel_table.horizontalHeader().sectionResized.connect(
            lambda *_: self._salvar_preferencias_tabela(
                "produtos_selecionados",
                self.produtos_sel_table,
                self.produtos_selecionados_model,
            )
        )
        self.produtos_sel_table.horizontalHeader().sortIndicatorChanged.connect(
            lambda _index, _order: self._salvar_preferencias_tabela(
                "produtos_selecionados",
                self.produtos_sel_table,
                self.produtos_selecionados_model,
            )
        )

