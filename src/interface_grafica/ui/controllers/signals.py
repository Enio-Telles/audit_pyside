from __future__ import annotations

from typing import Any

from PySide6.QtCore import Qt


def connect_signals(win: Any) -> None:
    """Connect all signals for a MainWindow instance.

    This mirrors the original MainWindow._connect_signals implementation but
    keeps the wiring in a separate module to reduce the size of
    `main_window.py`.
    """

    def schedule_mov() -> None:
        win._schedule_debounced("mov_filters", win.aplicar_filtros_mov_estoque)

    def schedule_anual() -> None:
        win._schedule_debounced("anual_filters", win.aplicar_filtros_aba_anual)

    def schedule_mensal() -> None:
        win._schedule_debounced("mensal_filters", win.aplicar_filtros_aba_mensal)

    def schedule_nfe_entrada() -> None:
        win._schedule_debounced(
            "nfe_entrada_filters", win.aplicar_filtros_nfe_entrada
        )

    def schedule_produtos_sel() -> None:
        win._schedule_debounced(
            "produtos_sel_filters", win.aplicar_filtros_produtos_selecionados
        )

    def schedule_id_agrupados() -> None:
        win._schedule_debounced(
            "id_agrupados_filters", win.aplicar_filtros_id_agrupados
        )

    def schedule_conv() -> None:
        win._schedule_debounced("conversao_filters", win.aplicar_filtros_conversao)

    def schedule_consulta_quick() -> None:
        win._schedule_debounced("consulta_quick_filters", win.apply_quick_filters)

    def schedule_agregacao_bottom() -> None:
        win._schedule_debounced(
            "agregacao_bottom_filters", win.apply_aggregation_results_filters
        )

    def schedule_sql_search() -> None:
        win._schedule_debounced("sql_result_search", win._filter_sql_results)

    # Top-level controls
    win.btn_refresh_cnpjs.clicked.connect(win.refresh_cnpjs)
    win.btn_run_pipeline.clicked.connect(win.run_pipeline_for_input)
    win.btn_extrair_brutas.clicked.connect(win.extrair_tabelas_brutas)
    win.btn_processamento.clicked.connect(win.executar_processamento)
    win.btn_apagar_dados.clicked.connect(win.apagar_dados_cnpj)
    win.btn_apagar_cnpj.clicked.connect(win.apagar_cnpj_completo)
    win.cnpj_list.itemSelectionChanged.connect(win.on_cnpj_selected)
    win.file_tree.itemClicked.connect(win.on_file_activated)
    win.file_tree.itemDoubleClicked.connect(win.on_file_activated)
    win.btn_open_cnpj_folder.clicked.connect(win.open_cnpj_folder)
    win.btn_toggle_panel.toggled.connect(win._toggle_left_panel)
    win.tabs.currentChanged.connect(win._on_main_tab_changed)

    # --- Estoque Tab signals ---
    win.mov_filter_id.currentTextChanged.connect(lambda _value: schedule_mov())
    win.mov_filter_desc.textChanged.connect(lambda _value: schedule_mov())
    win.mov_filter_ncm.textChanged.connect(lambda _value: schedule_mov())
    win.mov_filter_tipo.currentIndexChanged.connect(lambda _index: schedule_mov())
    win.mov_filter_texto.textChanged.connect(lambda _value: schedule_mov())
    win.mov_filter_data_col.currentIndexChanged.connect(lambda _index: schedule_mov())
    win.mov_filter_data_ini.dateChanged.connect(lambda _date: schedule_mov())
    win.mov_filter_data_fim.dateChanged.connect(lambda _date: schedule_mov())
    win.mov_filter_num_col.currentIndexChanged.connect(lambda _index: schedule_mov())
    win.mov_filter_num_min.textChanged.connect(lambda _value: schedule_mov())
    win.mov_filter_num_max.textChanged.connect(lambda _value: schedule_mov())
    win.btn_mov_profile.clicked.connect(
        lambda: win._aplicar_perfil_tabela(
            "mov_estoque",
            win.mov_estoque_table,
            win.mov_estoque_model,
            win.mov_profile.currentText(),
            "mov_estoque",
        )
    )
    win.btn_mov_save_profile.clicked.connect(
        lambda: win._salvar_perfil_tabela_com_dialogo(
            "mov_estoque",
            win.mov_estoque_table,
            win.mov_estoque_model,
            win.mov_profile,
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
    win.btn_mov_colunas.clicked.connect(
        lambda: win._abrir_menu_colunas_tabela("mov_estoque", win.mov_estoque_table)
    )
    win.btn_mov_destacar.clicked.connect(lambda: win._destacar_tabela("mov_estoque"))
    win.mov_estoque_table.horizontalHeader().customContextMenuRequested.connect(
        lambda pos: win._abrir_menu_colunas_tabela("mov_estoque", win.mov_estoque_table, pos)
    )
    win.mov_estoque_table.horizontalHeader().sectionMoved.connect(
        lambda *_: win._salvar_preferencias_tabela(
            "mov_estoque", win.mov_estoque_table, win.mov_estoque_model
        )
    )
    win.mov_estoque_table.horizontalHeader().sectionResized.connect(
        lambda *_: win._salvar_preferencias_tabela(
            "mov_estoque", win.mov_estoque_table, win.mov_estoque_model
        )
    )
    win.mov_estoque_table.horizontalHeader().sortIndicatorChanged.connect(
        lambda _index, _order: win._salvar_preferencias_tabela(
            "mov_estoque", win.mov_estoque_table, win.mov_estoque_model
        )
    )

    win.btn_export_mov_estoque.clicked.connect(win.exportar_mov_estoque_excel)
    win.btn_refresh_aba_anual.clicked.connect(win.atualizar_aba_anual)
    win.btn_apply_aba_anual_filters.clicked.connect(win.aplicar_filtros_aba_anual)
    win.btn_clear_aba_anual_filters.clicked.connect(win.limpar_filtros_aba_anual)
    win.btn_filtrar_estoque_anual.clicked.connect(win.filtrar_estoque_pela_selecao_anual)
    win.btn_limpar_filtro_cruzado.clicked.connect(win.limpar_filtro_cruzado_anual)
    win.btn_export_aba_anual.clicked.connect(win.exportar_aba_anual_excel)

    def schedule_periodos() -> None:
        win._schedule_debounced("periodos_filters", win.aplicar_filtros_aba_periodos)

    win.btn_refresh_aba_periodos.clicked.connect(win.atualizar_aba_periodos)
    win.btn_apply_aba_periodos_filters.clicked.connect(win.aplicar_filtros_aba_periodos)
    win.btn_clear_aba_periodos_filters.clicked.connect(win.limpar_filtros_aba_periodos)
    win.btn_export_aba_periodos.clicked.connect(win.exportar_aba_periodos_excel)
    win.periodo_filter_id.currentTextChanged.connect(lambda _value: schedule_periodos())
    win.periodo_filter_desc.textChanged.connect(lambda _value: schedule_periodos())
    win.periodo_filter_texto.textChanged.connect(lambda _value: schedule_periodos())
    win.btn_periodo_profile.clicked.connect(
        lambda: win._aplicar_perfil_tabela(
            "aba_periodos",
            win.aba_periodos_table,
            win.aba_periodos_model,
            win.periodo_profile.currentText(),
            "aba_periodos",
        )
    )
    win.btn_periodo_save_profile.clicked.connect(
        lambda: win._salvar_perfil_tabela_com_dialogo(
            "aba_periodos",
            win.aba_periodos_table,
            win.aba_periodos_model,
            win.periodo_profile,
            ["Exportar", "Padrao", "Auditoria", "Estoque", "Custos"],
        )
    )
    win.btn_periodo_colunas.clicked.connect(
        lambda: win._abrir_menu_colunas_tabela("aba_periodos", win.aba_periodos_table)
    )
    win.btn_destacar_aba_periodos.clicked.connect(lambda: win._destacar_tabela("aba_periodos"))
    win.aba_periodos_table.horizontalHeader().customContextMenuRequested.connect(
        lambda pos: win._abrir_menu_colunas_tabela("aba_periodos", win.aba_periodos_table, pos)
    )

    win.anual_filter_id.currentTextChanged.connect(lambda _value: schedule_anual())
    win.anual_filter_desc.textChanged.connect(lambda _value: schedule_anual())
    win.anual_filter_ano.currentIndexChanged.connect(lambda _index: schedule_anual())
    win.anual_filter_texto.textChanged.connect(lambda _value: schedule_anual())
    win.anual_filter_num_col.currentIndexChanged.connect(lambda _index: schedule_anual())
    win.anual_filter_num_min.textChanged.connect(lambda _value: schedule_anual())
    win.anual_filter_num_max.textChanged.connect(lambda _value: schedule_anual())
    win.btn_anual_profile.clicked.connect(
        lambda: win._aplicar_perfil_tabela(
            "aba_anual",
            win.aba_anual_table,
            win.aba_anual_model,
            win.anual_profile.currentText(),
            "aba_anual",
        )
    )
    win.btn_anual_save_profile.clicked.connect(
        lambda: win._salvar_perfil_tabela_com_dialogo(
            "aba_anual",
            win.aba_anual_table,
            win.aba_anual_model,
            win.anual_profile,
            ["Exportar", "Padrao", "Auditoria", "Estoque", "Custos"],
        )
    )
    win.btn_anual_colunas.clicked.connect(
        lambda: win._abrir_menu_colunas_tabela("aba_anual", win.aba_anual_table)
    )
    win.btn_destacar_aba_anual.clicked.connect(lambda: win._destacar_tabela("aba_anual"))
    win.aba_anual_table.horizontalHeader().customContextMenuRequested.connect(
        lambda pos: win._abrir_menu_colunas_tabela("aba_anual", win.aba_anual_table, pos)
    )
    win.aba_anual_table.horizontalHeader().sectionMoved.connect(
        lambda *_: win._salvar_preferencias_tabela("aba_anual", win.aba_anual_table, win.aba_anual_model)
    )
    win.aba_anual_table.horizontalHeader().sectionResized.connect(
        lambda *_: win._salvar_preferencias_tabela("aba_anual", win.aba_anual_table, win.aba_anual_model)
    )
    win.aba_anual_table.horizontalHeader().sortIndicatorChanged.connect(
        lambda _index, _order: win._salvar_preferencias_tabela("aba_anual", win.aba_anual_table, win.aba_anual_model)
    )

    win.btn_refresh_resumo_global.clicked.connect(win.atualizar_aba_resumo_global)
    win.btn_export_resumo_global.clicked.connect(win.exportar_resumo_global_excel)

    win.btn_refresh_aba_mensal.clicked.connect(win.atualizar_aba_mensal)
    win.btn_apply_aba_mensal_filters.clicked.connect(win.aplicar_filtros_aba_mensal)
    win.btn_clear_aba_mensal_filters.clicked.connect(win.limpar_filtros_aba_mensal)
    win.btn_export_aba_mensal.clicked.connect(win.exportar_aba_mensal_excel)
    win.mensal_filter_num_col.currentIndexChanged.connect(lambda _index: schedule_mensal())
    win.mensal_filter_num_min.textChanged.connect(lambda _value: schedule_mensal())
    win.mensal_filter_num_max.textChanged.connect(lambda _value: schedule_mensal())
    win.mensal_filter_id.currentTextChanged.connect(lambda _value: schedule_mensal())
    win.mensal_filter_desc.textChanged.connect(lambda _value: schedule_mensal())
    win.mensal_filter_ano.currentIndexChanged.connect(lambda _index: schedule_mensal())
    win.mensal_filter_mes.currentIndexChanged.connect(lambda _index: schedule_mensal())
    win.mensal_filter_texto.textChanged.connect(lambda _value: schedule_mensal())
    win.btn_mensal_profile.clicked.connect(
        lambda: win._aplicar_perfil_tabela(
            "aba_mensal",
            win.aba_mensal_table,
            win.aba_mensal_model,
            win.mensal_profile.currentText(),
            "aba_mensal",
        )
    )
    win.btn_mensal_save_profile.clicked.connect(
        lambda: win._salvar_perfil_tabela_com_dialogo(
            "aba_mensal",
            win.aba_mensal_table,
            win.aba_mensal_model,
            win.mensal_profile,
            ["Exportar", "Padrao", "Auditoria", "Estoque", "Custos"],
        )
    )
    win.btn_mensal_colunas.clicked.connect(
        lambda: win._abrir_menu_colunas_tabela("aba_mensal", win.aba_mensal_table)
    )
    win.btn_destacar_aba_mensal.clicked.connect(lambda: win._destacar_tabela("aba_mensal"))
    win.aba_mensal_table.horizontalHeader().customContextMenuRequested.connect(
        lambda pos: win._abrir_menu_colunas_tabela("aba_mensal", win.aba_mensal_table, pos)
    )
    win.aba_mensal_table.horizontalHeader().sectionMoved.connect(
        lambda *_: win._salvar_preferencias_tabela("aba_mensal", win.aba_mensal_table, win.aba_mensal_model)
    )
    win.aba_mensal_table.horizontalHeader().sectionResized.connect(
        lambda *_: win._salvar_preferencias_tabela("aba_mensal", win.aba_mensal_table, win.aba_mensal_model)
    )
    win.aba_mensal_table.horizontalHeader().sortIndicatorChanged.connect(
        lambda _index, _order: win._salvar_preferencias_tabela("aba_mensal", win.aba_mensal_table, win.aba_mensal_model)
    )

    win.btn_extract_nfe_entrada.clicked.connect(win.extrair_dados_nfe_entrada)
    win.btn_refresh_nfe_entrada.clicked.connect(win.atualizar_aba_nfe_entrada)
    win.btn_apply_nfe_entrada_filters.clicked.connect(win.aplicar_filtros_nfe_entrada)
    win.btn_clear_nfe_entrada_filters.clicked.connect(win.limpar_filtros_nfe_entrada)
    win.btn_nfe_entrada_profile.clicked.connect(
        lambda: win._aplicar_perfil_tabela(
            "nfe_entrada",
            win.nfe_entrada_table,
            win.nfe_entrada_model,
            win.nfe_entrada_profile.currentText(),
            "nfe_entrada",
        )
    )
    win.btn_nfe_entrada_save_profile.clicked.connect(
        lambda: win._salvar_perfil_tabela_com_dialogo(
            "nfe_entrada",
            win.nfe_entrada_table,
            win.nfe_entrada_model,
            win.nfe_entrada_profile,
            ["Padrao", "Auditoria", "Estoque", "Custos"],
        )
    )
    win.btn_nfe_entrada_colunas.clicked.connect(
        lambda: win._abrir_menu_colunas_tabela("nfe_entrada", win.nfe_entrada_table)
    )
    win.btn_nfe_entrada_destacar.clicked.connect(lambda: win._destacar_tabela("nfe_entrada"))
    win.btn_export_nfe_entrada.clicked.connect(win.exportar_nfe_entrada_excel)
    win.nfe_entrada_filter_id.currentTextChanged.connect(lambda _value: schedule_nfe_entrada())
    win.nfe_entrada_filter_desc.textChanged.connect(lambda _value: schedule_nfe_entrada())
    win.nfe_entrada_filter_ncm.textChanged.connect(lambda _value: schedule_nfe_entrada())
    win.nfe_entrada_filter_sefin.textChanged.connect(lambda _value: schedule_nfe_entrada())
    win.nfe_entrada_filter_texto.textChanged.connect(lambda _value: schedule_nfe_entrada())
    win.nfe_entrada_filter_data_ini.dateChanged.connect(lambda _date: schedule_nfe_entrada())
    win.nfe_entrada_filter_data_fim.dateChanged.connect(lambda _date: schedule_nfe_entrada())
    win.nfe_entrada_table.horizontalHeader().customContextMenuRequested.connect(
        lambda pos: win._abrir_menu_colunas_tabela("nfe_entrada", win.nfe_entrada_table, pos)
    )
    win.nfe_entrada_table.horizontalHeader().sectionMoved.connect(
        lambda *_: win._salvar_preferencias_tabela("nfe_entrada", win.nfe_entrada_table, win.nfe_entrada_model)
    )
    win.nfe_entrada_table.horizontalHeader().sectionResized.connect(
        lambda *_: win._salvar_preferencias_tabela("nfe_entrada", win.nfe_entrada_table, win.nfe_entrada_model)
    )
    win.nfe_entrada_table.horizontalHeader().sortIndicatorChanged.connect(
        lambda _index, _order: win._salvar_preferencias_tabela("nfe_entrada", win.nfe_entrada_table, win.nfe_entrada_model)
    )

    win.btn_refresh_id_agrupados.clicked.connect(win.atualizar_aba_id_agrupados)
    win.btn_apply_id_agrupados_filters.clicked.connect(win.aplicar_filtros_id_agrupados)
    win.btn_clear_id_agrupados_filters.clicked.connect(win.limpar_filtros_id_agrupados)
    win.btn_id_agrupados_profile.clicked.connect(
        lambda: win._aplicar_perfil_tabela(
            "id_agrupados",
            win.id_agrupados_table,
            win.id_agrupados_model,
            win.id_agrupados_profile.currentText(),
            "id_agrupados",
        )
    )
    win.btn_id_agrupados_save_profile.clicked.connect(
        lambda: win._salvar_perfil_tabela_com_dialogo(
            "id_agrupados",
            win.id_agrupados_table,
            win.id_agrupados_model,
            win.id_agrupados_profile,
            ["Padrao", "Auditoria", "Estoque", "Custos"],
        )
    )
    win.btn_id_agrupados_colunas.clicked.connect(
        lambda: win._abrir_menu_colunas_tabela("id_agrupados", win.id_agrupados_table)
    )
    win.btn_destacar_id_agrupados.clicked.connect(lambda: win._destacar_tabela("id_agrupados"))
    win.btn_export_id_agrupados.clicked.connect(win.exportar_id_agrupados_excel)
    win.id_agrupados_filter_id.currentTextChanged.connect(lambda _value: schedule_id_agrupados())
    win.id_agrupados_filter_texto.textChanged.connect(lambda _value: schedule_id_agrupados())
    win.id_agrupados_table.horizontalHeader().customContextMenuRequested.connect(
        lambda pos: win._abrir_menu_colunas_tabela("id_agrupados", win.id_agrupados_table, pos)
    )
    win.id_agrupados_table.horizontalHeader().sectionMoved.connect(
        lambda *_: win._salvar_preferencias_tabela("id_agrupados", win.id_agrupados_table, win.id_agrupados_model)
    )
    win.id_agrupados_table.horizontalHeader().sectionResized.connect(
        lambda *_: win._salvar_preferencias_tabela("id_agrupados", win.id_agrupados_table, win.id_agrupados_model)
    )
    win.id_agrupados_table.horizontalHeader().sortIndicatorChanged.connect(
        lambda _index, _order: win._salvar_preferencias_tabela("id_agrupados", win.id_agrupados_table, win.id_agrupados_model)
    )

    win.btn_refresh_produtos_sel.clicked.connect(win.atualizar_aba_produtos_selecionados)
    win.btn_apply_produtos_sel_filters.clicked.connect(win.aplicar_filtros_produtos_selecionados)
    win.btn_clear_produtos_sel_filters.clicked.connect(win.limpar_filtros_produtos_selecionados)
    win.btn_produtos_sel_profile.clicked.connect(
        lambda: win._aplicar_perfil_tabela(
            "produtos_selecionados",
            win.produtos_sel_table,
            win.produtos_selecionados_model,
            win.produtos_sel_profile.currentText(),
            "produtos_selecionados",
        )
    )
    win.btn_produtos_sel_save_profile.clicked.connect(
        lambda: win._salvar_perfil_tabela_com_dialogo(
            "produtos_selecionados",
            win.produtos_sel_table,
            win.produtos_selecionados_model,
            win.produtos_sel_profile,
            ["Padrao", "Auditoria", "Estoque", "Custos"],
        )
    )
    win.btn_colunas_produtos_sel.clicked.connect(
        lambda: win._abrir_menu_colunas_tabela("produtos_selecionados", win.produtos_sel_table)
    )
    win.btn_destacar_produtos_sel.clicked.connect(lambda: win._destacar_tabela("produtos_selecionados"))
    win.btn_export_produtos_sel.clicked.connect(win.exportar_produtos_selecionados_excel)
    win.produtos_sel_filter_id.currentTextChanged.connect(lambda _value: schedule_produtos_sel())
    win.produtos_sel_filter_desc.textChanged.connect(lambda _value: schedule_produtos_sel())
    win.produtos_sel_filter_ano_ini.currentIndexChanged.connect(lambda _index: schedule_produtos_sel())
    win.produtos_sel_filter_ano_fim.currentIndexChanged.connect(lambda _index: schedule_produtos_sel())
    win.produtos_sel_filter_data_ini.dateChanged.connect(lambda _date: schedule_produtos_sel())
    win.produtos_sel_filter_data_fim.dateChanged.connect(lambda _date: schedule_produtos_sel())
    win.produtos_sel_filter_texto.textChanged.connect(lambda _value: schedule_produtos_sel())
    win.produtos_sel_table.horizontalHeader().customContextMenuRequested.connect(
        lambda pos: win._abrir_menu_colunas_tabela("produtos_selecionados", win.produtos_sel_table, pos)
    )
    win.produtos_sel_table.horizontalHeader().sectionMoved.connect(
        lambda *_: win._salvar_preferencias_tabela("produtos_selecionados", win.produtos_sel_table, win.produtos_selecionados_model)
    )
    win.produtos_sel_table.horizontalHeader().sectionResized.connect(
        lambda *_: win._salvar_preferencias_tabela("produtos_selecionados", win.produtos_sel_table, win.produtos_selecionados_model)
    )
    win.produtos_sel_table.horizontalHeader().sortIndicatorChanged.connect(
        lambda _index, _order: win._salvar_preferencias_tabela("produtos_selecionados", win.produtos_sel_table, win.produtos_selecionados_model)
    )

    win.btn_add_filter.clicked.connect(win.add_filter_from_form)
    win.btn_clear_filters.clicked.connect(win.clear_filters)
    win.btn_remove_filter.clicked.connect(win.remove_selected_filter)
    win.btn_choose_columns.clicked.connect(win.choose_columns)
    win.btn_apply_consulta_profile.clicked.connect(win._aplicar_perfil_consulta)
    win.btn_save_consulta_profile.clicked.connect(
        lambda: win._salvar_perfil_tabela_com_dialogo(
            "consulta",
            win.table_view,
            win.table_model,
            win.consulta_profile,
            ["Padrao", "Auditoria", "Estoque", "Custos"],
            win._consulta_scope(),
        )
    )
    win.btn_consulta_destacar.clicked.connect(lambda: win._destacar_tabela("consulta"))
    win.table_view.horizontalHeader().customContextMenuRequested.connect(
        lambda pos: win._abrir_menu_colunas_tabela("consulta", win.table_view, pos, scope=win._consulta_scope())
    )
    win.table_view.horizontalHeader().sectionMoved.connect(
        lambda *_: win._salvar_preferencias_tabela("consulta", win.table_view, win.table_model, scope=win._consulta_scope())
    )
    win.table_view.horizontalHeader().sectionResized.connect(
        lambda *_: win._salvar_preferencias_tabela("consulta", win.table_view, win.table_model, scope=win._consulta_scope())
    )
    win.btn_prev_page.clicked.connect(win.prev_page)
    win.btn_next_page.clicked.connect(win.next_page)

    win.btn_export_excel_full.clicked.connect(lambda: win.export_excel("full"))
    win.btn_export_excel_filtered.clicked.connect(lambda: win.export_excel("filtered"))
    win.btn_export_excel_visible.clicked.connect(lambda: win.export_excel("visible"))
    win.btn_export_docx.clicked.connect(win.export_docx)
    win.btn_export_html_txt.clicked.connect(win.export_txt_html)

    win.btn_open_editable_table.clicked.connect(win.open_editable_aggregation_table)
    win.btn_execute_aggregation.clicked.connect(win.execute_aggregation)
    win.btn_reprocessar_agregacao.clicked.connect(win.reprocessar_agregacao)
    win.btn_clear_top_agg_filters.clicked.connect(win.clear_top_aggregation_filters)
    win.btn_clear_bottom_agg_filters.clicked.connect(win.clear_bottom_aggregation_filters)
    win.btn_top_match_ncm_cest.clicked.connect(
        lambda: win._aplicar_filtro_relacional_agregacao("top", include_gtin=False)
    )
    win.btn_top_match_ncm_cest_gtin.clicked.connect(
        lambda: win._aplicar_filtro_relacional_agregacao("top", include_gtin=True)
    )
    win.btn_apply_top_profile.clicked.connect(
        lambda: win._aplicar_perfil_agregacao(
            "agregacao_top",
            win.aggregation_table,
            win.aggregation_table_model,
            win.top_profile.currentText(),
        )
    )
    win.btn_save_top_profile.clicked.connect(
        lambda: win._salvar_perfil_tabela_com_dialogo(
            "agregacao_top",
            win.aggregation_table,
            win.aggregation_table_model,
            win.top_profile,
            ["Padrao", "Auditoria", "Estoque", "Custos"],
        )
    )
    win.btn_top_colunas.clicked.connect(lambda: win._abrir_menu_colunas_tabela("agregacao_top", win.aggregation_table))
    win.btn_top_destacar.clicked.connect(lambda: win._destacar_tabela("agregacao_top"))
    win.aggregation_table.horizontalHeader().customContextMenuRequested.connect(
        lambda pos: win._abrir_menu_colunas_tabela("agregacao_top", win.aggregation_table, pos)
    )
    win.aggregation_table.horizontalHeader().sectionMoved.connect(
        lambda *_: win._salvar_preferencias_tabela("agregacao_top", win.aggregation_table, win.aggregation_table_model)
    )
    win.aggregation_table.horizontalHeader().sectionResized.connect(
        lambda *_: win._salvar_preferencias_tabela("agregacao_top", win.aggregation_table, win.aggregation_table_model)
    )
    win.aggregation_table.horizontalHeader().sortIndicatorChanged.connect(
        lambda *_: win._salvar_preferencias_tabela("agregacao_top", win.aggregation_table, win.aggregation_table_model)
    )
    win.btn_apply_bottom_profile.clicked.connect(
        lambda: win._aplicar_perfil_agregacao(
            "agregacao_bottom",
            win.results_table,
            win.results_table_model,
            win.bottom_profile.currentText(),
        )
    )
    win.btn_save_bottom_profile.clicked.connect(
        lambda: win._salvar_perfil_tabela_com_dialogo(
            "agregacao_bottom",
            win.results_table,
            win.results_table_model,
            win.bottom_profile,
            ["Padrao", "Auditoria", "Estoque", "Custos"],
        )
    )
    win.btn_bottom_colunas.clicked.connect(lambda: win._abrir_menu_colunas_tabela("agregacao_bottom", win.results_table))
    win.results_table.horizontalHeader().customContextMenuRequested.connect(
        lambda pos: win._abrir_menu_colunas_tabela("agregacao_bottom", win.results_table, pos)
    )
    win.results_table.horizontalHeader().sectionMoved.connect(
        lambda *_: win._salvar_preferencias_tabela("agregacao_bottom", win.results_table, win.results_table_model)
    )
    win.results_table.horizontalHeader().sectionResized.connect(
        lambda *_: win._salvar_preferencias_tabela("agregacao_bottom", win.results_table, win.results_table_model)
    )
    win.results_table.horizontalHeader().sortIndicatorChanged.connect(
        lambda *_: win._salvar_preferencias_tabela("agregacao_bottom", win.results_table, win.results_table_model)
    )
    win.btn_bottom_match_ncm_cest.clicked.connect(
        lambda: win._aplicar_filtro_relacional_agregacao("bottom", include_gtin=False)
    )
    win.btn_bottom_match_ncm_cest_gtin.clicked.connect(
        lambda: win._aplicar_filtro_relacional_agregacao("bottom", include_gtin=True)
    )
    win.btn_bottom_destacar.clicked.connect(lambda: win._destacar_tabela("agregacao_bottom"))

    for tabela, contexto in [
        (win.aggregation_table, "agregacao_top"),
        (win.results_table, "agregacao_bottom"),
        (win.sql_result_table, "sql_result"),
        (win.conversion_table, "conversao"),
        (win.aba_mensal_table, "aba_mensal"),
        (win.aba_anual_table, "aba_anual"),
        (win.nfe_entrada_table, "nfe_entrada"),
        (win.produtos_sel_table, "produtos_selecionados"),
        (win.id_agrupados_table, "id_agrupados"),
    ]:
        tabela.setContextMenuPolicy(Qt.CustomContextMenu)
        tabela.customContextMenuRequested.connect(
            lambda pos, t=tabela, ctx=contexto: win._abrir_menu_contexto_celula(ctx, t, pos)
        )

    for qf in [
        win.qf_norm,
        win.qf_desc,
        win.qf_ncm,
        win.qf_cest,
        win.aqf_norm,
        win.aqf_desc,
        win.aqf_ncm,
        win.aqf_cest,
    ]:
        qf.returnPressed.connect(win.apply_quick_filters)
        qf.textChanged.connect(lambda _value: schedule_consulta_quick())
    for qf in [win.bqf_norm, win.bqf_desc, win.bqf_ncm, win.bqf_cest]:
        qf.returnPressed.connect(win.apply_aggregation_results_filters)
        qf.textChanged.connect(lambda _value: schedule_agregacao_bottom())

    # --- Consulta SQL tab ---
    win.sql_combo.currentIndexChanged.connect(win._on_sql_selected)
    win.btn_sql_execute.clicked.connect(win._execute_sql_query)
    win.btn_sql_export.clicked.connect(win._export_sql_results)
    win.btn_sql_destacar.clicked.connect(lambda: win._destacar_tabela("sql_result"))
    win.sql_result_search.returnPressed.connect(win._filter_sql_results)
    win.sql_result_search.textChanged.connect(lambda _value: schedule_sql_search())
    win.btn_sql_prev.clicked.connect(win._sql_prev_page)
    win.btn_sql_next.clicked.connect(win._sql_next_page)

    # --- Conversao tab ---
    win.btn_refresh_conversao.clicked.connect(win.atualizar_aba_conversao)
    win.chk_show_single_unit.stateChanged.connect(lambda _state: win.atualizar_aba_conversao())
    win.btn_export_conversao.clicked.connect(win.exportar_conversao_excel)
    win.btn_import_conversao.clicked.connect(win.importar_conversao_excel)
    win.btn_conversao_destacar.clicked.connect(lambda: win._destacar_tabela("conversao"))
    win.btn_recalcular_fatores.clicked.connect(lambda: win.recalcular_derivados_conversao())
    win.btn_apply_conversao_profile.clicked.connect(
        lambda: win._aplicar_perfil_tabela(
            "conversao",
            win.conversion_table,
            win.conversion_model,
            win.conversao_profile.currentText(),
            "conversao",
        )
    )
    win.btn_save_conversao_profile.clicked.connect(
        lambda: win._salvar_perfil_tabela_com_dialogo(
            "conversao",
            win.conversion_table,
            win.conversion_model,
            win.conversao_profile,
            ["Padrao", "Auditoria", "Estoque", "Custos"],
        )
    )
    win.btn_conversao_colunas.clicked.connect(lambda: win._abrir_menu_colunas_tabela("conversao", win.conversion_table))
    win.conversion_table.horizontalHeader().customContextMenuRequested.connect(
        lambda pos: win._abrir_menu_colunas_tabela("conversao", win.conversion_table, pos)
    )
    win.conversion_table.horizontalHeader().sectionMoved.connect(
        lambda *_: win._salvar_preferencias_tabela("conversao", win.conversion_table, win.conversion_model)
    )
    win.conversion_table.horizontalHeader().sectionResized.connect(
        lambda *_: win._salvar_preferencias_tabela("conversao", win.conversion_table, win.conversion_model)
    )
    win.conversion_table.horizontalHeader().sortIndicatorChanged.connect(
        lambda _index, _order: win._salvar_preferencias_tabela("conversao", win.conversion_table, win.conversion_model)
    )
    win.conv_filter_id.currentTextChanged.connect(lambda _value: schedule_conv())
    win.conv_filter_desc.textChanged.connect(lambda _value: schedule_conv())
    win.conversion_model.dataChanged.connect(win._on_conversion_model_changed)

    win.conversion_table.selectionModel().selectionChanged.connect(win._on_conversion_selection_changed)
    win.btn_apply_unid_ref.clicked.connect(win._apply_unid_ref_to_all)
