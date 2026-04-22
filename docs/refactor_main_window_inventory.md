# Inventário da decomposição de `main_window.py`

## Escopo auditado

- Arquivo real auditado: `src/interface_grafica/ui/main_window.py`
- Data da auditoria: `2026-04-22`
- Branch de trabalho: `refactor/p3-decompose-main-window`
- Observação 1: o pedido menciona `src/interface_grafica/main_window.py`, mas o entrypoint real da GUI continua em `src/interface_grafica/ui/main_window.py`.
- Observação 2: `docs/plano_melhorias_backend_frontend_arquitetura.md` não existe neste checkout; portanto, não há como citar a seção P3 localmente sem inventar conteúdo.

## Métricas atuais

| Item | Valor |
|---|---:|
| Linhas de `src/interface_grafica/ui/main_window.py` | 9 539 |
| Classes top-level | 6 |
| Métodos totais nas classes | 262 |
| Métodos em `MainWindow` | 249 |
| Imports globais (`from` + `import`) | 27 |
| Definições explícitas de `Signal(...)` | 7 |
| Decorators `@Slot` / `@pyqtSlot` | 0 |
| Chamadas `.connect(...)` | 280 |
| Métodos distintos conectados como slots | 75 |

## Imports globais

- `from __future__ import annotations`
- `from dataclasses import dataclass`
- `from pathlib import Path`
- `from typing import Callable`
- `import base64`
- `import inspect`
- `import re`
- `import polars as pl`
- `from openpyxl import Workbook`
- `from openpyxl.styles import Font as OpenPyxlFont`
- `from PySide6.QtCore import QDate, QThread, Qt, Signal, QUrl, QByteArray, QTimer`
- `from PySide6.QtGui import (...)`
- `from PySide6.QtWidgets import (...)`
- `from interface_grafica.config import (...)`
- `from interface_grafica.models.table_model import PolarsTableModel`
- `from interface_grafica.services.aggregation_service import ServicoAgregacao`
- `from interface_grafica.services.export_service import ExportService`
- `from interface_grafica.services.parquet_service import FilterCondition, ParquetService`
- `from interface_grafica.services.pipeline_funcoes_service import (...)`
- `from interface_grafica.services.pipeline_service import PipelineService`
- `from interface_grafica.services.profile_utils import (...)`
- `from interface_grafica.services.query_worker import QueryWorker`
- `from interface_grafica.services.registry_service import RegistryService`
- `from interface_grafica.services.selection_persistence_service import (...)`
- `from interface_grafica.services.sql_service import SqlService, ParamInfo, WIDGET_DATE`
- `from interface_grafica.ui.dialogs import (...)`
- `from utilitarios.text import (...)`

## Sinais e slots

### Sinais explícitos

- `PipelineWorker.finished_ok`
- `PipelineWorker.failed`
- `PipelineWorker.progress`
- `ServiceTaskWorker.finished_ok`
- `ServiceTaskWorker.failed`
- `ServiceTaskWorker.progress`
- `DetachedTableWindow.closed`

### Slots conectados com maior impacto de regressão

- Pipeline/importação: `refresh_cnpjs`, `run_pipeline_for_input`, `extrair_tabelas_brutas`, `executar_processamento`, `apagar_dados_cnpj`, `apagar_cnpj_completo`, `on_cnpj_selected`, `on_file_activated`, `open_cnpj_folder`
- Auditoria/estoque: `atualizar_aba_anual`, `aplicar_filtros_aba_anual`, `limpar_filtros_aba_anual`, `filtrar_estoque_pela_selecao_anual`, `limpar_filtro_cruzado_anual`, `atualizar_aba_nfe_entrada`, `aplicar_filtros_nfe_entrada`, `atualizar_aba_id_agrupados`
- Agregação/conversão: `execute_aggregation`, `reprocessar_agregacao`, `reverter_agregacao`, `reverter_mapa_manual_ui`, `atualizar_aba_conversao`, `importar_conversao_excel`, `_execute_sql_query`, `_filter_sql_results`
- Relatórios: `atualizar_aba_mensal`, `atualizar_aba_periodos`, `atualizar_aba_resumo_global`, `atualizar_aba_produtos_selecionados`
- Compartilhados: `_toggle_left_panel`, `_on_main_tab_changed`, `_copy_selection_from_active_table`, `_on_conversion_model_changed`

## Mapa atual de tabs para a estrutura-alvo

| UI atual | Destino-alvo principal |
|---|---|
| Painel lateral (CPF/CNPJ, árvore de arquivos, ações de pipeline) | `windows/aba_importacao.py` + `windows/main_window.py` |
| `Configurações` | `windows/aba_importacao.py` |
| `Consulta` | `windows/aba_auditoria.py` |
| `Consulta SQL` | `windows/aba_agregacao.py` |
| `Agregacao` | `windows/aba_agregacao.py` |
| `Conversao` | `windows/aba_agregacao.py` |
| `Estoque` / `Mov. Estoque` | `windows/aba_auditoria.py` |
| `NFe Entrada` | `windows/aba_auditoria.py` |
| `Análise Lote CNPJ` | `windows/aba_importacao.py` |
| `Logs` | `windows/aba_importacao.py` |
| `Produtos Selecionados` | `windows/aba_relatorios.py` |
| `Aba Anual` | `windows/aba_relatorios.py` |
| `Aba Períodos` | `windows/aba_relatorios.py` |
| `Aba Mensal` | `windows/aba_relatorios.py` |
| `Resumo Global` | `windows/aba_relatorios.py` |
| Tema inline “High-Contrast Noir” | `themes/noir.qss` |

## Dependências cruzadas críticas

| Dependência atual | Impacto | Destino recomendado |
|---|---|---|
| `state.current_cnpj` é consumido por praticamente todas as tabs | sem isso, carregamento de parquet e títulos quebram | `controllers/shared_state.py` |
| `_connect_signals()` conecta 280 ações de múltiplas tabs no mesmo método | maior hotspot de regressão de signal/slot | quebrar por aba e delegar de `windows/main_window.py` |
| `_carregar_dados_parquet_async()` é reutilizado por estoque, NFe, conversão, relatórios e produtos | cache, worker lifecycle e estado assíncrono são transversais | `controllers/data_loading.py` |
| Perfis de tabela (`_carregar_preferencias_tabela`, `_salvar_preferencias_tabela`, `_aplicar_perfil_tabela`) são usados em quase todas as grids | risco alto de regressão visual/comportamental | `controllers/table_profiles.py` |
| `_destacar_tabela()` + `DetachedTableWindow` servem consulta, agregação, conversão e relatórios | janela destacada é cross-tab | `widgets/detached_table_window.py` + helper compartilhado |
| `filtrar_estoque_pela_selecao_anual()` acopla relatório anual ao grid de estoque | dependência entre auditoria e relatórios | `controllers/shared_state.py` ou `controllers/auditoria_relatorios_bridge.py` |
| `reprocessar_agregacao()` recalcula agregação, conversão, tabelas `_agr`, histórico e recargas posteriores | afeta auditoria e relatórios inteiros | `controllers/agregacao_reprocessamento.py` |
| `atualizar_aba_produtos_selecionados()` consome `mov_estoque`, `aba_mensal`, `aba_anual` e `aba_periodos` | depende de múltiplos datasets derivados | `controllers/relatorios_produtos.py` |
| `_obter_cnpj_valido()` existe duas vezes (linhas ~4065 e ~8076) | duplicação real com risco de divergência futura | unificar em `controllers/shared_state.py` |
| `_atualizar_titulo_aba_mensal()` e `_atualizar_titulo_aba_anual()` aparecem duplicados | duplicação funcional | unificar em helper compartilhado |

## Inventário de classes e destino proposto

| Classe | Linha | Destino-alvo |
|---|---:|---|
| `FloatDelegate` | 91 | `widgets/float_delegate.py` |
| `PipelineWorker` | 100 | `controllers/workers.py` |
| `ServiceTaskWorker` | 147 | `controllers/workers.py` |
| `DetachedTableWindow` | 189 | `widgets/detached_table_window.py` |
| `ViewState` | 370 | `controllers/shared_state.py` |
| `MainWindow` | 381 | `windows/main_window.py` + mixins/controllers por aba |

## Mapa de métodos para destino-alvo

### Widgets / workers / estado

- `FloatDelegate.createEditor` (92) → `widgets/float_delegate.py`
- `PipelineWorker.__init__` (105) → `controllers/workers.py`
- `PipelineWorker.run` (120) → `controllers/workers.py`
- `ServiceTaskWorker.__init__` (152) → `controllers/workers.py`
- `ServiceTaskWorker.run` (158) → `controllers/workers.py`
- `DetachedTableWindow.__init__` (192) → `widgets/detached_table_window.py`
- `DetachedTableWindow.contexto` (283) → `widgets/detached_table_window.py`
- `DetachedTableWindow.table_model` (287) → `widgets/detached_table_window.py`
- `DetachedTableWindow._expr_texto_coluna` (290) → `widgets/detached_table_window.py`
- `DetachedTableWindow._refresh_from_source` (304) → `widgets/detached_table_window.py`
- `DetachedTableWindow.apply_filters` (319) → `widgets/detached_table_window.py`
- `DetachedTableWindow.clear_filters` (347) → `widgets/detached_table_window.py`
- `DetachedTableWindow.closeEvent` (352) → `widgets/detached_table_window.py`
- `ViewState` (370) → `controllers/shared_state.py`

### `windows/main_window.py` — shell/orquestração

- `MainWindow.__init__` (382)
- `MainWindow._executar_callback_debounce` (484)
- `MainWindow._schedule_debounced` (490)
- `MainWindow._registrar_limpeza_worker` (502)
- `MainWindow._workers_ativos` (513)
- `MainWindow._atualizar_estado_botao_nfe_entrada` (520)
- `MainWindow._tentar_fechar_apos_workers` (526)
- `MainWindow.closeEvent` (532)
- `MainWindow._resize_table_once` (549)
- `MainWindow._reset_table_resize_flag` (555)
- `MainWindow._build_ui` (571)
- `MainWindow._build_right_panel` (661)

### `themes/noir.qss` / widgets compartilhados

- `MainWindow._estilo_botao_destacar` (558)
- `MainWindow._criar_botao_destacar` (566)
- `MainWindow._abrir_fio_de_ouro` (3670)
- `MainWindow._copiar_valor_celula` (3714)
- `MainWindow._abrir_menu_contexto_celula` (3720)
- `MainWindow.show_error` (3749)
- `MainWindow.show_info` (3752)
- `MainWindow._setup_copy_shortcut` (3755)
- `MainWindow._copy_selection_from_active_table` (3759)
- `MainWindow._detached_title` (3803)
- `MainWindow._detached_assets` (3820)
- `MainWindow._detached_scope` (3841)
- `MainWindow._on_detached_window_closed` (3873)
- `MainWindow._destacar_tabela` (3876)
- `MainWindow._destacar_tabela_estoque` (3961)

### `windows/aba_importacao.py`

- `MainWindow._build_left_panel` (588)
- `MainWindow._build_tab_configuracoes` (695)
- `MainWindow._verificar_conexoes` (895)
- `MainWindow._testar_conexao_para_status` (918)
- `MainWindow._testar_conexao` (969)
- `MainWindow._salvar_configuracoes` (1018)
- `MainWindow._build_tab_analise_lote_cnpj` (1550)
- `MainWindow._build_tab_logs` (1568)
- `MainWindow.refresh_cnpjs` (3964)
- `MainWindow.run_pipeline_for_input` (3976)
- `MainWindow.on_pipeline_finished` (4033)
- `MainWindow.on_pipeline_failed` (4057)
- `MainWindow._obter_cnpj_valido` (4065)
- `MainWindow.extrair_tabelas_brutas` (4083)
- `MainWindow._on_extracao_finished` (4124)
- `MainWindow._on_extracao_failed` (4140)
- `MainWindow.extrair_dados_nfe_entrada` (4146)
- `MainWindow._on_nfe_entrada_extract_finished` (4212)
- `MainWindow._on_nfe_entrada_extract_failed` (4233)
- `MainWindow.executar_processamento` (4239)
- `MainWindow._on_processamento_finished` (4269)
- `MainWindow._on_processamento_failed` (4287)
- `MainWindow.apagar_dados_cnpj` (4293)
- `MainWindow.apagar_cnpj_completo` (4316)
- `MainWindow.refresh_logs` (8923)
- `MainWindow.open_cnpj_folder` (9163)

### `windows/aba_auditoria.py`

- `MainWindow._build_tab_consulta` (1058)
- `MainWindow._build_tab_mov_estoque` (1576)
- `MainWindow._build_tab_estoque` (1719)
- `MainWindow._build_tab_nfe_entrada` (2000)
- `MainWindow.atualizar_aba_mov_estoque` (4351)
- `MainWindow.aplicar_filtros_mov_estoque` (4399)
- `MainWindow.atualizar_aba_nfe_entrada` (6616)
- `MainWindow.aplicar_filtros_nfe_entrada` (6665)
- `MainWindow.limpar_filtros_nfe_entrada` (6785)
- `MainWindow.exportar_nfe_entrada_excel` (6799)
- `MainWindow.atualizar_aba_id_agrupados` (6818)
- `MainWindow.aplicar_filtros_id_agrupados` (6870)
- `MainWindow.limpar_filtros_id_agrupados` (6949)
- `MainWindow.exportar_id_agrupados_excel` (6954)

### `windows/aba_agregacao.py`

- `MainWindow._build_tab_agregacao` (1180)
- `MainWindow._build_tab_sql_query` (1365)
- `MainWindow._build_tab_conversao` (1470)
- `MainWindow._build_tab_id_agrupados` (1922)
- `MainWindow._refresh_filter_list_widget` (8358)
- `MainWindow.choose_columns` (8364)
- `MainWindow.prev_page` (8391)
- `MainWindow.next_page` (8396)
- `MainWindow._save_dialog` (8409)
- `MainWindow._filters_text` (8415)
- `MainWindow._dataset_for_export` (8421)
- `MainWindow.export_excel` (8438)
- `MainWindow.export_docx` (8459)
- `MainWindow.export_txt_html` (8484)
- `MainWindow.open_editable_aggregation_table` (8509)
- `MainWindow._abrir_tabela_agrupada` (8532)
- `MainWindow._desfazer_agregacao` (8535)
- `MainWindow._obter_ids_agrupados_para_reversao` (8540)
- `MainWindow.reverter_agregacao` (8562)
- `MainWindow.reverter_mapa_manual_ui` (8611)
- `MainWindow._load_aggregation_table` (8689)
- `MainWindow.execute_aggregation` (8716)
- `MainWindow.apply_quick_filters` (8799)
- `MainWindow.apply_aggregation_results_filters` (8929)
- `MainWindow._obter_linha_selecionada_tabela` (9018)
- `MainWindow._resolver_coluna_agregacao` (9041)
- `MainWindow._aplicar_modo_relacional_agregacao_df` (9055)
- `MainWindow._aplicar_filtro_relacional_agregacao` (9117)
- `MainWindow.clear_top_aggregation_filters` (9148)
- `MainWindow.clear_bottom_aggregation_filters` (9155)
- `MainWindow._on_conversion_selection_changed` (9177)
- `MainWindow._apply_unid_ref_to_all` (9221)
- `MainWindow.recalcular_derivados_conversao` (9305)
- `MainWindow._enriquecer_dataframe_conversao` (9367)
- `MainWindow._montar_descricoes_exibicao_por_grupo` (9412)
- `MainWindow._carregar_descr_padrao_canonico_conversao` (9450)
- `MainWindow._carregar_descricoes_canonicas_conversao` (9500)
- `MainWindow._reconstruir_descricoes_conversao_via_produtos_final` (9544)
- `MainWindow._preparar_dataframe_para_salvar_conversao` (9616)
- `MainWindow._enriquecer_descricoes_conversao` (9633)
- `MainWindow.atualizar_aba_conversao` (9679)
- `MainWindow.aplicar_filtros_conversao` (9746)
- `MainWindow._on_conversion_model_changed` (9818)
- `MainWindow._atualizar_titulo_aba_conversao` (9932)
- `MainWindow.exportar_conversao_excel` (9945)
- `MainWindow.importar_conversao_excel` (9981)
- `MainWindow.reprocessar_agregacao` (10031)
- `MainWindow.recalcular_padroes_agregacao` (10084)
- `MainWindow.recalcular_totais_agregacao` (10092)
- `MainWindow.refazer_tabelas_agr_agregacao` (10095)
- `MainWindow.refazer_fontes_produtos_agregacao` (10098)
- `MainWindow.recarregar_historico_agregacao` (10102)
- `MainWindow.atualizar_tabelas_agregacao` (10129)
- `MainWindow._populate_sql_combo` (10152)
- `MainWindow._on_sql_selected` (10164)
- `MainWindow._clear_param_form` (10184)
- `MainWindow._rebuild_param_form` (10190)
- `MainWindow._collect_param_values` (10211)
- `MainWindow._execute_sql_query` (10223)
- `MainWindow._on_query_finished` (10247)
- `MainWindow._on_query_failed` (10264)
- `MainWindow._set_sql_status` (10269)
- `MainWindow._show_sql_result_page` (10276)
- `MainWindow._sql_prev_page` (10291)
- `MainWindow._sql_next_page` (10296)
- `MainWindow._filter_sql_results` (10304)
- `MainWindow._export_sql_results` (10346)

### `windows/aba_relatorios.py`

- `MainWindow._build_tab_produtos_selecionados` (1749)
- `MainWindow._build_tab_aba_anual` (2118)
- `MainWindow._build_tab_aba_periodos` (2303)
- `MainWindow.atualizar_aba_periodos` (2418)
- `MainWindow._reprocessar_periodos_auto` (2466)
- `MainWindow.aplicar_filtros_aba_periodos` (2498)
- `MainWindow.limpar_filtros_aba_periodos` (2551)
- `MainWindow.exportar_aba_periodos_excel` (2564)
- `MainWindow._build_tab_resumo_global` (2591)
- `MainWindow._build_tab_aba_mensal` (2676)
- `MainWindow.atualizar_aba_anual` (6219)
- `MainWindow.atualizar_aba_mensal` (6269)
- `MainWindow.aplicar_filtros_aba_mensal` (6327)
- `MainWindow.limpar_filtros_aba_mensal` (6406)
- `MainWindow.exportar_aba_mensal_excel_metodo` (6416)
- `MainWindow.exportar_aba_mensal_excel` (6434)
- `MainWindow.aplicar_filtros_aba_anual` (6437)
- `MainWindow.limpar_filtros_aba_anual` (6539)
- `MainWindow.filtrar_estoque_pela_selecao_anual` (6548)
- `MainWindow.limpar_filtro_cruzado_anual` (6566)
- `MainWindow.exportar_aba_anual_excel_metodo` (6572)
- `MainWindow.exportar_aba_anual_excel` (6590)
- `MainWindow.exportar_mov_estoque_excel` (6593)
- `MainWindow.atualizar_aba_produtos_selecionados` (6990)
- `MainWindow._coletar_base_produtos_selecionados` (7098)
- `MainWindow._anos_disponiveis_produtos_selecionados` (7149)
- `MainWindow._intervalo_anos_produtos_selecionados` (7177)
- `MainWindow._intervalo_datas_produtos_selecionados` (7186)
- `MainWindow._filtrar_dataframe_por_ids` (7195)
- `MainWindow._filtrar_dataframe_por_ano` (7210)
- `MainWindow._filtrar_dataframe_produtos_selecionados_por_data` (7236)
- `MainWindow._ids_produtos_selecionados_para_exportacao` (7287)
- `MainWindow.aplicar_filtros_produtos_selecionados` (7298)
- `MainWindow.limpar_filtros_produtos_selecionados` (7534)
- `MainWindow._escrever_planilha_openpyxl` (7548)
- `MainWindow._gerar_resumo_global` (7601)
- `MainWindow.atualizar_aba_resumo_global` (7745)
- `MainWindow.exportar_resumo_global_excel` (7821)
- `MainWindow._montar_valores_consolidados_produtos_selecionados` (7848)
- `MainWindow.exportar_produtos_selecionados_excel` (7879)
- `MainWindow._aba_mensal_foreground` (7978)
- `MainWindow._aba_mensal_background` (7985)
- `MainWindow._aba_anual_foreground` (7993)
- `MainWindow._aba_anual_background` (8001)
- `MainWindow._mov_estoque_foreground` (8013)
- `MainWindow._mov_estoque_font` (8035)
- `MainWindow._mov_estoque_background` (8042)
- `MainWindow._formatar_resumo_filtros` (8072)

### `controllers/shared_state.py` / `controllers/table_profiles.py`

- `MainWindow._connect_signals` (2861)
- `MainWindow._marcar_recalculo_conversao_pendente` (3846)
- `MainWindow._limpar_recalculo_conversao_pendente` (3855)
- `MainWindow._on_main_tab_changed` (3860)
- `MainWindow._toggle_left_panel` (4341)
- `MainWindow._atualizar_titulo_aba_mov_estoque` (4544)
- `MainWindow._atualizar_titulo_aba_anual` (4557)
- `MainWindow._atualizar_titulo_aba_mensal` (4573)
- `MainWindow._atualizar_titulo_aba_produtos_selecionados` (4589)
- `MainWindow._atualizar_titulo_aba_id_agrupados` (4607)
- `MainWindow._atualizar_titulo_aba_mensal` (4623)
- `MainWindow._atualizar_titulo_aba_anual` (4636)
- `MainWindow._atualizar_titulo_aba_nfe_entrada` (4649)
- `MainWindow._atualizar_titulo_aba_periodos` (4665)
- `MainWindow._popular_combo_texto` (4681)
- `MainWindow._filtrar_texto_em_colunas` (4697)
- `MainWindow._valor_qdate_ativo` (4720)
- `MainWindow._parse_numero_filtro` (4723)
- `MainWindow._filtrar_intervalo_numerico` (4732)
- `MainWindow._filtrar_intervalo_data` (4754)
- `MainWindow._preferencia_tabela_key` (4782)
- `MainWindow._consulta_scope` (4786)
- `MainWindow._carregar_preferencias_tabela` (4795)
- `MainWindow._capturar_estado_tabela` (4801)
- `MainWindow._aplicar_estado_tabela` (4830)
- `MainWindow._colunas_estado_perfil` (4863)
- `MainWindow._nomes_perfis_nomeados_tabela` (4913)
- `MainWindow._obter_estado_perfil_nomeado` (4925)
- `MainWindow._atualizar_combo_perfis_tabela` (4935)
- `MainWindow._salvar_perfil_nomeado_tabela` (4957)
- `MainWindow._serializar_estado_header` (4980)
- `MainWindow._restaurar_estado_header` (4984)
- `MainWindow._salvar_preferencias_tabela` (4991)
- `MainWindow._aplicar_preferencias_tabela` (5006)
- `MainWindow._obter_colunas_preset_perfil` (5016)
- `MainWindow._aplicar_layout_padrao_agregacao` (5675)
- `MainWindow._abrir_menu_colunas_tabela` (5717)
- `MainWindow._aplicar_perfil_tabela` (5754)
- `MainWindow._salvar_perfil_tabela_com_dialogo` (5777)
- `MainWindow._aplicar_ordenacao_padrao` (5804)
- `MainWindow._aplicar_preset_colunas` (5824)
- `MainWindow._aplicar_ordem_colunas` (5836)
- `MainWindow._dataframe_colunas_visiveis` (5853)
- `MainWindow._dataframe_colunas_perfil` (5882)
- `MainWindow._refresh_profile_combos` (5906)
- `MainWindow._aplicar_preset_mov_estoque` (5981)
- `MainWindow._aplicar_preset_aba_anual` (6000)
- `MainWindow._aplicar_preset_aba_mensal` (6017)
- `MainWindow._aplicar_perfil_consulta` (6039)
- `MainWindow._aplicar_perfil_agregacao` (6051)
- `MainWindow._carregar_dataset_ui` (6062)
- `MainWindow._carregar_dados_parquet_async` (6078)
- `MainWindow._limpar_aba_resumo_estoque` (6146)
- `MainWindow._garantir_resumos_estoque_atualizados` (6161)
- `MainWindow._obter_cnpj_valido` (8076)
- `MainWindow._executar_em_worker` (8084)
- `MainWindow.on_cnpj_selected` (8119)
- `MainWindow._carregar_aba_atual` (8154)
- `MainWindow.refresh_file_tree` (8182)
- `MainWindow.on_file_activated` (8228)
- `MainWindow.load_current_file` (8241)
- `MainWindow.reload_table` (8271)
- `MainWindow._update_page_label` (8301)
- `MainWindow._update_context_label` (8316)
- `MainWindow.add_filter_from_form` (8325)
- `MainWindow.clear_filters` (8345)
- `MainWindow.remove_selected_filter` (8350)

## Ordem recomendada da decomposição

1. Extrair `widgets/float_delegate.py`, `widgets/detached_table_window.py`, `controllers/workers.py` e `controllers/shared_state.py`.
2. Criar `windows/main_window.py` como shell de composição e manter `src/interface_grafica/ui/main_window.py` como shim compatível de import.
3. Migrar `aba_importacao` preservando painel lateral, configurações, logs e fluxo de pipeline.
4. Migrar `aba_auditoria` preservando consulta, estoque e NFe Entrada.
5. Migrar `aba_agregacao` preservando agregação, reversão, SQL e conversão.
6. Migrar `aba_relatorios` preservando anual, mensal, períodos, resumo global e produtos selecionados.
7. Mover QSS inline para `themes/noir.qss` sem alterar comportamento observável.

## Riscos explícitos

- Risco alto de regressão silenciosa em `_connect_signals()` se a extração for feita por cópia em vez de movimento.
- Risco alto de import circular entre `windows/main_window.py` e módulos de aba se os mixins chamarem `MainWindow` diretamente.
- Risco médio de divergência por duplicidade atual de `_obter_cnpj_valido`, `_atualizar_titulo_aba_anual` e `_atualizar_titulo_aba_mensal`.
- Risco médio de regressão visual ao externalizar QSS, porque hoje parte do tema “High-Contrast Noir” está distribuída em `setStyleSheet(...)`.
