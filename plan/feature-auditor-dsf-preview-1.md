---
goal: Adicionar prévia de PDF e inserção condicional da DSF + tabela Fisconforme na notificação TXT
version: 1.0
date_created: 2026-04-05
last_updated: 2026-04-05
owner: Enio Telles
status: 'Planned'
tags: [feature, pyside6, pdf-preview, dsf, fisconforme, notificacao]
---

# Introduction

![Status: Planned](https://img.shields.io/badge/status-Planned-blue)

Estende o wizard PySide6 "Fisconforme — Geração de Notificações" na etapa **Auditor / DSF (Etapa 3)** com:

1. **Prévia visual do PDF da DSF** — ao selecionar um arquivo PDF, suas páginas são renderizadas dentro da própria UI (via PyMuPDF) em um `QScrollArea`, sem abrir programa externo.
2. **Inserção condicional** — um `QCheckBox` "Incluir imagens da DSF" controla se as páginas renderizadas em base64 são embutidas no arquivo `.txt` de notificação (placeholder `{{DSF_IMAGENS}}`).
3. **Tabela de pendências Fisconforme** — confirma e corrige a alimentação do `{{TABELA}}` a partir de `Fisconforme_malha_cnpj.sql` com filtro de período efetivo (parâmetros bind `:data_inicio` / `:data_fim` adicionados ao SQL).

## 1. Requirements & Constraints

- **REQ-001**: A prévia do PDF deve ser renderizada diretamente no painel da `AuditorPage` ao selecionar o arquivo, sem dependências de visualizadores externos.
- **REQ-002**: O usuário deve poder marcar/desmarcar "Incluir imagens da DSF na notificação" através de um `QCheckBox` visível na `AuditorPage`, e a escolha deve persistir no `WizardState`.
- **REQ-003**: O arquivo TXT gerado deve conter o resultado da consulta `Fisconforme_malha_cnpj.sql` filtrado pelo CNPJ e pelo período (início/fim) definidos no wizard.
- **REQ-004**: O campo `{{DSF_IMAGENS}}` só deve ser preenchido quando `incluir_imagens_dsf == True` **e** um arquivo PDF válido foi selecionado.
- **REQ-005**: A prévia deve ser leve (DPI ≤ 72 para thumbnail; respeitar limite de 800 px de largura máxima do widget).
- **SEC-001**: Nenhum dado binário do PDF deve ser enviado pela rede; tudo permanece local.
- **CON-001**: PyMuPDF (`fitz`) já é dependência declarada em `requirements.txt`; nenhuma nova dependência de runtime é necessária.
- **CON-002**: A mudança não deve afetar o fluxo de processamento em lote (WorkerThread roda em QThread separado).
- **CON-003**: As páginas 4 e 5 do wizard (PeriodPage, ProcessingPage) não recebem alterações funcionais.
- **PAT-001**: Seguir o padrão `BaseWizardPage` (`load_state` / `persist_state` / `validate`).
- **PAT-002**: Nenhum código de UI dentro de funções de ETL (`extracao.py`, `preenchimento.py`).
- **GUD-001**: Manter retrocompatibilidade: se `incluir_imagens_dsf` não estiver presente no estado serializado, assumir `True` (comportamento anterior preservado).

## 2. Implementation Steps

### Implementation Phase 1 — Modelo de estado

- GOAL-001: Adicionar campo `incluir_imagens_dsf: bool = True` ao dataclass `WizardState` para que a escolha do usuário flua até o worker de processamento.

| Task     | Description                                                                                                                    | Completed | Date |
| -------- | ------------------------------------------------------------------------------------------------------------------------------ | --------- | ---- |
| TASK-001 | Em `src/interface_grafica/fisconforme/state.py`, adicionar campo `incluir_imagens_dsf: bool = field(default=True)` ao dataclass `WizardState`, depois de `pdf_dsf` e antes de `periodo_inicio`. | | |

### Implementation Phase 2 — Prévia do PDF e checkbox na AuditorPage

- GOAL-002: Tornar a seleção do PDF imediata visualmente e dar controle explícito ao usuário sobre a inserção das imagens no TXT.

| Task     | Description                                                                                                                                              | Completed | Date |
| -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- | ---- |
| TASK-002 | Em `pages.py`, adicionar imports: `from PySide6.QtGui import QPixmap, QImage` e `from PySide6.QtWidgets import QScrollArea, QCheckBox`. Verificar se `QLabel` já está importado. | | |
| TASK-003 | Na `__init__` de `AuditorPage`, após o bloco `layout.addWidget(dsf_card)`, criar um novo `SectionCard("Prévia do PDF da DSF")` contendo: (a) um `QScrollArea` com política `ScrollBarAsNeeded`; (b) um `QWidget` container interno com `QVBoxLayout` para empilhar as páginas; (c) altura mínima de 240 px; (d) visibilidade inicial `setVisible(False)`. Armazenar referências: `self._preview_scroll`, `self._preview_container`, `self._preview_layout`. | | |
| TASK-004 | Na `__init__` de `AuditorPage`, no bloco `dsf_card` (logo após a linha `btn_pdf.clicked.connect(self._select_pdf)`), adicionar um `QCheckBox("Incluir imagens da DSF na notificação")` pré-marcado (`setChecked(True)`). Armazenar como `self.chk_incluir_dsf`. | | |
| TASK-005 | Criar método privado `_renderizar_preview_pdf(self, caminho_pdf: Path)` em `AuditorPage`. O método deve: (a) limpar widgets existentes em `self._preview_layout`; (b) abrir `fitz.open(str(caminho_pdf))`; (c) para cada página (limite de 20), chamar `page.get_pixmap(matrix=fitz.Matrix(72/72, 72/72))`, converter para QPixmap via `QImage.fromData(pix.tobytes('png'))`, criar `QLabel` com `setPixmap(scaled_pixmap.scaledToWidth(780, Qt.SmoothTransformation))`; (d) adicionar o QLabel ao `self._preview_layout`; (e) tornar `self._preview_scroll.parentWidget()` visível; (f) fechar o documento `fitz`; (g) tratar `Exception` com log e exibir mensagem no `status_banner`. | | |
| TASK-006 | Em `AuditorPage._select_pdf`, após `self.lbl_pdf.setText(...)`, chamar `self._renderizar_preview_pdf(self._pdf_path)`. | | |
| TASK-007 | Em `AuditorPage.load_state`, ao restaurar `state.pdf_dsf`, chamar `self._renderizar_preview_pdf(state.pdf_dsf)` se o arquivo existir. Restaurar `self.chk_incluir_dsf.setChecked(state.incluir_imagens_dsf)`. | | |
| TASK-008 | Em `AuditorPage.persist_state`, adicionar `state.incluir_imagens_dsf = self.chk_incluir_dsf.isChecked()`. | | |

### Implementation Phase 3 — Propagação do flag pelo pipeline de processamento

- GOAL-003: Garantir que `incluir_imagens_dsf` chegue até a camada `preenchimento.py` onde `{{DSF_IMAGENS}}` é preenchido.

| Task     | Description                                                                                                                                                                     | Completed | Date |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- | ---- |
| TASK-009 | Em `workers.py` (`WorkerThread.__init__`), adicionar parâmetro `incluir_imagens_dsf: bool = True` e armazená-lo em `self.incluir_imagens_dsf`. | | |
| TASK-010 | Em `workers.py` (`WorkerThread.run`), passar `incluir_imagens_dsf=self.incluir_imagens_dsf` para `gerar_notificacao_para_cnpj(...)`. | | |
| TASK-011 | Em `pages.py` (`ProcessingPage._start_processing`), ao instanciar `WorkerThread(...)`, passar `incluir_imagens_dsf=state.incluir_imagens_dsf`. | | |
| TASK-012 | Em `gerar_notificacoes.py` (`gerar_notificacao_para_cnpj`), adicionar parâmetro `incluir_imagens_dsf: bool = True` e passá-lo na chamada a `processar_notificacao(... incluir_imagens_dsf=incluir_imagens_dsf)`. | | |
| TASK-013 | Em `preenchimento.py` (`processar_notificacao`), adicionar parâmetro `incluir_imagens_dsf: bool = True` e passá-lo para `preencher_modelo(... incluir_imagens_dsf=incluir_imagens_dsf)`. | | |
| TASK-014 | Em `preenchimento.py` (`preencher_modelo`), adicionar parâmetro `incluir_imagens_dsf: bool = True`. No bloco de processamento de `{{DSF_IMAGENS}}`, verificar `if incluir_imagens_dsf and dsf_num:` antes de chamar `converter_pdf_para_base64_html`. Se `incluir_imagens_dsf == False`, atribuir `dados_completos['DSF_IMAGENS'] = ''` diretamente sem invocar a conversão. | | |

### Implementation Phase 4 — Filtro de período no SQL Fisconforme_malha_cnpj.sql

- GOAL-004: Corrigir o filtro de período na consulta Oracle, que atualmente está comentado, fazendo com que o período definido no wizard (Etapa 4) seja efetivamente aplicado.

| Task     | Description                                                                                                                                                                     | Completed | Date |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- | ---- |
| TASK-015 | Em `sql/Fisconforme_malha_cnpj.sql`, descomentar e ajustar o filtro de período no CTE `PendenciasRankeadas`, adicionando `AND dp.periodo BETWEEN :data_inicio AND :data_fim` logo após `WHERE dp.cpf_cnpj = :CNPJ`. Remover o comentário `--AND dp.periodo < '202601'` que ficou obsoleto. | | |
| TASK-016 | Em `extracao.py` (`extrair_dados_malha`), verificar que as chaves do dict `params` batem com os bind variables do SQL: `"cnpj"` → `:CNPJ`, `"data_inicio"` → `:data_inicio`, `"data_fim"` → `:data_fim`. Ajustar capitalização se necessário (oracledb é case-insensitive, mas manter consistência). | | |

### Implementation Phase 5 — Verificação e testes

- GOAL-005: Rodar verificações de tipagem e testes existentes para garantir que nada foi quebrado.

| Task     | Description                                                                                                  | Completed | Date |
| -------- | ------------------------------------------------------------------------------------------------------------ | --------- | ---- |
| TASK-017 | Executar `cd frontend && pnpm exec tsc --noEmit` — deve continuar sem erros (mudanças são somente no backend Python). | | |
| TASK-018 | Executar `PYTHONPATH=src python -m pytest tests/ -x -q` e garantir que não há novas falhas introduzidas por esta feature. | | |
| TASK-019 | Teste manual: abrir o wizard PySide6, selecionar um PDF na Etapa 3, verificar que a prévia aparece no painel; desmarcar o checkbox; processar 1 CNPJ; confirmar que `{{DSF_IMAGENS}}` fica vazio no TXT gerado. | | |
| TASK-020 | Teste manual: processar 1 CNPJ com checkbox marcado e verificar que o TXT gerado contém as imagens em base64 e a tabela HTML de pendências com o período correto. | | |

## 3. Alternatives

- **ALT-001**: Usar `QWebEngineView` para renderizar o PDF nativamente. Descartado porque adiciona dependência pesada (`PySide6-WebEngine`) e pode causar conflitos com o ambiente conda `audit`.
- **ALT-002**: Abrir o PDF externamente via `subprocess.run(["start", arquivo])`. Descartado pois não atende o requisito de visualização integrada na UI.
- **ALT-003**: Adicionar uma etapa 6 dedicada à "Aprovação do PDF" no wizard. Descartado pois o controle simples de checkbox na Etapa 3 é suficiente e não fragmenta o fluxo.
- **ALT-004**: Aplicar o filtro de período via Python (filtrar a lista retornada do Oracle). Descartado pois é mais eficiente fazer o filtro no banco, reduzindo tráfego de dados.

## 4. Dependencies

- **DEP-001**: `PyMuPDF` (`fitz`) — já em `requirements.txt`, usado em `preenchimento.py`; reutilizado no widget de prévia.
- **DEP-002**: `PySide6` — já instalado; os widgets `QScrollArea`, `QCheckBox`, `QPixmap`, `QImage` fazem parte do pacote base.
- **DEP-003**: `oracledb` — já em uso; o SQL modificado (TASK-015) utiliza bind variables já suportados.

## 5. Files

- **FILE-001**: `src/interface_grafica/fisconforme/state.py` — adiciona campo `incluir_imagens_dsf`.
- **FILE-002**: `src/interface_grafica/fisconforme/pages.py` — modifica `AuditorPage` (preview widget + checkbox) e `ProcessingPage` (repasse do flag ao WorkerThread).
- **FILE-003**: `src/interface_grafica/fisconforme/workers.py` — `WorkerThread` aceita e repassa `incluir_imagens_dsf`.
- **FILE-004**: `src/interface_grafica/fisconforme/gerar_notificacoes.py` — `gerar_notificacao_para_cnpj` aceita e repassa `incluir_imagens_dsf`.
- **FILE-005**: `src/interface_grafica/fisconforme/preenchimento.py` — `preencher_modelo` e `processar_notificacao` aceitam `incluir_imagens_dsf` e condicionam a chamada a `converter_pdf_para_base64_html`.
- **FILE-006**: `sql/Fisconforme_malha_cnpj.sql` — adiciona filtro `AND dp.periodo BETWEEN :data_inicio AND :data_fim`.

## 6. Testing

- **TEST-001**: Teste unitário (ou manual) verificando que `WizardState.incluir_imagens_dsf` padrão é `True` e que `persist_state` salva corretamente o valor do checkbox.
- **TEST-002**: Teste unitário para `preencher_modelo` com `incluir_imagens_dsf=False`: verificar que `{{DSF_IMAGENS}}` é substituído por string vazia sem chamar `converter_pdf_para_base64_html`.
- **TEST-003**: Teste unitário para `preencher_modelo` com `incluir_imagens_dsf=True` e PDF ausente: verificar que a função faz fallback para string vazia graciosamente (comportamento já testado indiretamente).
- **TEST-004**: Verificar manualmente que a renderização da prévia não bloqueia a UI (deve ser rápida para PDFs de até 20 páginas a 72 DPI).
- **TEST-005**: Executar `tests/test_main_window_descricoes_conversao.py` e quaisquer testes que referenciem `WizardState` para confirmar compatibilidade com o novo campo.

## 7. Risks & Assumptions

- **RISK-001**: PDFs muito grandes (>100 páginas) podem tornar a renderização da prévia lenta. Mitigação: limitar a prévia a 20 páginas (TASK-005 item c) com aviso ao usuário.
- **RISK-002**: O SQL `Fisconforme_malha_cnpj.sql` pode não ter sido testado com os bind variables `:data_inicio`/`:data_fim` ativos; a lógica de conversão de período `MM/AAAA → YYYYMM` em `extrair_dados_malha` deve ser revisada (TASK-016).
- **RISK-003**: Parâmetros extras no dicionário `params` do Oracle (que existiam antes do TASK-015) podem causar erro em versões mais restritivas do oracledb. Após TASK-015, todos os bind variables estarão referenciados no SQL.
- **ASSUMPTION-001**: O arquivo `sql/Fisconforme_malha_cnpj.sql` está acessível em runtime via `path_resolver.get_root_dir() / "sql"` (já validado pelo código existente em `extracao.py`).
- **ASSUMPTION-002**: O formato de período da coluna `dp.periodo` no Oracle é `YYYYMM` (inferido da conversão já implementada em `extrair_dados_malha`).

## 8. Related Specifications / Further Reading

- [src/interface_grafica/fisconforme/pages.py](src/interface_grafica/fisconforme/pages.py) — código completo das páginas do wizard
- [src/interface_grafica/fisconforme/preenchimento.py](src/interface_grafica/fisconforme/preenchimento.py) — lógica de template e conversão de PDF
- [src/interface_grafica/fisconforme/state.py](src/interface_grafica/fisconforme/state.py) — dataclass WizardState
- [sql/Fisconforme_malha_cnpj.sql](sql/Fisconforme_malha_cnpj.sql) — consulta de pendências fiscais
- [src/interface_grafica/fisconforme/modelo_notificacao_fisconforme_n_atendido.txt](src/interface_grafica/fisconforme/modelo_notificacao_fisconforme_n_atendido.txt) — template HTML com placeholders `{{DSF_IMAGENS}}` e `{{TABELA}}`
