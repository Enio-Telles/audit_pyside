---
goal: Integrar o projeto Fisconforme no Sistema_pysisde como painel 'Fisconforme não Atendido' dentro de uma nova aba 'Análise Lote CNPJ'
version: 1.0
date_created: 2025-07-16
last_updated: 2025-07-16
owner: Equipe GEFIS / SEFIN-RO
status: 'Planned'
tags: [feature, integration, pyside6, polars, oracle, fiscal]
---

# Introduction

![Status: Planned](https://img.shields.io/badge/status-Planned-blue)

Este plano detalha a integração completa do projeto **Fisconforme** (C:\fisconforme\) no projeto
**Fiscal Parquet Analyzer** (C:\Sistema_pysisde\) como um painel embarcado chamado
**"Fisconforme não Atendido"**, acessível via nova aba **"Análise Lote CNPJ"** no main_window.

O objetivo é reproduzir **exatamente** o mesmo resultado do projeto Fisconforme standalone:
- Extração de dados cadastrais e pendências de malha do Oracle DW
- Preenchimento do modelo de notificação HTML com dados do contribuinte
- Renderização de páginas PDF da DSF como imagens base64 embutidas
- Geração de arquivos .txt (HTML) de notificação em 
otificacoes/<DSF>/notificacao_det_<cnpj>.txt

O painel reutiliza diretamente o código do projeto Fisconforme, adaptado como subpacote
src/interface_grafica/fisconforme/, preservando toda a lógica de negócio e aumentando a
cobertura da análise já presente no projeto base.

---

## 1. Requirements & Constraints

- **REQ-001**: Resultado idêntico ao projeto C:\fisconforme\ — mesma estrutura de arquivo .txt (HTML), mesma tabela de pendências, mesmas imagens PDF embutidas em base64 com DPI=170 e largura=565px.
- **REQ-002**: O painel deve estar acessível na nova aba **"Análise Lote CNPJ"** adicionada ao QTabWidget de main_window.py após a aba "NFe Entrada".
- **REQ-003**: O painel deve incorporar o fluxo wizard completo: Banco → CNPJs → Auditor/DSF → Período → Processamento, com a mesma UX do projeto Fisconforme.
- **REQ-004**: Os arquivos de saída devem ser gravados em 
otificacoes/<DSF>/notificacao_det_<cnpj>.txt, relativo ao diretório raiz do projeto (C:\Sistema_pysisde\).
- **REQ-005**: Os Parquets extraídos do Oracle devem ser armazenados em dados/fisconforme/data_parquet/ dentro do projeto.
- **REQ-006**: A pasta dsf/ para os PDFs deve ser resolvida como dados/fisconforme/dsf/ dentro do projeto.
- **REQ-007**: O template modelo_notificacao_fisconforme_n_atendido.txt deve ser copiado para dentro do subpacote src/interface_grafica/fisconforme/.
- **REQ-008**: Todos os imports do código Fisconforme devem ser convertidos para imports relativos ao pacote src.interface_grafica.fisconforme.
- **REQ-009**: As dependências oracledb e pymupdf (fitz) devem ser declaradas em equirements.txt caso não estejam.
- **REQ-010**: Nenhuma lógica do pipeline ETL existente (src/transformacao/, src/extracao/) deve ser modificada.
- **SEC-001**: Credenciais Oracle (usuário/senha) não devem ser logadas nem expostas na UI — campo senha com EchoMode.Password.
- **SEC-002**: O arquivo .env da fisconforme (C:\fisconforme\.env) deve ser lido apenas como referência de configuração padrão; as credenciais para esta instância de devem ser lidas do .env em C:\Sistema_pysisde\.
- **CON-001**: Não criar arquivos .py redundantes com a mesma lógica já existente em src/utilitarios/ (ex: alidar_cnpj, limpar_cnpj — se disponíveis, reutilizar).
- **CON-002**: Não alterar o layout das abas existentes (Consulta, Consulta SQL, Agregacao, Conversao, Estoque, NFe Entrada, Logs).
- **CON-003**: Não usar sys.path.insert ou hacks de path — usar imports absolutos do pacote.
- **GUD-001**: Seguir o padrão de componentes PySide6 já existentes em main_window.py (QGroupBox, QHBoxLayout, QVBoxLayout).
- **GUD-002**: Usar QThread (padrão WorkerThread do Fisconforme) para processamento assíncrono.
- **PAT-001**: Estrutura de subpacote: src/interface_grafica/fisconforme/ com __init__.py e 1 arquivo por responsabilidade.

---

## 2. Implementation Steps

### Implementation Phase 1 — Preparação do Subpacote Fisconforme

- GOAL-001: Criar a estrutura de diretórios e copiar/adaptar todos os arquivos Python do projeto Fisconforme para src/interface_grafica/fisconforme/ com imports corrigidos.

| Task     | Description                                                                                                                                                                                  | Completed | Date |
| -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- | ---- |
| TASK-001 | Criar diretório src/interface_grafica/fisconforme/ e arquivo __init__.py vazio                                                                                                          |           |      |
| TASK-002 | Copiar C:\fisconforme\src\utils\path_resolver.py → src/interface_grafica/fisconforme/path_resolver.py e adaptar get_root_dir() para retornar Path(__file__).parents[4] (ra diretório de C:\Sistema_pysisde\) e get_resource_path() para aceitar caminhos relativos à nova raiz |           |      |
| TASK-003 | Copiar C:\fisconforme\src\ui\state.py → src/interface_grafica/fisconforme/state.py sem alterações (dataclasses puras, sem imports de projeto) |           |      |
| TASK-004 | Copiar C:\fisconforme\src\ui\models.py → src/interface_grafica/fisconforme/models.py e substituir imports rom .theme import ... por imports da tema local do fisconforme; ajustar rom .state import ... para rom .state import ... (relativo ao pacote fisconforme) |           |      |
| TASK-005 | Copiar C:\fisconforme\extrator_oracle.py → src/interface_grafica/fisconforme/extrator_oracle.py e substituir rom utils.path_resolver import ... por rom .path_resolver import ...; ajustar ROOT_DIR e DATA_PARQUET_DIR para usar get_root_dir() / "dados" / "fisconforme" / "data_parquet" |           |      |
| TASK-006 | Copiar C:\fisconforme\processador_polars.py → src/interface_grafica/fisconforme/processador_polars.py com mesmas adaptações de path (DATA_PARQUET_DIR → dados/fisconforme/data_parquet) |           |      |
| TASK-007 | Copiar C:\fisconforme\src\extracao.py → src/interface_grafica/fisconforme/extracao.py; substituir rom utils.path_resolver import ... por rom .path_resolver import ...; ajustar imports de extrair_tabela e conectar_oracle relativos ao pacote |           |      |
| TASK-008 | Copiar C:\fisconforme\src\extracao_cadastral.py → src/interface_grafica/fisconforme/extracao_cadastral.py; ajustar imports para .path_resolver, .extracao; corrigir ROOT_DIR = get_root_dir(), CACHE_PARQUET_DIR = ROOT_DIR / "dados" / "fisconforme" / "data_parquet" |           |      |
| TASK-009 | Copiar C:\fisconforme\src\preenchimento.py → src/interface_grafica/fisconforme/preenchimento.py; ajustar imports para .path_resolver; DSF_DIR = get_root_dir() / "dados" / "fisconforme" / "dsf" |           |      |
| TASK-010 | Copiar C:\fisconforme\src\gerar_notificacoes.py → src/interface_grafica/fisconforme/gerar_notificacoes.py; substituir todos os imports de extracao, extracao_cadastral, preenchimento com imports relativos (.extracao, .extracao_cadastral, .preenchimento); ajustar DIR_SAIDA_NOTIFICACOES = get_root_dir() / "notificacoes", MODELO_NOTIFICACAO = Path(__file__).parent / "modelo_notificacao_fisconforme_n_atendido.txt", DIR_PARQUET = get_root_dir() / "dados" / "fisconforme" / "data_parquet" |           |      |
| TASK-011 | Copiar C:\fisconforme\src\ui\workers.py → src/interface_grafica/fisconforme/workers.py; ajustar imports para .gerar_notificacoes, .extracao_cadastral, .state; remover sys.path.insert |           |      |
| TASK-012 | Copiar modelo_notificacao_fisconforme_n_atendido.txt → src/interface_grafica/fisconforme/modelo_notificacao_fisconforme_n_atendido.txt |           |      |

### Implementation Phase 2 — Criação das Configurações e Theme do Subpacote

- GOAL-002: Garantir que o subpacote tenha seus próprios helpers de configuração Oracle e theme PySide6 (adaptados do Fisconforme), sem conflito com o projeto base.

| Task     | Description                                                                                                                                                                       | Completed | Date |
| -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- | ---- |
| TASK-013 | Copiar C:\fisconforme\src\ui\theme.py → src/interface_grafica/fisconforme/theme.py sem alterações (constantes COLORS, SPACING, etc.) |           |      |
| TASK-014 | Copiar C:\fisconforme\src\ui\components.py → src/interface_grafica/fisconforme/components.py; ajustar import rom .theme import ... relativo ao pacote fisconforme |           |      |
| TASK-015 | Criar src/interface_grafica/fisconforme/config_service.py contendo: carregar_config_db() (lê C:\Sistema_pysisde\.env, chaves: DB_USER, DB_PASSWORD, ORACLE_HOST, ORACLE_PORT, ORACLE_SERVICE); salvar_config_db(dados: dict) -> bool (grava/atualiza .env); carregar_dados_salvos() -> dict (lê JSON de perfis de auditor de dados/fisconforme/perfis_auditor.json); salvar_dados_salvos(nome: str, dados: dict) -> bool (grava perfil) |           |      |
| TASK-016 | Criar dir dados/fisconforme/data_parquet/, dados/fisconforme/dsf/, 
otificacoes/ dentro do projeto (são diretórios de runtime, criar com .gitkeep) |           |      |

### Implementation Phase 3 — Criação do Widget Painel "Fisconforme não Atendido"

- GOAL-003: Criar o widget FisconformeNaoAtendidoPanel que encapsula o wizard completo (5 etapas) como um QWidget embarcável dentro de qualquer aba ou groupbox, com toda a lógica de navegação do shell original.

| Task     | Description                                                                                                                                                                       | Completed | Date |
| -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- | ---- |
| TASK-017 | Copiar C:\fisconforme\src\ui\pages.py → src/interface_grafica/fisconforme/pages.py; corrigir todos os imports: rom .components import ..., rom .models import ..., rom .state import ..., rom .theme import ..., rom .workers import ..., rom .extracao_cadastral import ..., rom .config_service import carregar_config_db, salvar_config_db, carregar_dados_salvos, salvar_dados_salvos, obter_estatisticas_cache, exportar_cache_completo; remover sys.path.insert |           |      |
| TASK-018 | Criar src/interface_grafica/fisconforme/panel.py — widget FisconformeNaoAtendidoPanel(QWidget) que: 1) instancia as 5 páginas (DatabaseConfigPage, CNPJsPage, AuditorPage, PeriodPage, ProcessingPage); 2) usa QStackedWidget para navegação; 3) replica a lógica de _build_sidebar(), _activate_step(), _go_previous(), _go_primary() e _handle_page_update() do shell.py original, mas como QWidget (não QMainWindow); 4) conecta ction_updated e workflow_requested das páginas; 5) expõe state: WizardState para acesso externo |           |      |
| TASK-019 | Atualizar src/interface_grafica/fisconforme/__init__.py para exportar: FisconformeNaoAtendidoPanel, WizardState |           |      |

### Implementation Phase 4 — Integração no main_window.py (Nova Aba)

- GOAL-004: Adicionar a nova aba "Análise Lote CNPJ" com o painel "Fisconforme não Atendido" ao QTabWidget em main_window.py, respeitando o layout e padrão existentes.

| Task     | Description                                                                                                                                                                                 | Completed | Date |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- | ---- |
| TASK-020 | Adicionar import de FisconformeNaoAtendidoPanel no topo de main_window.py: rom .fisconforme import FisconformeNaoAtendidoPanel (import lazy dentro do método para evitar circular imports se necessário) |           |      |
| TASK-021 | Criar método _build_tab_analise_lote_cnpj(self) -> QWidget em MainWindow que: 1) cria um QWidget + QVBoxLayout; 2) instancia self.fisconforme_panel = FisconformeNaoAtendidoPanel(); 3) adiciona o painel dentro de um QGroupBox("Fisconforme não Atendido"); 4) retorna o widget composto |           |      |
| TASK-022 | Adicionar a nova aba ao QTabWidget em _build_right_panel() após a linha que adiciona "NFe Entrada": self.tabs.addTab(self._build_tab_analise_lote_cnpj(), "Análise Lote CNPJ") |           |      |

### Implementation Phase 5 — Atualização de Dependências e Testes

- GOAL-005: Garantir que as dependências Python necessárias estejam declaradas e que os testes existentes continuem passando.

| Task     | Description                                                                                                                                                                    | Completed | Date |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------- | ---- |
| TASK-023 | Verificar equirements.txt de C:\Sistema_pysisde\: adicionar oracledb>=1.4.0 e pymupdf>=1.22.0 caso não estejam declarados |           |      |
| TASK-024 | Executar PYTHONPATH=src python -m pytest tests/ e confirmar 0 falhas não relacionadas às novas funcionalidades |           |      |
| TASK-025 | Testar importação do subpacote manualmente: python -c "from src.interface_grafica.fisconforme import FisconformeNaoAtendidoPanel; print('OK')" |           |      |
| TASK-026 | Verificar que o painel aparece corretamente na UI executando python app.py e navegando até a aba "Análise Lote CNPJ" |           |      |

---

## 3. Alternatives

- **ALT-001**: Utilizar o FisconformeApp (QMainWindow) em uma sub-janela (QMdiSubWindow) em vez de criar um widget embarcável. Descartado porque QMainWindow não pode ser embarcado limpo num QTabWidget; a abordagem de QWidget com QStackedWidget é mais idiomática.
- **ALT-002**: Chamar o executável do Fisconforme como subprocesso via subprocess.Popen. Descartado porque o objetivo é integração de código-fonte e resultado reproduzível dentro da mesma janela PySide6.
- **ALT-003**: Criar um serviço FastAPI separado para a lógica Fisconforme, consumido via HTTP pelo frontend React. Descartado porque o usuário especificou inserção no contexto PySide6 (desktop), não no frontend web.
- **ALT-004**: Manter o código Fisconforme em C:\fisconforme\ e adicioná-lo ao sys.path em runtime. Descartado por CON-003 (proibido usar sys.path.insert) e porque dificulta empacotamento e rastreabilidade.

---

## 4. Dependencies

- **DEP-001**: oracledb >= 1.4.0 — conexão com o Oracle DW (pool + retry)
- **DEP-002**: pymupdf >= 1.22.0 — renderização de páginas PDF da DSF em PNG base64 (import fitz)
- **DEP-003**: polars >= 0.20.0 — já presente no projeto; usado por processador_polars.py
- **DEP-004**: python-dotenv >= 1.0.0 — ler/escrever .env para credenciais Oracle
- **DEP-005**: PySide6 >= 6.5.0 — já presente no projeto
- **DEP-006**: Arquivo dados_cadastrais.sql — já presente em C:\Sistema_pysisde\sql\dados_cadastrais.sql
- **DEP-007**: Arquivo Fisconforme_malha_cnpj.sql — já presente em C:\Sistema_pysisde\sql\Fisconforme_malha_cnpj.sql

---

## 5. Files

- **FILE-001**: src/interface_grafica/fisconforme/__init__.py — novo (criado)
- **FILE-002**: src/interface_grafica/fisconforme/path_resolver.py — novo (adaptado de C:\fisconforme\src\utils\path_resolver.py)
- **FILE-003**: src/interface_grafica/fisconforme/state.py — novo (copiado de C:\fisconforme\src\ui\state.py)
- **FILE-004**: src/interface_grafica/fisconforme/models.py — novo (adaptado de C:\fisconforme\src\ui\models.py)
- **FILE-005**: src/interface_grafica/fisconforme/extrator_oracle.py — novo (adaptado de C:\fisconforme\extrator_oracle.py)
- **FILE-006**: src/interface_grafica/fisconforme/processador_polars.py — novo (adaptado de C:\fisconforme\processador_polars.py)
- **FILE-007**: src/interface_grafica/fisconforme/extracao.py — novo (adaptado de C:\fisconforme\src\extracao.py)
- **FILE-008**: src/interface_grafica/fisconforme/extracao_cadastral.py — novo (adaptado de C:\fisconforme\src\extracao_cadastral.py)
- **FILE-009**: src/interface_grafica/fisconforme/preenchimento.py — novo (adaptado de C:\fisconforme\src\preenchimento.py)
- **FILE-010**: src/interface_grafica/fisconforme/gerar_notificacoes.py — novo (adaptado de C:\fisconforme\src\gerar_notificacoes.py)
- **FILE-011**: src/interface_grafica/fisconforme/workers.py — novo (adaptado de C:\fisconforme\src\ui\workers.py)
- **FILE-012**: src/interface_grafica/fisconforme/theme.py — novo (copiado de C:\fisconforme\src\ui\theme.py)
- **FILE-013**: src/interface_grafica/fisconforme/components.py — novo (adaptado de C:\fisconforme\src\ui\components.py)
- **FILE-014**: src/interface_grafica/fisconforme/pages.py — novo (adaptado de C:\fisconforme\src\ui\pages.py)
- **FILE-015**: src/interface_grafica/fisconforme/config_service.py — novo (criado)
- **FILE-016**: src/interface_grafica/fisconforme/panel.py — novo (criado, adapta shell.py como QWidget)
- **FILE-017**: src/interface_grafica/fisconforme/modelo_notificacao_fisconforme_n_atendido.txt — novo (copiado de C:\fisconforme\modelo_notificacao_fisconforme_n_atendido.txt)
- **FILE-018**: src/interface_grafica/ui/main_window.py — modificado (adiciona import, método e aba)
- **FILE-019**: equirements.txt — possivelmente modificado (adicionar oracledb, pymupdf)
- **FILE-020**: dados/fisconforme/data_parquet/.gitkeep — novo (cria dir)
- **FILE-021**: dados/fisconforme/dsf/.gitkeep — novo (cria dir)
- **FILE-022**: 
otificacoes/.gitkeep — novo (cria dir de saída)

---

## 6. Testing

- **TEST-001**: Teste de importação do subpacote: python -c "from src.interface_grafica.fisconforme import FisconformeNaoAtendidoPanel" deve retornar sem erro.
- **TEST-002**: Teste de importação do config_service.py: rom src.interface_grafica.fisconforme.config_service import carregar_config_db deve retornar sem erro.
- **TEST-003**: Teste de importação do gerar_notificacoes.py: rom src.interface_grafica.fisconforme.gerar_notificacoes import gerar_notificacao_para_cnpj deve retornar sem erro (mesmo sem Oracle disponível).
- **TEST-004**: Teste do path_resolver.py: get_root_dir() deve retornar Path('C:/Sistema_pysisde').
- **TEST-005**: Teste visual: executar pp.py e verificar que a aba "Análise Lote CNPJ" aparece no QTabWidget.
- **TEST-006**: Teste visual: verificar que o painel "Fisconforme não Atendido" exibe as 5 etapas do wizard com sidebar lateral.
- **TEST-007**: Regressão do projeto base: PYTHONPATH=src python -m pytest tests/ deve passar em todos os testes existentes sem alterações.
- **TEST-008**: Teste de geração offline (sem Oracle): popular dados/fisconforme/data_parquet/ com Parquets de teste e executar gerar_notificacao_para_cnpj — deve gerar arquivo .txt com tabela e imagem placeholder.

---

## 7. Risks & Assumptions

- **RISK-001**: Conflito de nomes de classe DataTable, SectionCard, StatusBanner, etc. — tanto main_window.py quanto o subpacote fisconforme definem esses nomes. Mitigação: imports do fisconforme ficam isolados em src/interface_grafica/fisconforme/, sem vazamento para main_window.py além do FisconformeNaoAtendidoPanel.
- **RISK-002**: oracledb pode requerer Oracle Instant Client instalado no SO — fora do escopo deste plano. Mitigação: capturar ImportError / DatabaseError graciosamente com mensagem na StatusBanner.
- **RISK-003**: pymupdf pode conflitar com outras versões de itz — verificar com pip show pymupdf. Mitigação: pinnar versão em equirements.txt.
- **RISK-004**: O wizard do Fisconforme usa WorkerThread(QThread) que acessa o QThreadPool.globalInstance(). Executado dentro do main_window, pode competir com PipelineWorker e ServiceTaskWorker. Mitigação: WorkerThread usa thread própria (não pool), então não há contenção.
- **RISK-005**: O arquivo modelo_notificacao_fisconforme_n_atendido.txt contém uma logo embutida em base64 (~200KB). Copiar como recurso estático garante que o fluxo de preenchimento (ler_modelo_notificacao) funcione sem depender de caminho externo.
- **ASSUMPTION-001**: O .env de C:\Sistema_pysisde\ será usado para credenciais Oracle (mesmo banco que o Fisconforme usa), portanto não requer arquivo .env separado.
- **ASSUMPTION-002**: As SQL queries dados_cadastrais.sql e Fisconforme_malha_cnpj.sql já existentes em C:\Sistema_pysisde\sql\ são compatíveis com as usadas pelo Fisconforme.
- **ASSUMPTION-003**: O módulo extracao_cadastral.py do Fisconforme usa cache local em Parquet; no contexto integrado, o cache ficará em dados/fisconforme/data_parquet/dados_cadastrais_cache.parquet.

---

## 8. Related Specifications / Further Reading

- [C:\fisconforme\GUIA_RAPIDO.md](C:\fisconforme\GUIA_RAPIDO.md) — guia de uso do projeto Fisconforme standalone
- [C:\fisconforme\INDICE.md](C:\fisconforme\INDICE.md) — índice de arquivos e responsabilidades do projeto Fisconforme
- [C:\Sistema_pysisde\AGENTS.md](C:\Sistema_pysisde\AGENTS.md) — regras arquiteturais do projeto base
- [C:\fisconforme\src\ui\shell.py](C:\fisconforme\src\ui\shell.py) — implementação do wizard shell (QMainWindow) a ser convertido em QWidget
- [C:\fisconforme\modelo_notificacao_fisconforme_n_atendido.txt](C:\fisconforme\modelo_notificacao_fisconforme_n_atendido.txt) — template HTML de notificação
