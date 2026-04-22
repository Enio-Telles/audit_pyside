Central de gerenciamento de projetos com integração GitHub e IA cooperativa.

## Visão Geral

Este hub centraliza o gerenciamento de todos os projetos em desenvolvimento, integrando GitHub com fluxo de trabalho assistido por múltiplas IAs: OpenAI Codex, GitHub Copilot e Claude Code.


## Seções do Hub

- 📁 Projetos — banco de dados com todos os projetos


- ✅ Tarefas e Issues — rastreamento de tarefas por projeto e IA


- 🔀 Pull Requests — acompanhamento de PRs abertas


- 🤖 Fluxo de IA Cooperativa — protocolo de uso de Codex, Copilot e Claude Code


- 📋 Sprints e Milestones — planejamento de entregas


- 📚 Documentação Técnica — decisões de arquitetura e guias


## 🤖 Fluxo de IA Cooperativa

  Protocolo de uso cooperativo de OpenAI Codex, GitHub Copilot e Claude Code nos projetos.

## Filosofia Cooperativa

  Cada IA tem especialidades distintas. O fluxo cooperativo maximiza a qualidade ao atribuir tarefas conforme o ponto forte de cada ferramenta, com revisão cruzada e aprovação humana antes de qualquer merge.

## Perfil de Cada IA

### OpenAI Codex

  Especialidade em geração de código a partir de descrições em linguagem natural, criação de scripts completos, refatoração estrutural e geração de testes unitários.

  Quando usar:

  - Criar novos módulos do zero a partir de especificação


  - Gerar testes unitários e de integração


  - Refatorar código com mudança de estrutura


  - Implementar algoritmos complexos


  - Gerar documentação técnica


  Configuração no projeto: Arquivo AGENTS.md em cada pasta define escopo e contexto.

### GitHub Copilot

  Especialidade em sugestões inline no editor, completar código em contexto, otimizações locais e revisão de PRs via Copilot Code Review.

  Quando usar:

  - Desenvolvimento incremental no editor (VS Code)


  - Completar funções e métodos existentes


  - Sugestões de otimização pontual


  - Revisão automática de PRs


  - Geração de docstrings e comentários


  Configuração no projeto: Arquivo .github/copilot-instructions.md (ou AGENTS.md para Codex).

### Claude Code

  Especialidade em análise de código complexo, raciocínio sobre arquitetura, revisão de PRs com contexto amplo, debugging de lógica fiscal e decisões de design.

  Quando usar:

  - Analisar impacto de mudanças em schema ou contratos


  - Revisar PRs com contexto de múltiplos arquivos


  - Debugging de lógica fiscal complexa


  - Decisões de arquitetura e trade-offs


  - Análise de rastreabilidade ponta a ponta


  Configuração no projeto: Arquivo .claude/agent-index.md mapeia AGENTS.md por camada.

## Fluxo de Trabalho Padrão

### Fase 1 — Planejamento (Humano + Claude)

  1. Humano descreve o requisito ou problema


  1. Claude analisa o contexto (AGENTS.md, código existente)


  1. Claude propõe solução no formato A–E (Diagnóstico, Reaproveitamento, Decisão, Justificativa, Plano)


  1. Humano aprova o plano


### Fase 2 — Implementação (Codex ou Copilot)

  1. Codex implementa módulos novos ou refatorações estruturais


  1. Copilot completa código incremental no editor


  1. Ambos seguem as regras do AGENTS.md do escopo


  1. Branch criada com prefixo feat/, fix/ ou refactor/


### Fase 3 — Revisão (Claude + Humano)

  1. Claude revisa a PR com contexto completo


  1. Identifica riscos de schema, fiscal, performance e rastreabilidade


  1. Copilot Code Review adiciona sugestões inline


  1. Humano aprova ou solicita ajustes


### Fase 4 — Merge e Documentação

  1. Humano faz o merge após aprovação


  1. Claude atualiza documentação técnica


  1. Notion é atualizado com status da tarefa


## Matriz de Responsabilidades


## Regras Críticas para Todas as IAs

  As seguintes regras se aplicam a Codex, Copilot e Claude em todos os projetos:

  - Nunca sugerir commit direto na main


  - Sempre criar branch com prefixo temático (feat/, fix/, refactor/)


  - Toda mudança relevante deve passar por PR


  - Tratar como sensível qualquer alteração em: schema Parquet, chaves de join, agrupamento de produtos, conversão de unidades, movimentação de estoque, cálculos mensais/anuais


  - Preservar id_agrupado, id_agregado e __qtd_decl_final_audit__ como chaves invariantes


  - Explicitar riscos antes de implementar mudanças sensíveis


  - Registrar lineage e metadados em cada dataset materializado


## Configuração dos Arquivos de Instrução

### audit_pyside

  - AGENTS.md — instruções globais para Codex e Claude


  - src/transformacao/AGENTS.md — escopo de transformação


  - src/interface_grafica/AGENTS.md — escopo de GUI PySide6


  - tests/AGENTS.md — escopo de testes


  - docs/AGENTS.md — escopo de documentação


  - .claude/agent-index.md — mapeamento de agentes por pasta


### sistema_ro

  - AGENTS.md — instruções globais


  - backend/AGENTS.md — escopo FastAPI


  - frontend/AGENTS.md — escopo React/Tauri


  - pipeline/*/AGENTS.md — escopo por camada (extraction, normalization, mercadorias, conversao, estoque, fisconforme)


  - agentes_sistema_ro/ — agentes especializados por domínio (00 a 12)


  - .claude/agent-index.md — mapeamento de agentes por pasta


## 🔍 audit_pyside — Projeto

  Sistema de auditoria fiscal com interface PySide6, pipeline Python e backend FastAPI.

  Area de Regras

## Informações do Projeto

  Repositório: github.com/Enio-Telles/audit_pyside

  Status: Ativo — Fase de Otimização

  Stack: Python 3 / PySide6 / Polars / FastAPI / Parquet / Oracle

## Arquitetura


## Prioridades Atuais (Plano P0–P5 — 2026-Q2)

  > Sprint ativa: 2026-Q2 Otimização Arquitetura · Índice do plano: 📐 Plano P0–P5 · Documento-fonte: docs/plano_melhorias_backend_frontend_arquitetura.md

  Histórico (arquivado 2026-04-22): PRs #112 (Bolt vetorização) e #114 (Jules testes) já foram merged em main — as antigas T04/T05 foram concluídas. T01/T02/T03 continuam em aberto e foram realocadas no novo plano.

  P0 — Hot-fixes (1 dia, Codex + Copilot + Humano)

  - [ ] P0-01 — Remover duplicações em movimentacao_estoque.py (linhas 673-682, 702-789)


  - [ ] P0-02 — Corrigir .groupby → .group_by em movimentacao_estoque.py:118 (Polars 1.x)


  - [ ] P0-03 — Deduplicar test_q_conv_semantica_estoque.py (linhas 135-237)


  - [ ] P0-04 — Remover diretórios obsoletos docs copy/, src copy/, sql copy/, tests copy/


  - [ ] P0-05 — Executar docs/runbook_sync_repo.md (rebase, PRs, deletar 89 branches remotas)


  P1 — Consolidação & base de engenharia (1-2 semanas)

  - [ ] P1-01 — Consolidar 11 AGENTS.md em 4 (raíz + pipeline + backend + frontend)


  - [ ] P1-02 — Criar docs/README.md (índice mestre)


  - [ ] P1-03 — ADR-001: decidir futuro do backend FastAPI (manter ou remover)


  - [ ] P1-04 — Adotar pyproject.toml + uv


  - [ ] P1-05 — Configurar ruff + mypy + pre-commit


  - [ ] P1-06 — Workflow CI/CD (GitHub Actions)


  P2 — Backend (depende de P1-03 ADR-001)

  P3 — Frontend (decompor main_window.py de 10.366 linhas em módulos < 800)

  P4 — Testes & observabilidade (cobertura 80% pipeline / 60% frontend; structlog; golden files)

  P5 — Hardening (PyInstaller otimizado; sign & notarize; auto-update)

## Chaves Invariantes

  - id\_agrupado — chave mestra de produto


  - id\_agregado — alias de apresentação


  - \_\_qtd\_decl\_final\_audit\_\_ — valor de auditoria sem alterar saldo físico


## Instruções para IAs

  Leia AGENTS.md na raiz e no escopo da pasta antes de qualquer tarefa. Siga o formato de resposta: Objetivo, Contexto, Reaproveitamento, Arquitetura, Implementação, Validação, Riscos, MVP.

## Centralização da Documentação Técnica

  A documentação técnica do audit_pyside passa a ter uma wiki dedicada no Notion, alinhada à estratégia de múltiplos arquivos AGENTS.md e à necessidade de consolidar documentação viva por escopo.

### Estrutura criada

  - Wiki Técnica — audit_pyside


  - Área de Transformação — audit_pyside


  - Área de UI — audit_pyside


  - Área de Regras de Negócio — audit_pyside


### Regra de uso

  - src/transformacao/** deve atualizar a Área de Transformação.


  - src/interface_grafica/** deve atualizar a Área de UI.


  - alterações fiscais e funcionais críticas devem atualizar a Área de Regras de Negócio.


  - toda PR relevante deve refletir a mudança correspondente na wiki técnic/


  - /fi/


  ## 📚 Wiki Técnica — audit_pyside

## Objetivo

    Centralizar a documentação técnica do audit_pyside no Notion, espelhando a hierarquia operacional dos arquivos AGENTS.md e a estrutura do repositório.

## Estrutura da Wiki

    - Área de Transformação — lógica de processamento, pipeline fiscal, Polars, Numba, agregação, conversão e estoque.


    - Área de UI — interface PySide6, componentes visuais, serviços de UI, design High-Contrast Noir e padrões de experiência.


    - Área de Regras de Negócio — regras fiscais e operacionais críticas, incluindo C176 (ST), PMU baseado em preço de venda, inventário, devoluções e decisões funcionais.


## Subtópicos padrão de cada área

    Cada área desta wiki deve manter os seguintes blocos:

    1. Visão geral


    1. Decisões ativas


    1. Riscos conhecidos


    1. Prompts de implementação


    1. Checklist de validação


    1. Referências para preencher depois


## Relação com o repositório

    A wiki segue a divisão recomendada para documentação e AGENTS por escopo:

    - AGENTS.md na raiz


    - src/transformacao/AGENTS.md


    - src/interface_grafica/AGENTS.md


    - tests/AGENTS.md


    - backend/AGENTS.md (se o backend permanecer)


## Índice vivo

    Use esta página como ponto de entrada para o conhecimento técnico do projeto. As páginas filhas devem concentrar:

    1. visão arquitetural


    1. decisões ativas


    1. riscos conhecidos


    1. prompts de implementação


    1. checklist de validação por área


## Padrão de atualização

    Sempre que uma PR alterar comportamento, arquitetura, cálculo fiscal ou fluxo operacional, atualize a página correspondente desta wiki no mesmo ciclo da tarefa.

## Regra de manutenção

    Quando uma decisão técnica sair do README, de um AGENTS, de uma issue, de uma auditoria ou de uma PR, a versão viva deve ficar nesta wiki.

    ## ⚙️ Área de Transformação — audit_pyside

## Escopo

      Documentação técnica da camada src/transformacao/** e módulos relacionados ao pipeline fiscal.

## Visão geral

      Esta área centraliza a lógica de processamento vetorizado com Polars, o cálculo de saldos com Numba, a conversão de unidades, a agregação de produtos, a movimentação de estoque e a persistência analítica por parquet.

### Fluxo canônico

      item_unidades -> produtos_agrupados / map_produto_agrupado / produtos_final -> fatores_conversao -> movimentacao_estoque -> calculo_saldos -> calculos_anuais / calculos_mensais

## Decisões ativas

      - O pipeline canônico de agregação e transformação deve ser documentado aqui antes de qualquer refatoração estrutural.


      - Mudanças em id_agrupado, id_agregado, q_conv, q_conv_fisica e __qtd_decl_final_audit__ exigem atualização imediata desta página.


      - Toda alteração em benchmark, normalização, rastreabilidade, parquet ou contratos de dados deve ser registrada aqui com contexto e impacto.


## Riscos conhecidos

      - Divergência entre algoritmos de agrupamento e instabilidade de identificadores.


      - Default incorreto de unidade de referência afetando fator de conversão.


      - Regressões silenciosas em estoque, inventário e saldo ao mexer em conversão ou agregação.


      - Mudanças de performance sem benchmark de comparação.


## Regras e invariantes

      - Preservar rastreabilidade de id_agrupado e id_agregado.


      - Não alterar a semântica de q_conv, q_conv_fisica e __qtd_decl_final_audit__ sem documentar a motivação fiscal.


      - Todo cálculo deve manter coerência entre fluxo físico e fluxo observacional de auditoria.


      - Overrides manuais precisam sobreviver a reprocessamentos.


## Prompts de implementação

### Prompt de análise

      Leia os arquivos em src/transformacao/** e explique o impacto desta tarefa sobre o pipeline fiscal. Identifique riscos em agregação, conversão, estoque, rastreabilidade e contratos de dados.

### Prompt de implementação

      Implemente a alteração respeitando o pipeline canônico, preservando id_agrupado, id_agregado, q_conv, q_conv_fisica e __qtd_decl_final_audit__ quando aplicável. Não introduza atalhos fora do fluxo oficial.

### Prompt de revisão

      Revise a implementação procurando quebra de invariantes fiscais, regressão de parquet, inconsistência entre cálculo anual/mensal e impacto em conversão, agregação e estoque.

## Checklist de validação

      - [ ] Houve mudança em conversão de unidades?


      - [ ] Houve mudança em agregação ou critérios de vínculo?


      - [ ] Houve mudança em estoque, saldo, inventário ou devolução?


      - [ ] Houve mudança em contratos de parquet?


      - [ ] Foram adicionados ou ajustados testes de transformação?


      - [ ] Benchmark ou smoke test foi executado quando houve impacto de performance?


## Referências para preencher depois

      - Algoritmo canônico de agregação


      - Estratégia de overrides manuais


      - Benchmark base do núcleo crítico


      - Parquets obrigatórios por CNPJ


    ## 🖥️ Área de UI — audit_pyside

## Escopo

      Documentação técnica da camada src/interface_grafica/** e dos fluxos operacionais conduzidos pela interface PySide6.

## Visão geral

      Esta área concentra a documentação dos componentes PySide6, do main_window.py, dos painéis operacionais, dos serviços ligados à interface e do padrão visual High-Contrast Noir.

## Objetivo da área

      Reduzir acoplamento entre UI, regras de negócio e acesso a dados, deixando claro o papel de cada painel, serviço, worker e componente visual.

## Decisões ativas

      - A interface deve caminhar para uma decomposição por painéis, controllers e viewmodels.


      - A camada visual não deve concentrar regra de negócio fiscal.


      - O padrão visual High-Contrast Noir deve ser mantido como referência de contraste, legibilidade e consistência.


      - Toda mudança em navegação, sinais/slots, workers ou serviços acoplados à UI deve ser registrada aqui.


## Riscos conhecidos

      - Concentração excessiva de responsabilidades em arquivos grandes da UI.


      - Regras fiscais vazando para callbacks e handlers visuais.


      - Regressões em navegação e progresso de tarefas longas.


      - Estilo visual inconsistente entre painéis e componentes reutilizáveis.


## Componentes a documentar

      - painéis principais


      - diálogos críticos


      - widgets reutilizáveis


      - workers e sinais


      - serviços de UI e pontos de integração com o pipeline


      - tokens visuais do High-Contrast Noir


## Prompts de implementação

### Prompt de análise

      Analise a mudança proposta na interface PySide6. Identifique impacto em painéis, navegação, sinais, workers, serviços e consistência visual.

### Prompt de implementação

      Implemente a mudança de UI respeitando o padrão visual High-Contrast Noir e evitando concentrar regra de negócio dentro da camada visual.

### Prompt de revisão

      Revise a alteração procurando acoplamento indevido com regras fiscais, regressão em sinais/slots, quebra de navegação e inconsistências visuais.

## Checklist de validação

      - [ ] A PR alterou um painel ou fluxo operacional da UI?


      - [ ] A navegação continua consistente?


      - [ ] O padrão visual High-Contrast Noir foi preservado?


      - [ ] Houve vazamento de regra de negócio para a camada visual?


      - [ ] Há impacto em workers, progresso, cancelamento ou feedback visual?


      - [ ] Teste funcional ou smoke test da interface foi executado?


## Referências para preencher depois

      - mapa de painéis


      - padrão de workers e sinais


      - biblioteca de componentes reutilizáveis


      - guia visual do High-Contrast Noir


    ## 📘 Área de Regras de Negócio — audit_pyside

## Escopo

      Centralização da lógica fiscal e operacional que não deve ficar espalhada entre UI, helpers e serviços genéricos.

## Visão geral

      Esta área reúne as regras de negócio críticas do audit_pyside, com foco em coerência fiscal, rastreabilidade, interpretação correta dos eventos e estabilidade dos cálculos.

## Sistema de agregação

### Objetivo

      O sistema de agregação transforma descrições fiscais heterogêneas em uma chave mestre de produto, permitindo consolidar movimentações, conversões e análises por item lógico.

### Conceitos centrais

      - id_agrupado é a chave mestra do produto.


      - id_agregado é o alias de apresentação.


      - produtos_agrupados, map_produto_agrupado e produtos_final são os artefatos principais da agregação.


### Regra canônica

      A documentação desta área deve tratar como canônico o fluxo que consolida o agrupamento no artefato final do pipeline, evitando ambiguidade entre implementações alternativas.

### Regras operacionais

      - a agregação deve manter rastreabilidade entre descrição original, descrição normalizada, código-fonte e chave mestre


      - alterações no critério de agrupamento exigem revisão de impacto em conversão, estoque e relatórios


      - overrides e agrupamentos manuais não podem ser perdidos em reprocessamentos


      - a normalização precisa ser tratada como dependência crítica da estabilidade do id_agrupado


### Riscos específicos

      - mudança de normalização pode gerar reidentificação em massa de produtos


      - coexistência de abordagens diferentes de agrupamento pode gerar inconsistência histórica


      - agrupamento incorreto afeta diretamente fator de conversão, saldo e auditoria fiscal


### Incoerências conhecidas

      - Dualidade id_agrupado / id_agregado: a chave mestra é id_agrupado, mas id_agregado (alias de apresentação) circula em relatórios e na UI sem regra formal de quando cada uma deve aparecer. Isso permite que consumidores comparem identidades diferentes como se fossem a mesma.


      - Coexistência de algoritmos alternativos de agrupamento: o fluxo canônico (produtos_agrupados → map_produto_agrupado → produtos_final) convive com implementações paralelas que recalculam o agrupamento a partir da descrição normalizada, gerando id_agrupado divergentes entre módulos e entre execuções.


      - Múltiplos artefatos sem fonte de verdade explícita: produtos_agrupados, map_produto_agrupado e produtos_final coexistem sem regra clara de qual artefato cada consumidor (conversão, estoque, relatórios, GUI) deve ler. Divergências entre os três não são detectadas automaticamente.


      - Dependência instável da normalização: id_agrupado é derivado de descricao_normalizada, mas a normalização não é versionada. Ajustes em stopwords, acentuação, stemming ou regex reidentificam produtos em massa e quebram a rastreabilidade histórica sem aviso.


      - Fallback ambíguo por descrição na matriz de vínculo: a ordem id_item_unid → codigo_fonte → descricao_normalizada só restringe a descrição "quando não houver ambiguidade", mas não há definição operacional de ambiguidade nem log obrigatório quando o fallback textual é usado. Vínculos incorretos passam silenciosamente.


      - Overrides manuais sem garantia estrutural: a regra exige que overrides sobrevivam a reprocessamentos, mas o risco reaparece em várias áreas, indicando que a persistência depende de convenção e não de invariante do pipeline. Não há teste automatizado obrigando a preservação.


      - Propagação não isolada de mudanças de agrupamento: alterações de critério afetam conversão, saldo físico, PMU, inventário declarado e C176, mas não existe suíte end-to-end obrigatória cobrindo essa cadeia, então regressões só aparecem em auditoria manual.


      - Rastreabilidade assimétrica entre chaves: id_agrupado é tratado como invariante, enquanto id_agregado pode mudar entre execuções. Relatórios antigos que citam id_agregado deixam de casar com o artefato atual sem sinalização de quebra.


      - Ausência de definição canônica documentada: a regra canônica diz apenas "consolida o agrupamento no artefato final do pipeline", sem especificar algoritmo, critérios de similaridade e momento exato de emissão do id_agrupado — "definição canônica do algoritmo de agregação" segue listada como referência a preencher.


### Incoerências a investigar

      - [ ] Mapear todos os pontos do código que geram id_agrupado e confirmar se há uma única implementação efetivamente usada em produção.


      - [ ] Verificar se produtos_agrupados, map_produto_agrupado e produtos_final são sempre consistentes ao final do pipeline ou se divergem em execuções reais.


      - [ ] Medir quantos vínculos caem no fallback por descricao_normalizada e quantos disparam ambiguidade silenciosa.


      - [ ] Avaliar impacto de mudanças históricas de normalização sobre a estabilidade de id_agrupado entre reprocessamentos.


      - [ ] Confirmar se overrides manuais de agrupamento sobrevivem ao reprocessamento em todos os caminhos (GUI, CLI, reexecução parcial).


## Sistema de conversão de unidades

### Objetivo

      Converter quantidades de diferentes unidades para uma unidade de referência coerente, preservando rastreabilidade fiscal e integridade dos cálculos.

### Fluxo de negócio

      O sistema parte de item_unidades, vincula cada linha ao produto mestre e calcula fatores de conversão com base no preço médio e na unidade de referência.

### Prioridade de vínculo

      A regra de vínculo deve seguir a ordem de robustez do projeto:

      1. id_item_unid


      1. codigo_fonte via map_produto_agrupado


      1. descricao_normalizada apenas como fallback quando não houver ambiguidade


### Regras operacionais

      - a conversão deve preservar valor econômico e coerência fiscal, não apenas quantidade nominal


      - a unidade de referência precisa representar a unidade operacional correta do item


      - ajustes manuais de fator e unidade de referência devem sobreviver a reprocessamentos


      - divergências de vínculo devem ser auditáveis e registráveis


### Campos críticos

      - q_conv representa a quantidade convertida observada no fluxo


      - q_conv_fisica representa a quantidade convertida que pode afetar saldo físico


      - __qtd_decl_final_audit__ representa o valor declarado de estoque final para auditoria sem alterar o saldo físico


### Riscos específicos

      - unidade de referência mal escolhida distorce fator de conversão


      - fallback por descrição ambígua pode mascarar erro de vínculo


      - alterações em conversão podem contaminar saldo, PMU e diferenças entre declarado e físico


## Sistema de estoque

### Objetivo

      Produzir saldo físico e auditoria de estoque a partir das movimentações convertidas, preservando separação entre fluxo operacional e valor observacional de inventário.

### Componentes centrais

      - movimentacao_estoque aplica vínculo, fator, deduplicação e geração de eventos


      - calculo_saldos consolida o saldo anual e por período


      - cálculos anuais e mensais consomem o resultado para análise fiscal


### Regras operacionais

      - o sistema deve diferenciar claramente entrada, saída, estoque inicial e estoque final


      - inventário final declarado deve ser observado sem contaminar o saldo físico


      - movimentações duplicadas precisam ser neutralizadas pela chave documental apropriada


      - eventos sintéticos de estoque inicial e final devem ser identificados de forma rastreável


### Regras de interpretação

      - saldo físico e valor declarado não são a mesma coisa e não devem ser confundidos


      - divergência entre declarado e físico precisa ser explicável e auditável


      - o cálculo de omissão de entradas depende de conversão correta e vínculo correto


      - qualquer mudança em filtro por fonte, devolução, neutralização ou inventário precisa ser tratada como mudança de regra de negócio


### Riscos específicos

      - filtro de fonte mal documentado pode descartar entradas válidas


      - deduplicação incorreta pode gerar falsa omissão ou saldo indevido


      - mudança em estoque final observacional pode distorcer análises anuais e mensais


## Regras prioritárias adicionais

      - Registro C176 (ST)


      - cálculo de PMU baseado em preço de venda


      - critérios de neutralização, devolução e inventário


      - interpretação de diferenças entre declarado e físico


## Decisões ativas

      - Regras fiscais críticas devem ser documentadas aqui antes de serem redistribuídas em serviços ou pipeline.


      - Toda mudança em agregação, conversão, estoque, PMU, ST, inventário, devolução, saldo ou cálculo mensal/anual deve gerar atualização imediata desta página.


      - Casos de exceção e premissas precisam ficar explícitos para revisão por IA e por humanos.


## Invariantes de domínio

      - O cálculo fiscal precisa preservar coerência entre eventos de entrada, saída, estoque inicial e estoque final.


      - A agregação precisa manter rastreabilidade suficiente para explicar a composição de um produto mestre.


      - A conversão de unidades não pode quebrar coerência entre valor, quantidade e unidade de referência.


      - Regras de ST e C176 não podem ser tratadas como detalhe visual ou auxiliar.


      - PMU deve respeitar a lógica baseada em preço de venda quando essa for a regra validada do domínio.


      - Qualquer divergência entre declarado e físico deve ser explicável e rastreável.


## Prompts de implementação

### Prompt de análise

      Explique qual parte do sistema de agregação, conversão de unidades ou estoque é afetada por esta tarefa. Indique impacto fiscal, efeitos colaterais, arquivos envolvidos e risco de regressão.

### Prompt de implementação

      Implemente a mudança preservando coerência fiscal, rastreabilidade e compatibilidade com os relatórios anuais e mensais. Explicite premissas, exceções e efeitos sobre agregação, vínculo, fatores, inventário, saldo, C176 e PMU.

### Prompt de revisão

      Revise a alteração procurando inconsistência de regra fiscal, conflito entre módulos, quebra de invariantes, perda de rastreabilidade e ausência de cenários de teste de borda.

## Checklist de validação

      - [ ] A PR alterou regra de agregação?


      - [ ] A PR alterou vínculo ou fator de conversão?


      - [ ] A PR alterou cálculo de PMU?


      - [ ] A PR alterou C176 ou lógica de ST?


      - [ ] A PR mudou interpretação de inventário, devolução ou saldo?


      - [ ] Foram adicionados exemplos e cenários de borda?


      - [ ] A mudança foi refletida em testes anuais e mensais quando necessário?


## Exemplos práticos

### Exemplo 1 — Agregação de produto

      Duas descrições fiscais próximas, como um item com variação textual pequena e mesma identidade operacional, precisam convergir para um mesmo produto mestre quando o vínculo canônico permitir isso. O objetivo não é apenas normalizar texto, mas consolidar a rastreabilidade do item em id_agrupado e id_agregado, preservando explicação de origem, descrição normalizada e artefatos finais.

### Exemplo 2 — Conversão de unidade

      Um produto pode aparecer em unidades diferentes, como UN e CX. O sistema deve escolher uma unidade de referência coerente, vincular corretamente o item ao produto mestre e calcular o fator de conversão preservando valor econômico. Se houver ajuste manual de fator ou de unidade de referência, esse ajuste deve sobreviver ao reprocessamento.

### Exemplo 3 — Estoque declarado vs. físico

      O estoque final declarado precisa ser observado para auditoria sem contaminar o saldo físico. Por isso, o sistema diferencia quantidade convertida observacional, quantidade convertida que afeta saldo e valor declarado para auditoria. Quando houver divergência entre declarado e físico, a diferença precisa ser explicável e auditável, e não simplesmente absorvida pelo saldo.

## Casos de teste de negócio

### CT01 — Agregação canônica de produto mestre

      Regra vinculada: sistema de agregação

      Cenário: o mesmo item aparece com pequenas variações textuais, mas representa o mesmo produto operacional.

      O que deve acontecer:

      - o sistema precisa convergir para um mesmo id_agrupado quando o vínculo canônico permitir isso


      - a rastreabilidade deve continuar explicável por descrição normalizada, origem e artefatos finais


      - overrides e agrupamentos manuais não podem ser perdidos


      Falha que este caso tenta evitar: reidentificação indevida de produto, quebra histórica de agrupamento e impacto em conversão ou estoque.

      Tarefa de implementação associada: criar ou manter teste cobrindo estabilidade de id_agrupado e preservação de rastreabilidade em cenários com descrição próxima.

### CT02 — Conversão coerente entre unidades

      Regra vinculada: sistema de conversão de unidades

      Cenário: o mesmo produto aparece em unidades diferentes, como UN e CX, exigindo escolha de unidade de referência e cálculo de fator.

      O que deve acontecer:

      - o vínculo deve priorizar id_item_unid, depois codigo_fonte, e só então descricao_normalizada sem ambiguidade


      - o fator deve preservar coerência econômica e fiscal


      - ajustes manuais de fator e unidade de referência devem sobreviver ao reprocessamento


      Falha que este caso tenta evitar: fator distorcido, vínculo incorreto, perda de override manual e contaminação de saldo.

      Tarefa de implementação associada: criar ou manter teste cobrindo vínculo prioritário, persistência de override manual e comportamento com unidades diferentes.

### CT03 — Estoque declarado versus saldo físico

      Regra vinculada: sistema de estoque

      Cenário: existe inventário final declarado que precisa ser auditado sem alterar o saldo físico calculado.

      O que deve acontecer:

      - q_conv pode registrar valor observacional


      - q_conv_fisica deve preservar a lógica do saldo físico


      - __qtd_decl_final_audit__ deve capturar o estoque final declarado sem contaminar o cálculo do saldo


      - divergências entre declarado e físico precisam ser explicáveis e auditáveis


      Falha que este caso tenta evitar: mistura entre inventário declarado e saldo físico, gerando resultado fiscal enganoso.

      Tarefa de implementação associada: criar ou manter teste cobrindo separação entre fluxo observacional de auditoria e saldo físico real.

## Referências para preencher depois

      - regra consolidada do C176 (ST)


      - regra consolidada do PMU por preço de venda


      - definição canônica do algoritmo de agregação


      - matriz de vínculo e fallback da conversão


      - tabela de invariantes por tipo de evento


  ## 📐 Plano P0–P5 — Otimização de Backend, Frontend e Arquitetura

    > Página-índice da Sprint 2026-Q2 Otimização Arquitetura. Resumo executivo do plano completo, que vive em docs/plano_melhorias_backend_frontend_arquitetura.md no repositório.

## Documentos-fonte

    - Auditoria técnica — docs/auditoria_conversao_agregacao_estoque.md (8 riscos R1–R8, 10 recomendações).


    - Plano de melhorias — docs/plano_melhorias_backend_frontend_arquitetura.md (10 seções, P0–P5).


    - Runbook de sincronização — docs/runbook_sync_repo.md (rebase + PRs + limpeza de 89 branches obsoletos).


## Estado do repositório em 2026-04-22

    - 0 PRs abertos no GitHub.


    - 5 commits órfãos locais na branch feat/aggregation-snapshots-retention (limpeza de snapshots), sem PR.


    - 89 branches remotas obsoletas vindas de PRs já merged (#111–#115 e anteriores).


    - Branch feat/aggregation-snapshots-retention está 14 commits atrás de origin/main.


    - Ruído de CRLF no git status (~462 arquivos) — line-ending mismatch, não alterações reais.


    - 2 documentos novos não commitados produzidos nesta sprint: auditoria + plano.


## Fases do plano

### P0 — Hot-fixes (1 dia)

    - P0-01 Remover duplicações em movimentacao_estoque.py (linhas 673-682, 702-789).


    - P0-02 Corrigir .groupby → .group_by em movimentacao_estoque.py:118 (Polars 1.x).


    - P0-03 Deduplicar test_q_conv_semantica_estoque.py (linhas 135-237).


    - P0-04 Remover diretórios obsoletos docs copy/, src copy/, sql copy/, tests copy/.


    - P0-05 Executar runbook de sincronização do repo (rebase, PRs, deletar 89 branches).


### P1 — Consolidação & base de engenharia (1–2 semanas)

    - P1-01 Consolidar os 11 AGENTS.md em 4 (raíz + pipeline + backend + frontend).


    - P1-02 Criar docs/README.md — índice mestre da documentação.


    - P1-03 ADR-001: decidir futuro do backend FastAPI (manter evoluindo ou remover stub).


    - P1-04 Adotar pyproject.toml + uv (substitui requirements*.txt).


    - P1-05 Configurar ruff + mypy + pre-commit.


    - P1-06 Workflow CI/CD (GitHub Actions) com matriz de testes.


### P2 — Backend (após ADR-001)

    Depende da decisão P1-03. Se manter: multi-tenant, auth, endpoints de sync. Se remover: excluir src/audit_pyside/backend/ e referências.

### P3 — Frontend (2–3 semanas)

    Decompor main_window.py (10.366 linhas) em módulos < 800 linhas cada:

    - windows/main_window.py (orquestração).


    - windows/aba_importacao.py, aba_auditoria.py, aba_agregacao.py, aba_relatorios.py.


    - controllers/ para lógica de cada aba.


    - widgets/ para componentes reutilizáveis.


### P4 — Testes & observabilidade

    - Cobertura alvo: 80% em pipeline/, 60% em frontend/.


    - structlog + contexto por execução.


    - Golden files para hashes de agregação.


### P5 — Hardening

    - Empacotamento PyInstaller otimizado (exclude wheels de ML não usadas).


    - Sign & notarize do executável Windows.


    - Auto-update (Squirrel ou equivalente).


## Divisão de responsabilidade entre IAs


## Links

    - Sprint ativa: 2026-Q2 Otimização Arquitetura


    - Projeto: 🔍 audit_pyside — Projeto


    - Repositório: https://github.com/Enio-Telles/audit_pyside


## ⚙️ sistema_ro — Projeto

  Sistema de reconciliação e operações fiscais com arquitetura em camadas e múltiplos agentes de IA.

## Informações do Projeto

  Repositório: github.com/Enio-Telles/sistema_ro

  Status: Implementação Ativa

  Stack: Python 3 / FastAPI / React / Tauri / Polars / Oracle / Parquet

## Arquitetura em Camadas


## Issues e Branches Ativas

  Issue #25 (aberta): Planejar correção incremental de agregação, conversão e estoque

  Branches em andamento:

  - feat/fix-fisconforme-current-v5 — FisConforme v5


  - feat/expose-operational-surface-index — Superfície operacional


  - feat/ci-tests-and-frontend-docs — CI/CD e documentação


  - feature/stitch-mcp — Integração MCP Stitch


  - docs/plano-correcao-gold-mercadorias-conversao-estoque — Plano de correção


## Princípios Fundamentais

  - Cache-first e Bronze-first: Consultar Parquet antes de reexecutar extrações Oracle


  - Oracle apenas para extração: Toda transformação em Polars


  - Chaves invariantes: Preservar id\_agrupado, id\_agregado, \_\_qtd\_decl\_final\_audit\_\_


  - Lineage obrigatório: Registrar origem, filtros, período, CNPJ em cada dataset


  - Resposta A–E: Diagnóstico, Reaproveitamento, Decisão, Justificativa, Plano


## Agentes Especializados

  Os arquivos em agentes\_sistema\_ro/ definem 13 agentes especializados (00 a 12) para cada domínio do sistema, mais agentes de bugs, execução e integração entre agentes.

## 📚 Documentação e Guia de Uso

  Guia completo de uso do sistema de gerenciamento de projetos com IA cooperativa.

## Como Usar Este Hub

  Este hub no Notion é o ponto central de gerenciamento dos projetos GitHub. Ele está organizado em 6 seções principais:


## Fluxo de Trabalho Diário

### Início do Dia

  1. Abrir o banco Tarefas e Issues filtrado por Status = "Em Andamento"


  1. Verificar Pull Requests abertas e status de revisão


  1. Escolher a tarefa de maior prioridade (P0 > P1 > P2)


### Durante o Desenvolvimento

  1. Atualizar o Status da tarefa para "Em Andamento"


  1. Preencher o campo Branch com o nome da branch criada


  1. Registrar a IA Responsável pela implementação


  1. Usar o protocolo do Fluxo de IA Cooperativa para cada fase


### Ao Abrir uma PR

  1. Criar entrada no banco Pull Requests com URL da PR


  1. Marcar Revisado por IA após revisão do Claude/Copilot


  1. Atualizar Status para "Em Revisão"


### Ao Mergear uma PR

  1. Atualizar Status da PR para "Merged"


  1. Atualizar Status da Tarefa para "Concluído"


  1. Atualizar o Progresso do Sprint correspondente


## Configuração das IAs nos Projetos

### Como Ativar o Codex (OpenAI)

  O Codex usa arquivos AGENTS.md como instruções por escopo de pasta. Ambos os projetos já estão configurados:

  - Abra o projeto no OpenAI Codex


  - O Codex lerá automaticamente o AGENTS.md da pasta raiz e subpastas


  - Para tarefas específicas, mencione a camada: "Implemente na camada src/transformacao/"


### Como Ativar o GitHub Copilot

  O Copilot funciona diretamente no VS Code:

  - Instale a extensão GitHub Copilot e GitHub Copilot Chat


  - Para instruções de projeto, crie .github/copilot-instructions.md (já existe como AGENTS.md)


  - Use Copilot Chat para análises e inline suggestions para completar código


  - Para revisão de PRs, use Copilot Code Review na interface do GitHub


### Como Ativar o Claude Code

  O Claude Code usa o arquivo .claude/agent-index.md para mapear contexto por pasta:

  - Instale o Claude Code (CLI ou extensão VS Code)


  - O arquivo .claude/agent-index.md já está configurado em ambos os projetos


  - Claude lerá o AGENTS.md correto baseado na pasta onde você está trabalhando


  - Para análises de PR, use: claude review PR #<número>


## Script de Sincronização GitHub → Notion

  Para sincronizar manualmente issues e PRs do GitHub com o Notion, use o script abaixo no terminal:

```bash
# Listar issues abertas
gh issue list -R Enio-Telles/audit_pyside
gh issue list -R Enio-Telles/sistema_ro

# Listar PRs abertas
gh pr list -R Enio-Telles/audit_pyside
gh pr list -R Enio-Telles/sistema_ro
```

  Após verificar, atualize manualmente as entradas nos bancos de dados do Notion.

## Convenções de Nomenclatura


## Adicionando Novos Projetos

  Para adicionar um novo projeto ao hub:

  1. Adicionar entrada no banco Projetos com todas as propriedades


  1. Criar uma nova Página de Projeto dentro deste hub


  1. Configurar AGENTS.md na raiz do repositório GitHub


  1. Configurar .claude/agent-index.md para Claude Code


  1. Criar o primeiro Sprint no banco Sprints e Milestones



