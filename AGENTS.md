# AGENTS.md — audit_pyside (raiz)

Este repositório é uma **aplicação desktop Python/PySide6** com pipeline analítico-fiscal.
Não proponha arquitetura web, frontend React ou backend web, salvo se o usuário pedir
isso explicitamente para uma área já existente no código.

---

## Mapa de escopo

| Escopo | Arquivo | Consumidores |
|---|---|---|
| Raiz / transversal | `AGENTS.md` (este) | Claude, Codex, Copilot, Jules |
| Pipeline de transformação | `src/transformacao/AGENTS.md` | Claude, Codex, Jules |
| Interface gráfica PySide6 | `src/interface_grafica/AGENTS.md` | Claude, Copilot, Jules |

> **Backend:** removido (ADR-001 Opção B, 2026-04-22). Pipeline + GUI desktop são a única superfície.
> **Regra de precedência:** em caso de conflito, este arquivo prevalece sobre os escopados.
> Os arquivos escopados aprofundam; nunca contradizem este.

---

## For automated agents

Este repositorio e operado por varios agentes automatizados. Trate este arquivo
como a fonte versionada de regras do repositorio. A cadencia operacional viva fica
no Hub de Agentes do Notion.

```yaml
automated_agents:
  live_coordination:
    notion_hub: "https://app.notion.com/p/668e144a3db2423cb4bcf8683a34ff73"
  repository_rules:
    root: "AGENTS.md"
    transformacao: "src/transformacao/AGENTS.md"
    interface_grafica: "src/interface_grafica/AGENTS.md"
  async_agents:
    jules:
      provider: "Google"
      mode: "asynchronous coding agent"
      allowed_default_scope:
        - "docs/**"
        - "tests/**"
        - "scripts/**"
        - "src/interface_grafica/**"
      requires_human_authorization:
        - "src/transformacao/**"
        - "schema Parquet"
        - "read_only_files"
        - "fiscal invariants"
        - "release/signing/auto-update workflows"
  read_only_files:
    - "src/transformacao/rastreabilidade_produtos/_produtos_final_impl.py"
    - "src/transformacao/rastreabilidade_produtos/fatores_conversao.py"
    - "src/transformacao/movimentacao_estoque_pkg/calculo_saldos.py"
    - "src/transformacao/movimentacao_estoque_pkg/movimentacao_estoque.py"
    - "src/transformacao/fatores_conversao.py"
    - "src/transformacao/movimentacao_estoque.py"
  invariants:
    - "id_agrupado"
    - "id_agregado"
    - "__qtd_decl_final_audit__"
    - "q_conv"
    - "q_conv_fisica"
  required_local_validation:
    - "ruff check"
    - "mypy src/transformacao"
    - "pytest -q -m \"not gui and not gui_smoke\""
  gui_testing_policy:
    - "Testes GUI/PySide6 devem ficar em tests/ui/"
    - "Sempre definir QT_QPA_PLATFORM=offscreen no topo do arquivo de teste"
    - "Usar pytest.importorskip('PySide6') para evitar falhas de coleta"
    - "Testes GUI sao ignorados no Windows CI devido a instabilidade (0xc0000139)"
    - "Instalar via uv sync --group test-gui apenas em jobs dedicados"
  transformacao_perf_or_refactor_gate:
    - "differential test byte-a-byte sobre amostra real"
    - "ADR em docs/adr/ quando tocar regra fiscal"
    - "aprovacao humana explicita antes do merge"
  pr_title_format: "tipo(area-fase): descricao"
```

Regras para Codex, Copilot Agent, Jules e Antigravity:

- Nao altere semantica fiscal.
- Nao modifique os arquivos read-only listados acima.
- Nao modifique as 5 chaves invariantes listadas acima.
- Qualquer PR `perf` ou `refactor` em `src/transformacao/` exige differential test
  byte-a-byte sobre amostra real antes do merge. Use `run_harness` em
  `tests/diff_harness/run_harness.py` e anexe o `DifferentialReport` no corpo da PR.
  Veja `docs/diff-harness.md` para o passo a passo completo.
- PRs que tocam arquivos read-only precisam do label `differential-validated` para
  passar o check `diff-harness.yml`.
- Use titulos de PR no formato `tipo(area-fase): descricao`.
- Use PT-BR sem acentos em titulos de PR, corpos de PR e comentarios de review.

---

## Jules — agente assincrono de codificacao

Jules e tratado neste repositorio como agente assincrono de implementacao em background.
Ele pode executar tarefas complexas em uma VM isolada, abrir PRs, responder a comentarios
e aplicar commits corretivos, mas nao decide estrategia fiscal nem substitui aprovacao humana.

Use Jules para:

- PRs pequenas e bem delimitadas;
- correcoes de bugs com criterio de aceite claro;
- refactors mecanicos em multiplos arquivos;
- criacao ou expansao de testes;
- docstrings e documentacao operacional;
- limpeza de codigo sem mudanca fiscal;
- tarefas repetitivas com validacao objetiva.

Nao use Jules para decidir ou executar sem autorizacao humana:

- regra fiscal;
- alteracao de invariantes;
- mudanca de schema Parquet;
- alteracao em arquivos read-only;
- estrategia de arquitetura;
- politica de release, signing ou auto-update;
- merge automatico.

### Regras especificas para Jules

Antes de acionar Jules, o prompt deve conter:

1. Objetivo em uma frase.
2. Escopo permitido por arquivos ou pastas.
3. Arquivos proibidos.
4. Gates de validacao.
5. Criterio de pronto.
6. Instrucao explicita para manter o diff pequeno.
7. Instrucao para nao fazer merge.
8. Instrucao para priorizar reviews humanos sobre comentarios de bots.

### Escopo seguro para Jules

Jules pode atuar sem autorizacao extra em:

- `docs/**`
- `tests/**`
- `scripts/**`
- `src/interface_grafica/**`, exceto quando afetar calculo fiscal
- melhorias de CI nao destrutivas
- refactors de GUI sem regra fiscal
- testes unitarios e regressoes

Jules precisa de autorizacao humana explicita antes de tocar:

- `src/transformacao/**`
- qualquer schema Parquet
- qualquer uma das 5 invariantes fiscais
- qualquer arquivo listado em `read_only_files`
- workflows de release, assinatura ou auto-update
- mudancas que afetem reprocessamento ou preservacao de ajustes manuais

### Fluxo de cooperacao com Jules

1. Humano ou agente de triagem define a tarefa.
2. Prompt deve ser colado em issue ou PR com escopo fechado.
3. Jules gera plano e diff.
4. Revisar o plano antes de aceitar mudancas amplas.
5. Se Jules abrir PR, manter como draft ate passar validacao.
6. Comentarios humanos tem prioridade sobre comentarios de bots.
7. Se houver conflito entre Jules, CodeRabbit, Codex, Copilot ou Gemini, prevalece:
   1. humano responsavel;
   2. `AGENTS.md` raiz;
   3. `AGENTS.md` escopado;
   4. `docs/Plano_duck/`;
   5. comentario de bot.

### Padrao de prompt para Jules

```text
Leia AGENTS.md, src/transformacao/AGENTS.md e src/interface_grafica/AGENTS.md antes de alterar qualquer arquivo.

Objetivo:
<descrever em uma frase>

Escopo permitido:
- <arquivos/pastas permitidos>

Fora de escopo:
- nao alterar regra fiscal
- nao alterar schema Parquet
- nao alterar as invariantes: id_agrupado, id_agregado, __qtd_decl_final_audit__, q_conv, q_conv_fisica
- nao tocar arquivos read-only listados em AGENTS.md
- nao fazer merge

Validacao obrigatoria:
- ruff check
- ruff format --check
- pytest -q -m "not gui_smoke"
- se tocar src/transformacao: mypy src/transformacao
- se tocar src/transformacao em perf/refactor: differential harness real

Criterio de pronto:
- diff pequeno e revisavel
- testes focados adicionados ou atualizados
- descricao da PR explica objetivo, risco, validacao e rollback
- nenhum artefato temporario versionado
```

### Resposta esperada de Jules

Toda PR criada por Jules deve conter:
- objetivo;
- arquivos alterados;
- o que foi reaproveitado;
- validacao executada;
- riscos;
- rollback;
- proximos passos;
- confirmacao de que nao tocou arquivos read-only, quando aplicavel.

---

## Hierarquia de autoridade

1. Humano responsavel pelo repositorio.
2. Claude Code, somente quando autorizado, com ADR e differential test para regra fiscal.
3. Codex, GitHub Copilot Agent, Jules e Antigravity, restritos a escopo seguro e sem regra fiscal.

Quando houver conflito entre instrucoes, siga a ordem acima. Em caso de duvida sobre
regra fiscal, pare e solicite decisao humana antes de editar.

---

## Missão

Atue como agente técnico de implementação, revisão e planejamento com foco em:
- corretude funcional e fiscal
- rastreabilidade ponta a ponta
- reaproveitamento de código e dados
- estabilidade de contratos (schema Parquet, chaves de join)
- preservação de ajustes manuais entre reprocessamentos
- evolução segura e revisável do repositório

---

## Contexto do projeto

| Camada | Pasta | Responsabilidade |
|---|---|---|
| Extração / raw | `src/transformacao/` (etapa raw) | Captura dados de Oracle, CSV, Parquet sem transformação |
| Base | `src/transformacao/` (etapa base) | Normaliza tipos, nomes, remove duplicatas, define chaves |
| Curated | `src/transformacao/` (etapa curated) | Agrega, harmoniza, calcula métricas fiscais |
| Marts / views | `src/transformacao/` (etapa marts) | Expõe dados prontos para GUI e relatórios |
| Interface | `src/interface_grafica/` | Orquestração, consulta, revisão operacional (PySide6) |
| Testes | `tests/` | pytest — corretude fiscal, schema, regressão |
| SQL | `sql/` | Scripts de extração SQL (nunca inline no Python) |
| Documentação | `docs/` | Docs técnicos, ADRs, runbooks, referências |

Entrypoints:
- `app.py` — lançador padrão (`MainWindow`) — importado por `tests/test_app.py`
- `app_safe.py` — lançador com `SafeMainWindow` (shutdown seguro de workers)
- Orquestrador principal: `src/orquestrador_pipeline.py`

---

## Prioridades (ordem decrescente)

1. Corretude funcional e fiscal
2. Rastreabilidade ponta a ponta
3. Reaproveitamento (reutilize antes de criar)
4. Clareza arquitetural
5. Estabilidade de contratos
6. Manutenibilidade
7. Performance

---

## Regras centrais

- **Reutilize** módulos, wrappers, utilitários, datasets e telas antes de criar novos artefatos.
- **Não duplique** regra de negócio entre pipeline e interface.
- O pipeline Python (`src/transformacao/`) é a **fonte principal** da regra analítica e fiscal.
- A interface PySide6 deve **orquestrar, consultar e apoiar revisão** — nunca reimplementar cálculo fiscal.
- Preserve a trilha auditável da origem do documento até o total analítico final.
- **Cache-first**: prefira ler materializações Parquet existentes antes de reextrair do Oracle.
- **Polars sobre Oracle**: use Polars para joins, harmonizações, cálculos e agregações. Oracle é apenas fonte de extração inicial.
- **Logs e lineage**: cada pipeline deve registrar CNPJ, período, dataset de origem, filtros aplicados e — ao final — nome do dataset gerado, schema e data.

---

## Chaves invariantes (5 canônicas)

Preserve estas colunas em **todas** as etapas do pipeline, na GUI e nos testes.
Nunca sobrescreva, renomeie ou descarte sem análise de impacto completa:

| Chave | Papel |
|---|---|
| `id_agrupado` | Chave mestra de produto; formato canônico: `id_agrupado_auto_<sha1[:12]>` |
| `id_agregado` | Alias de apresentação quando existir |
| `__qtd_decl_final_audit__` | Valor de auditoria — não altera o saldo físico |
| `q_conv` | Quantidade convertida para unidade de referência |
| `q_conv_fisica` | Quantidade convertida na perspectiva física (estoque) |

---

## Mudanças sensíveis

Trate como **sensível** qualquer alteração que impacte:
- schema de Parquet (colunas, tipos, ordem)
- chaves de join ou agrupamento de produtos
- conversão de unidades (`q_conv`, `q_conv_fisica`)
- movimentação de estoque
- cálculos mensais ou anuais
- comportamento da GUI PySide6
- preservação de ajustes manuais

Em mudanças sensíveis:
1. Declare a seção explícita de **Riscos** e **Rollback**
2. Proponha validação antes e depois
3. Preserve compatibilidade quando possível
4. Abra PR separado — nunca misture com refatoração ampla

---

## Anti-padrões

- Inserir SQL ad hoc em scripts Python ou na GUI.
- Pular etapas do pipeline (ex.: escrever direto no curated sem passar pelo base).
- Duplicar lógica fiscal na interface e no pipeline.
- Alterar `id_agrupado`, `id_agregado` ou `__qtd_decl_final_audit__` sem propagar consequências a todas as camadas.
- Usar `.groupby()` — **proibido em Polars 1.x**; sempre usar `.group_by()`.
- Criar Parquet sem registrar schema ou origem.
- Executar lógica analítica no Oracle/banco em vez do Polars.
- Ignorar logs e lineage.
- Concentrar cálculo fiscal pesado na camada de interface.

---

## Documentação

Ao criar ou alterar documentação em `docs/`:
- Escreva de forma objetiva; documente apenas o que auxilia manutenção, revisão e operação.
- Vincule arquivos relacionados (ex.: um doc de pipeline pode referenciar o SQL e o módulo Python correspondentes).
- Quando houver breaking change, descreva explicitamente a transição.
- Quando houver reprocessamento, descreva impacto e estratégia de recuperação.
- Mantenha `docs/README.md` como índice vivo da documentação.
- ADRs seguem convenção: `docs/adr/NNNN-kebab-case.md`, template Michael Nygard.

---

## Testes

Ao escrever ou alterar testes em `tests/`:
- Proteja: corretude fiscal, rastreabilidade, compatibilidade de schema, regressões em estoque e cálculos, preservação de ajustes manuais.
- Cubra: movimentação de estoque, cálculos mensais/anuais, conversão de unidades, agrupamento de produtos, integração GUI/serviços em pontos críticos.
- Valide reconciliação entre camadas (totais no `base` devem bater com `raw`; métricas no `curated` representam corretamente os originais).
- Prefira testes pequenos e determinísticos; nomeie cenários de forma explícita.
- Cubra casos de borda (campos faltantes, unidades não cadastradas, valores zerados).
- Use fixtures com dados de exemplo representativos; evite depender de dados reais ou sensíveis.
- Escreva testes de performance para operações Polars sobre grandes volumes.
- Deixe claro o cenário fiscal/operacional protegido por cada teste.

---

## Git e revisão

- **Nunca** sugira commit direto na `main`.
- Prefira branches curtas e focadas:
  `feat/<modulo>-<objetivo>`, `fix/<modulo>-<problema>`, `chore/<escopo>`, `docs/<tema>`.
- Toda mudança relevante deve passar por PR.
- PRs devem ser pequenas, revisáveis em uma sessão, com objetivo claro.
- Não misture refatoração ampla com correção funcional crítica.
- Descrição da PR deve incluir: objetivo, camadas afetadas, datasets e contratos envolvidos, riscos e plano de rollback.

---

## Critério de pronto ("Done means")

Considere uma tarefa pronta apenas quando:
- O objetivo estiver atendido.
- O impacto em dados e contratos estiver claro.
- Os testes/validações adequados tiverem sido executados ou indicados.
- A mudança preservar rastreabilidade e compatibilidade razoáveis.
- Riscos remanescentes tiverem sido explicitados.

---

## Formato de resposta

Ao planejar, analisar ou executar qualquer tarefa, estruture a resposta em:

1. **Objetivo** — o que será feito e por quê.
2. **Contexto no audit_pyside** — qual área, camada e módulos envolvidos.
3. **Reaproveitamento** — o que já existe e pode ser reutilizado.
4. **Arquitetura** — decisão de design e camada adequada.
5. **Implementação** — passos concretos, ordem de PRs, comandos.
6. **Validação** — testes, reconciliação, checagem de schema.
7. **Riscos** — schema, fiscal, performance, rollback.
8. **MVP recomendado** — menor entrega funcional e segura.
