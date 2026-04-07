---
goal: Melhorar performance, UX e visualização de tabelas do repositório audit_react
version: 1.0
date_created: 2026-04-06
last_updated: 2026-04-06
owner: Enio Telles
status: Planned
tags: [react, fastapi, performance, ux, tabelas, otimização]
---

# Plano completo de melhorias e otimização — `audit_react`

## 1. Resumo executivo

O projeto já tem uma base boa: React 19 + Vite + TypeScript no frontend, FastAPI no backend, React Query para cache, Zustand para estado e TanStack Table para tabelas principais.

O problema não é falta de base técnica. O problema é que há gargalos de arquitetura e consistência que limitam a fluidez da execução e a experiência do usuário:

- partes importantes ainda carregam dados demais de uma vez;
- a estratégia de cache do backend não está aproveitando o potencial do serviço de parquet;
- a camada de tabelas está parcialmente padronizada, mas ainda fragmentada;
- faltam recursos de visualização analítica mais ricos;
- faltam testes, documentação técnica real e critérios de observabilidade.

A meta deste plano é atacar quatro frentes ao mesmo tempo:

1. **reduzir tempo de resposta**;
2. **melhorar renderização de tabelas grandes**;
3. **padronizar a experiência entre telas**;
4. **acrescentar recursos de visualização e interação que facilitem análise operacional**.

---

## 2. Diagnóstico atual

## 2.1 Pontos fortes já existentes

- Backend e frontend já estão separados de forma clara.
- A aba `Consulta` já usa paginação server-side, ordenação server-side, filtros, exportação CSV e preferências de colunas.
- O componente compartilhado `DataTable` já suporta:
  - ordenação;
  - redimensionamento de colunas;
  - reordenação por drag and drop;
  - ocultação de colunas;
  - filtros por coluna;
  - destaque por regras.
- O projeto já usa `@tanstack/react-query`, `@tanstack/react-table` e `zustand`, o que facilita evoluir sem reescrever tudo.

## 2.2 Principais problemas encontrados

### A. Cache do backend pouco eficaz

O router de parquet instancia `ParquetService()` dentro da requisição. Como os caches (`_schema_cache`, `_count_cache`, `_page_cache`, `_dataset_cache`) ficam dentro da instância, eles tendem a ser descartados a cada request.

**Impacto:** perda de performance em navegação repetida, paginação, reaplicação de filtros e ordenação.

### B. Carregamento inicial maior do que o necessário

`App.tsx` importa todas as tabs diretamente. Isso impede code splitting real e faz o app carregar mais código logo no início.

**Impacto:** pior tempo de boot, principalmente em máquinas mais fracas.

### C. Tabelas grandes ainda podem travar a UI

A aba `Conversao` busca até **2000 linhas** em uma única consulta e usa uma tabela customizada sem virtualização. A aba SQL também executa consulta e retorna o resultado inteiro de uma vez.

**Impacto:** re-render pesado, scroll menos fluido, risco de travamento e sensação de lentidão.

### D. Inconsistência entre telas

Parte do projeto usa `DataTable`, mas outras telas mantêm tabelas próprias com comportamento parecido.

**Impacto:** manutenção mais difícil, UX desigual e retrabalho.

### E. Polling ainda pode ser mais inteligente

Há polling no painel lateral e na aba de logs. Isso funciona, mas ainda é uma solução mais cara do que o necessário.

**Impacto:** uso desnecessário de rede/CPU e atualização menos elegante do que poderia ser.

### F. Falta camada de visualização analítica

Hoje o foco é tabela tabular. Ainda faltam recursos como:

- visão resumida por indicadores;
- agrupamentos dinâmicos;
- pivot simplificado;
- gráficos rápidos;
- comparações entre linhas;
- salvamento de views.

**Impacto:** o usuário consegue consultar, mas ainda não consegue explorar os dados da forma mais produtiva.

### G. Lacunas de qualidade e manutenção

- `frontend/README.md` ainda está praticamente no template padrão do Vite.
- Há setup de testes, mas não há suíte real relevante no repositório indexado.
- Há indício de correção imediata necessária no `LeftPanel` por uso de `selectedFile` / `setSelectedFile` sem estar claro no destructuring atual.

**Impacto:** onboarding mais difícil, risco maior de regressão e manutenção menos previsível.

---

## 3. Objetivos do plano

## 3.1 Objetivos técnicos

- Reduzir tempo de resposta percebido nas consultas principais.
- Diminuir re-renderizações desnecessárias no frontend.
- Tornar o cache efetivo no backend.
- Padronizar a camada de tabelas.
- Limitar payloads excessivos.
- Preparar base para crescimento com menos retrabalho.

## 3.2 Objetivos de experiência do usuário

- Tornar navegação e leitura mais rápidas.
- Tornar filtros e seleção mais intuitivos.
- Facilitar comparação, agrupamento e inspeção de dados.
- Permitir salvar preferências úteis de visualização.
- Melhorar feedback visual de carregamento, sucesso, erro e processamento.

## 3.3 Objetivos de produto

- Fazer o sistema parecer mais “ferramenta de análise” e menos “grade de dados crua”.
- Reduzir cliques repetitivos.
- Dar ao usuário caminhos curtos para descobrir anomalias, outliers e inconsistências.

---

## 4. Estratégia geral

A recomendação é executar em cinco ondas:

1. **Correções imediatas e estabilização**.
2. **Performance estrutural backend + frontend**.
3. **Padronização da camada de tabelas**.
4. **Funcionalidades novas de visualização**.
5. **Qualidade, testes e observabilidade**.

---

## 5. Roadmap detalhado

## Fase 0 — Correções imediatas e higiene do projeto

### Objetivo
Eliminar problemas evidentes antes de otimizar profundamente.

### TODO

- [ ] Revisar `frontend/src/components/layout/LeftPanel.tsx` e corrigir uso de `selectedFile` / `setSelectedFile` no store.
- [ ] Validar se todas as tabs compilam sem erros de TypeScript no estado atual.
- [ ] Substituir o `frontend/README.md` por documentação real do projeto.
- [ ] Criar `docs/arquitetura.md` com visão rápida de frontend, backend, rotas e fluxo de dados.
- [ ] Padronizar nomenclatura com acentos/sem acentos nos labels visuais.
- [ ] Revisar textos de botões, placeholders e títulos para reduzir ambiguidade.
- [ ] Criar um checklist de smoke test manual por aba.

### Entregável

Projeto mais estável para seguir com otimização sem empilhar débito técnico.

---

## Fase 1 — Performance estrutural do backend

### Objetivo
Fazer o backend responder mais rápido e com cache realmente útil.

### TODO

- [ ] Transformar `ParquetService` em instância singleton compartilhada ou provedor memoizado por processo.
- [ ] Garantir que os caches de schema, count, páginas e dataset persistam entre requests.
- [ ] Adicionar invalidacão clara de cache quando parquet for alterado.
- [ ] Medir tempo de `schema`, `count`, `collect_page` e tempo total por endpoint de parquet.
- [ ] Criar endpoint opcional de metadados da tabela com:
  - total de linhas;
  - colunas;
  - tipos;
  - amostra;
  - estatísticas básicas.
- [ ] Adicionar limite configurável de tamanho de payload por resposta.
- [ ] Implementar paginação real e limite de linhas para execução SQL (`/api/sql/execute`).
- [ ] Criar endpoint separado para exportação completa SQL/Parquet, sem misturar com a visualização interativa.
- [ ] Revisar rotas de estoque/conversão/agregação para evitar cargas muito grandes em memória.
- [ ] Padronizar resposta paginada do backend em todas as tabelas grandes.
- [ ] Preparar opção de “resumo antes do detalhe” para datasets muito grandes.

### Recomendação técnica

- Instância compartilhada do `ParquetService` no módulo do router.
- Uso consistente de `page`, `page_size`, `sort_by`, `sort_desc`, `filters` em todas as rotas tabulares.
- Limite padrão sugerido:
  - visualização interativa: **100 a 300 linhas** por página;
  - exportação: endpoint dedicado.

### Entregável

Backend mais previsível, mais rápido e sem desperdiçar cache.

---

## Fase 2 — Performance e arquitetura do frontend

### Objetivo
Diminuir custo de renderização e melhorar velocidade percebida.

### TODO

- [ ] Aplicar `React.lazy` + `Suspense` nas tabs principais.
- [ ] Quebrar chunks por domínio: `consulta`, `sql`, `agregacao`, `conversao`, `estoque`, `logs`, `fisconforme`.
- [ ] Criar skeletons de carregamento em vez de apenas “Carregando...”.
- [ ] Revisar queries do React Query para padronizar:
  - `staleTime`;
  - `gcTime`;
  - `placeholderData`;
  - `enabled`;
  - retry.
- [ ] Evitar polling contínuo quando a tela não estiver ativa.
- [ ] Centralizar polling do pipeline em um hook dedicado (`usePipelineStatus`).
- [ ] Avaliar troca de polling por SSE em fase posterior, se necessário.
- [ ] Reduzir re-render por props volumosas com `memo`, `useMemo` e composição melhor de estado.
- [ ] Separar store Zustand por domínio:
  - app shell;
  - consulta;
  - pipeline;
  - preferências visuais.
- [ ] Sincronizar parte do estado da UI com URL (`tab`, `cnpj`, `arquivo`) para navegação mais amigável.
- [ ] Criar feedbacks de carregamento por ação:
  - carregando tabela;
  - recarregando;
  - salvando;
  - exportando;
  - processando pipeline.

### Recomendação técnica

Adicionar `@tanstack/react-virtual` para tabelas com grande quantidade de linhas/colunas.

### Entregável

Aplicação mais leve na abertura e mais fluida durante uso contínuo.

---

## Fase 3 — Padronização da camada de tabelas

### Objetivo
Criar uma experiência única de tabela no sistema inteiro.

### Decisão recomendada

Evoluir o `DataTable` para um componente mais robusto e migrar gradualmente as telas customizadas para ele, em vez de manter várias implementações paralelas.

### TODO

- [ ] Evoluir `DataTable` para um `DataTablePro` compartilhado.
- [ ] Migrar `ConversaoTab` para a base compartilhada, preservando edição inline.
- [ ] Revisar `ConsultaSqlTab` para suportar paginação/virtualização.
- [ ] Criar API unificada de tabela com suporte a:
  - paginação server-side;
  - paginação client-side;
  - ordenação local/server;
  - filtros locais/server;
  - seleção de linhas;
  - destaque por regra;
  - colunas fixas;
  - detalhes por linha;
  - exportação.
- [ ] Adicionar densidade visual configurável:
  - compacta;
  - padrão;
  - confortável.
- [ ] Adicionar pin/freeze de colunas à esquerda.
- [ ] Adicionar redimensionamento com duplo clique para “auto-fit”.
- [ ] Adicionar menu contextual por coluna.
- [ ] Adicionar busca rápida global na tabela.
- [ ] Permitir salvar views por tela:
  - colunas visíveis;
  - ordenação;
  - filtros;
  - densidade;
  - largura;
  - colunas fixas.
- [ ] Criar drawer lateral de detalhes da linha para evitar depender de tooltip truncado.
- [ ] Adicionar comparação entre 2 ou mais linhas selecionadas.

### Entregável

Uma única base de tabela para todo o produto, com menos retrabalho e UX consistente.

---

## Fase 4 — Novas funcionalidades de visualização de tabelas

### Objetivo
Ir além da grade crua e tornar a análise mais poderosa.

## 4.1 Funcionalidades prioritárias

### TODO — Visualização imediata

- [ ] Adicionar cartões de resumo acima das tabelas:
  - total de linhas;
  - quantidade filtrada;
  - soma de colunas numéricas-chave;
  - média;
  - contagem de nulos;
  - contagem de divergências.
- [ ] Adicionar painel de estatísticas da coluna selecionada.
- [ ] Adicionar “top valores” por coluna.
- [ ] Adicionar distribuição por faixa para campos numéricos.
- [ ] Adicionar heatmap visual leve para células numéricas.
- [ ] Adicionar destaque automático para:
  - nulos;
  - duplicados;
  - outliers simples;
  - divergência entre colunas relacionadas.
- [ ] Adicionar agrupamento por coluna com totalizadores.
- [ ] Adicionar subtotal por grupo.
- [ ] Adicionar visão “pivot simplificada” para análise rápida.
- [ ] Adicionar ordenação múltipla.
- [ ] Adicionar presets de filtro (“somente divergentes”, “somente nulos”, “somente manuais”, etc.).

## 4.2 Funcionalidades de exploração

### TODO — Exploração analítica

- [ ] Criar modo “Resumo” e modo “Detalhe” por tabela.
- [ ] Criar painel lateral “Insights rápidos”.
- [ ] Criar histórico de filtros recentes.
- [ ] Salvar filtros favoritos por usuário/navegador.
- [ ] Permitir duplicar view atual com outro conjunto de filtros.
- [ ] Adicionar comparação antes/depois para linhas editadas.
- [ ] Adicionar trilha visual para alterações manuais.

## 4.3 Recursos de exportação

### TODO — Exportação útil

- [ ] Manter CSV para página atual.
- [ ] Adicionar “Exportar página atual”.
- [ ] Adicionar “Exportar resultado filtrado”.
- [ ] Adicionar exportação XLSX com múltiplas abas quando aplicável.
- [ ] Adicionar exportação com layout amigável (cabeçalhos ajustados e tipos corretos).
- [ ] Exibir claramente quando exportação é parcial ou completa.

## 4.4 Recursos visuais opcionais

### TODO — Segunda onda

- [ ] Adicionar mini gráficos por coluna usando biblioteca leve (`recharts` ou `visx`).
- [ ] Criar barras inline em células numéricas para comparação visual.
- [ ] Adicionar gráficos rápidos acima da tabela quando fizer sentido:
  - barras;
  - linhas;
  - pizza apenas para distribuições pequenas;
  - séries temporais quando houver data.

### Entregável

Tabelas deixam de ser apenas consulta e passam a servir também para leitura analítica e tomada de decisão.

---

## Fase 5 — Usabilidade e experiência do usuário

### Objetivo
Tornar o sistema mais amigável, previsível e confortável para uso diário.

### TODO

- [ ] Melhorar landing page com atalhos para tarefas frequentes.
- [ ] Mostrar estado vazio útil em todas as telas.
- [ ] Mostrar mensagens de erro acionáveis, não genéricas.
- [ ] Adicionar onboarding curto para primeira utilização.
- [ ] Adicionar atalhos de teclado para ações comuns.
- [ ] Padronizar toolbar das telas com:
  - recarregar;
  - exportar;
  - filtros;
  - colunas;
  - salvar view.
- [ ] Melhorar acessibilidade:
  - foco visível;
  - navegação por teclado;
  - labels consistentes;
  - contraste revisado.
- [ ] Permitir esconder/mostrar painel lateral com persistência.
- [ ] Persistir aba ativa, CNPJ e último arquivo aberto.
- [ ] Adicionar confirmação visual para ações críticas.
- [ ] Exibir claramente origem e atualização dos dados.

### Entregável

Sistema mais confortável, mais autoexplicativo e mais profissional no uso contínuo.

---

## Fase 6 — Qualidade, testes e observabilidade

### Objetivo
Reduzir regressão e permitir evolução com segurança.

### TODO

- [ ] Criar testes unitários para `DataTable`, `ColumnToggle`, filtros e persistência de preferências.
- [ ] Criar testes de integração para fluxos principais:
  - selecionar CNPJ;
  - listar arquivos;
  - abrir tabela;
  - filtrar;
  - ordenar;
  - exportar.
- [ ] Criar testes de backend para rotas paginadas.
- [ ] Validar serialização segura de valores nulos, listas, `NaN` e `inf`.
- [ ] Criar benchmark simples para consultas parquet.
- [ ] Registrar métricas de performance por rota.
- [ ] Criar logging estruturado para falhas de consulta e exportação.
- [ ] Criar CI mínima com:
  - lint;
  - typecheck;
  - testes frontend;
  - testes backend.
- [ ] Criar documento `docs/performance.md` com baseline e metas.

### Entregável

Base mais confiável para evolução contínua.

---

## 6. Priorização prática

## P0 — Fazer primeiro

- [ ] Corrigir `LeftPanel`.
- [ ] Tornar `ParquetService` compartilhado entre requests.
- [ ] Aplicar lazy loading nas tabs.
- [ ] Colocar paginação real e limite de resultado na execução SQL.
- [ ] Reduzir carga da aba `Conversao` com paginação ou virtualização.
- [ ] Substituir README padrão por documentação real.
- [ ] Criar smoke tests básicos.

## P1 — Fazer logo depois

- [ ] Evoluir `DataTable` para componente padrão de alto nível.
- [ ] Migrar telas customizadas para a mesma base.
- [ ] Adicionar cartões de resumo e estatísticas por coluna.
- [ ] Adicionar exportação filtrada completa.
- [ ] Melhorar feedbacks visuais de loading/saving/error.
- [ ] Melhorar polling do pipeline.

## P2 — Segunda onda

- [ ] Pivot simplificado.
- [ ] Views salvas.
- [ ] Comparação entre linhas.
- [ ] Gráficos rápidos.
- [ ] SSE para pipeline, se houver necessidade real.

---

## 7. Arquivos prioritários para intervenção

### Frontend

- [ ] `frontend/src/App.tsx`
- [ ] `frontend/src/store/appStore.ts`
- [ ] `frontend/src/api/client.ts`
- [ ] `frontend/src/components/layout/LeftPanel.tsx`
- [ ] `frontend/src/components/table/DataTable.tsx`
- [ ] `frontend/src/components/table/ColumnToggle.tsx`
- [ ] `frontend/src/components/table/FilterBar.tsx`
- [ ] `frontend/src/hooks/usePreferenciasColunas.ts`
- [ ] `frontend/src/components/tabs/ConsultaTab.tsx`
- [ ] `frontend/src/components/tabs/ConsultaSqlTab.tsx`
- [ ] `frontend/src/components/tabs/ConversaoTab.tsx`
- [ ] `frontend/src/components/tabs/AgregacaoTab.tsx`
- [ ] `frontend/src/components/tabs/LogsTab.tsx`

### Backend

- [ ] `backend/main.py`
- [ ] `backend/routers/parquet.py`
- [ ] `backend/routers/sql_query.py`
- [ ] `src/interface_grafica/services/parquet_service.py`

### Documentação / qualidade

- [ ] `frontend/README.md`
- [ ] `FRONTEND.md`
- [ ] `docs/arquitetura.md`
- [ ] `docs/performance.md`
- [ ] `.github/workflows/ci.yml`

---

## 8. Critérios de aceite

## Performance

- [ ] Navegação inicial perceptivelmente mais rápida.
- [ ] Reabertura da mesma consulta usa cache de forma observável.
- [ ] Tabelas grandes não travam ao rolar.
- [ ] SQL não retorna payload gigante sem controle.

## UX

- [ ] Usuário entende rapidamente o que está acontecendo.
- [ ] Estados vazios, carregamento e erro estão claros.
- [ ] Preferências de visualização persistem de forma confiável.
- [ ] Experiência entre tabs é coerente.

## Visualização

- [ ] Tabelas oferecem leitura resumida e detalhada.
- [ ] Filtros, colunas, agrupamentos e exportação estão claros.
- [ ] Há pelo menos uma camada de insights visuais além da grade.

## Qualidade

- [ ] Projeto compila sem warnings críticos.
- [ ] Fluxos principais possuem testes automatizados.
- [ ] Documentação mínima de operação existe.

---

## 9. Sugestão de execução em sprints

## Sprint 1

- [ ] Correções imediatas.
- [ ] Singleton/cache do parquet.
- [ ] Lazy loading das tabs.
- [ ] Documentação real.
- [ ] Limite/paginação na SQL.

## Sprint 2

- [ ] Evolução do `DataTable`.
- [ ] Refatoração da `ConversaoTab`.
- [ ] Melhorias de polling.
- [ ] Skeletons e feedback visual.
- [ ] Testes iniciais.

## Sprint 3

- [ ] Cartões de resumo.
- [ ] Estatísticas por coluna.
- [ ] Exportação avançada.
- [ ] Views salvas.
- [ ] Comparação de linhas.

## Sprint 4

- [ ] Pivot simplificado.
- [ ] Mini gráficos.
- [ ] Observabilidade/CI.
- [ ] Refinos finais de UX.

---

## 10. Recomendação final

A melhor direção para este projeto não é reescrever. É **padronizar o que já está bom e remover os gargalos certos**.

A ordem mais inteligente é:

1. corrigir base e cache;
2. reduzir custo de carregamento;
3. consolidar a tabela compartilhada;
4. acrescentar visualização analítica;
5. fechar com testes e observabilidade.

Seguindo essa ordem, o sistema ganha velocidade real, melhora a experiência e fica pronto para crescer sem virar uma coleção de telas isoladas.
