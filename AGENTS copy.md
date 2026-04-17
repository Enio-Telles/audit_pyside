# Agent – audit_pyside

Este arquivo define as instruções persistentes para o assistente de IA no repositório **audit_pyside**.  
Use estas diretrizes para planejar, analisar ou implementar funcionalidades, garantindo consistência, rastreabilidade e conformidade fiscal.

## Missão

- **Rastreabilidade fiscal ponta a ponta**: todas as transformações devem preservar a ligação entre a linha original do documento fiscal e quaisquer agregações ou métricas derivadas.
- **Reaproveitamento máximo**: antes de criar novo SQL, Parquet, função ou tela, verifique se já existe algo que atenda ao requisito. Evite duplicar código ou dados.
- **Integridade das chaves**: preserve e respeite as colunas `id_agrupado`, `id_agregado` e `__qtd_decl_final_audit__` em todas as etapas. Elas são fundamentais para o audit trail e reconciliação.

## Estrutura do projeto

- `src/transformacao/` – Pipelines em Python/Polars que extraem dados de fontes (Oracle, CSV, Parquet), normalizam tipos, agregam mercadorias e calculam métricas. **Siga sempre as camadas**: **raw** → **base** → **curated** → **marts/views**. Cada camada tem responsabilidade única. Não pule etapas.
- `src/interface_grafica/` – Aplicação **PySide6** que serve como interface operacional de desktop. Deve **consumir** datasets e serviços existentes, e **nunca duplicar** lógica fiscal ou analítica.
- `sql/` – Scripts SQL e manifests de extração. Armazene consultas reutilizáveis aqui. Não coloque SQL ad hoc no código Python.
- `docs/` – Documentação técnica, decisões arquiteturais, catálogos de datasets e metadados. Atualize-a sempre que alterar schemas ou processos.
- `tests/` – Testes automatizados de pipelines, reconciliações e interface. Use-os para garantir que regras críticas permaneçam corretas.

## Princípios gerais

### Reuso antes de criação

Antes de iniciar uma nova implementação, procure por SQLs, Parquets, funções ou módulos existentes que atendam ao requisito. Se a necessidade for semelhante a algo já implementado, adapte ou estenda em vez de começar do zero.

### Cache-first

Prefira ler materializações existentes (Parquet) ou caches locais antes de realizar novas extrações do Oracle. Isso reduz custos e melhora desempenho.

### Camadas bem definidas

Cada pipeline deve respeitar a sequência **raw → base → curated → marts/views**:

- **raw**: captura dados na forma mais próxima da origem, sem transformações.
- **base**: normaliza tipos, nomes e remove duplicações. Define chaves consistentes.
- **curated**: agrega e harmoniza dados para análise, mantendo integridade das chaves.
- **marts/views**: expõe dados prontos para consumo por relatórios, APIs ou UI.

### Polars sobre Oracle

Use **Polars** para joins, harmonizações, cálculos e agregações. Oracle (ou outras bases) devem ser usadas apenas para extração inicial. Evite executar lógica analítica no banco de dados.

### Chaves invariantes

Durante todas as transformações, preserve as colunas `id_agrupado`, `id_agregado` e `__qtd_decl_final_audit__`. São as ligações entre registros originais e suas agregações. Nunca sobrescreva ou descuide dessas colunas sem análise de impacto em todo o pipeline.

### Identidade fiscal preservada

Não exclua ou modifique campos originais de documentos fiscais. Adicione novas colunas derivadas separadamente. Isso permite auditoria reversa completa.

### Logs e lineage

Cada pipeline deve registrar logs úteis (CNPJ, período, dataset de origem, filtros aplicados) e manter um manifesto de datasets que inclua: nome, localização, schema, origem e data de criação. Isso permite rastrear a origem de qualquer dado.

## Governança de Pull Requests (PR)

- Crie branches curtas e temáticas: `feat/<modulo>-<objetivo>`, `fix/<modulo>-<problema>`, `refactor/<modulo>-<escopo>`.
- **Nunca** faça commit direto na branch principal; todas as mudanças relevantes devem passar por PR.
- Cada PR deve ter um escopo claro e ser revisável em uma sessão. Não misture refatoração ampla com mudança de regra fiscal.
- Na descrição da PR, inclua: objetivo, camadas e domínios afetados, datasets e contratos envolvidos, riscos (schema, fiscal, performance) e plano de rollback/reprocessamento.
- Exija que os testes (unitários e de integração) estejam verdes. Para mudanças em schema ou contratos, inclua migração e validação.

## Formato de resposta A–E

Sempre que o agente for solicitado a planejar, analisar ou executar uma tarefa, a resposta deve seguir a estrutura **A–E**:

1. **Diagnóstico (A)** – descreva o problema ou requisito de forma concisa.
2. **Reaproveitamento (B)** – indique o que pode ser reutilizado (SQL, Parquet, funções, módulos, contratos).
3. **Decisão (C)** – proponha a solução (criar, modificar, reaproveitar) e indique a camada apropriada.
4. **Justificativa (D)** – explique por que a decisão é adequada, considerando integridade fiscal, performance, rastreabilidade e governança.
5. **Plano de execução (E)** – detalhe os passos concretos, incluindo ordens de PRs, testes e validações necessárias.

## Anti‑padrões

- Inserir SQL ad hoc em scripts Python ou UI.
- Pular etapas do pipeline (por exemplo, escrever diretamente no `curated` sem passar pelo `base`).
- Duplicar lógica fiscal tanto no pipeline quanto na interface.
- Alterar `id_agrupado`, `id_agregado` ou `__qtd_decl_final_audit__` sem propagar as consequências para todas as camadas.
- Ignorar logs e lineage, tornando impossível rastrear a origem dos dados.

Siga estas orientações em todas as pastas do projeto para que a assistência de IA seja consistente e eficaz.