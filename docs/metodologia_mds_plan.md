# Plano de Implementação — Metodologia MDS

## Objetivo

Implementar a metodologia descrita em `metodologia_mds/` no pipeline `audit_pyside` garantindo: rastreabilidade ponta a ponta, compatibilidade retroativa com Parquets legados, separação clara entre regras (services) e apresentação (UI), e entregas incrementais com testes e observabilidade.

## Contexto

- Repositório: `audit_pyside` (PySide6 + Polars + Parquet).
- A metodologia MDS define convenções de quantidades, agregação de produtos, conversão de unidades e geração das tabelas de período, mensal e anual.
- Prioridades: corretude funcional, rastreabilidade, reuso, separação ETL/UI, estabilidade.

## Reuso

- Reaproveitar módulos existentes em `src/transformacao/` e `src/transformacao/movimentacao_estoque_pkg` quando possível.
- Reutilizar formatos Parquet já adotados (`dados/CNPJ/...`) e convenções de nomes de colunas.
- Mapas de agrupamento manuais (`map_produto_agrupado_<cnpj>.parquet`) permanecem como fonte de verdade para ajustes manuais.

## Arquitetura proposta

- Estilo: monólito modular (modular monolith) com serviços pequenos e testáveis.
- Componentes principais:
  - Ingestão: leitura das buscas SQL convertidas para Parquet.
  - Normalização: conversão de unidades e aplicação de `fator_conversao`.
  - Agregação: geração de `id_produto_agrupado` e tabelas de mapeamento.
  - Movimentação: construção de `movimentacao_estoque` com `quantidade_fisica*` e sinalização.
  - Cálculos periódicos: geração de `tabela_periodos`, `tabela_mensal` e `tabela_anual`.
  - Auditoria/Export: registros de inconsistências e arquivos de auditoria.

## Divisão por stack

- Linguagem: Python 3.x
- Biblioteca de dados: Polars
- Armazenamento: Parquet em `dados/CNPJ/<cnpj>/analises/produtos/`
- Testes: `pytest` (tests/)
- Observabilidade: logs estruturados (LOG_LEVEL configurável), métricas mínimas e arquivos de auditoria.

## Engenharia

- Contratos e schemas: definir e validar schemas Parquet (tipagem e nomes de colunas) em unidades de teste.
- Testes automatizados: unitários para derivação de quantidades e integração leve que processa um Parquet de amostra.
- Cancelamento e idempotência: garantir que reprocessamentos preservem overrides manuais.
- Registro de alterações: `versao_agrupamento` e log auditável em todas as operações manuais.

## GitHub / Processo

- Branches: `feat/metodologia-mds-<parte>` ou `fix/metodologia-mds-<issue>`.
- PRs pequenas e revisáveis; incluir descrição do impacto em dados e migração se houver alteração de schema.
- CI mínimo: lint, testes unitários, validação de schema (quando aplicável).

## Contratos

- Entradas: Parquets gerados pelas buscas SQL (C170, NF‑e, NFC‑e, Bloco H).
- Saídas: `movimentacao_estoque_<cnpj>.parquet`, `tabela_periodos_<cnpj>.parquet`, `tabela_mensal_<cnpj>.parquet`, `tabela_anual_<cnpj>.parquet`.
- Campos críticos: `id_linha_origem`, `id_produto_origem`, `id_produto_agrupado`, `quantidade_convertida`, `quantidade_fisica`, `quantidade_fisica_sinalizada`, `estoque_final_declarado`.

## Implementação (High-level)

1. Materializar funções puras para derivação de quantidades (`quantidade_fisica*`) em `src/metodologia_mds/service.py`.
2. Implementar conversão de unidades e preservação de overrides.
3. Implementar agregação básica por `id_produto_agrupado_base` e ponte de mapeamento.
4. Materializar `movimentacao_estoque` com origem dos eventos e flags (`evento_sintetico`).
5. Implementar geradores de tabelas (`periodos`, `mensal`, `anual`) com fórmulas e arredondamento conforme documentação.
6. Adicionar testes unitários e integração leve com Parquet de amostra.

## Plano detalhado (fases e critérios)

### Fase 1 — Análise e plano (COMPLETED)
- Resultado: este documento salvo em `docs/metodologia_mds_plan.md`.

### Fase 2 — Núcleo de derivação de quantidades (IN PROGRESS)
- GOAL-001: Implementar funções idempotentes que derivam `quantidade_fisica`, `quantidade_fisica_sinalizada` e `estoque_final_declarado`.
- TASK-001: Criar `src/metodologia_mds/service.py` com `derive_quantities(df)` → saída validada.
- Critério de aceite: testes unitários que cobrem inventário, entrada, saída e devolução.

### Fase 3 — Conversão de unidades e fatores (PLANNED)
- GOAL-002: Implementar cálculo de `fator_conversao` respeitando `override` e fontes.
- TASK-002: Reaproveitar `produtos_final` e `descricao_produtos`; criar fallback e registrar `fator_conversao_origem`.

### Fase 4 — Agregação e mapeamento (PLANNED)
- GOAL-003: Gerar `id_produto_agrupado_base` e tabela ponte `map_produto_agrupado_<cnpj>.parquet`.

### Fase 5 — Movimentação e tabelas finais (PLANNED)
- GOAL-004: Materializar `movimentacao_estoque` e tabelas `periodos`, `mensal`, `anual`.

### Fase 6 — Testes, validação e performance (PLANNED)
- GOAL-005: Cobertura mínima de testes unitários e integração; benchmark em dataset reduzido.

### Fase 7 — PR e rollout (PLANNED)
- GOAL-006: Abrir PR com mudanças, documentação de impacto e plano de roll-back.

## Riscos

- Mudança de schema Parquet sem migração pode quebrar consumidores downstream.
- Fatores de conversão deduzidos por preço são arriscados e devem ser marcados como `preco` e revalidados manualmente.
- Agrupamento automático pode gerar ambiguidades que exigem intervenção manual; é importante exportar casos ambíguos.
- Operações demoradas precisam de chunking e processamento incremental para evitar OOM.

## MVP

- Implementar e testar `derive_quantities` (Fase 2) + documentação e testes básicos.
- Entregar arquivo de plano em `docs/` e scaffolding inicial em `src/metodologia_mds/`.

---

Arquivo criado automaticamente pelo Agente de Planejamento. Para próximos passos, confirme se deseja que eu:

- rode benchmarks em um Parquet de amostra;
- gere o primeiro PR com as alterações;
- agregue validações de schema automáticas na CI.
