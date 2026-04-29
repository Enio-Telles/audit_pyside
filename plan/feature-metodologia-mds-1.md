---
goal: Implementar metodologia MDS — Núcleo de derivação de quantidades
version: 1.0
date_created: 2026-04-20
last_updated: 2026-04-20
owner: engenharia-audit
status: 'In progress'
tags: [feature, data, arquitetura, metodologia]
---

# Introdução

Este plano descreve a implementação inicial (MVP) para materializar o núcleo de derivação de quantidades da metodologia MDS. O objetivo é produzir um artefato testável e idempotente que sirva de base para as fases seguintes.

## 1. Requirements & Constraints

- **REQ-001**: Manter compatibilidade retroativa com Parquets legados sem `quantidade_fisica`.
- **REQ-002**: Não sobrescrever `fator_conversao_override` ou `unidade_referencia_override` automaticamente.
- **CON-001**: Processos devem ser executáveis em máquinas com memória limitada; permitir chunking posterior.

## 2. Implementation Steps

### Implementation Phase 1

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-001 | Criar `src/metodologia_mds/service.py` com `derive_quantities(df)` | ✅ | 2026-04-20 |
| TASK-002 | Adicionar testes unitários `tests/test_metodologia_mds.py` | ✅ | 2026-04-20 |
| TASK-003 | Documentar plano em `docs/metodologia_mds_plan.md` | ✅ | 2026-04-20 |

### Implementation Phase 2

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-004 | Integrar `derive_quantities` ao fluxo existente de `movimentacao_estoque` | | |
| TASK-005 | Implementar conversão de unidades e preservação de override | | |

## 3. Alternatives

- **ALT-001**: Usar pandas em vez de Polars — rejeitado por performance em datasets grandes.
- **ALT-002**: Implementar transformação via SQL — rejeitado por menor rastreabilidade e flexibilidade.

## 4. Dependencies

- **DEP-001**: `polars` Python package
- **DEP-002**: formatos Parquet usados pelo pipeline

## 5. Files

- **FILE-001**: `src/metodologia_mds/service.py` — funções de derivação
- **FILE-002**: `tests/test_metodologia_mds.py` — testes unitários mínimos
- **FILE-003**: `docs/metodologia_mds_plan.md` — plano e descrição

## 6. Testing

- **TEST-001**: `test_derive_quantities_basic` — verifica inventário, entrada e saída.
- **TEST-002**: integração leve — processar Parquet de amostra e validar schema de saída.

## 7. Risks & Assumptions

- **RISK-001**: Ambiguidades em `tipo_operacao` podem exigir normalização de origem.
- **ASSUMPTION-001**: As colunas mínimas (`tipo_operacao` e `quantidade_convertida`) estão presentes ou podem ser derivadas.

## 8. Related Specifications / Further Reading

- `metodologia_mds/01_abordagem_quantidades.md`
- `metodologia_mds/02_agregacao_produtos.md`
