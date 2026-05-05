# 2. Docstrings para arquivos fiscais read-only do batch 5

Date: 2025-01-24

## Status

Accepted

## Context

A PR #175 incluiu docstrings para diversos arquivos do pipeline de transformação. No entanto, quatro desses arquivos estão marcados como `read-only` no `AGENTS.md` devido à sua sensibilidade fiscal. Para garantir a segurança e conformidade com as regras de governança do repositório, esses arquivos foram extraídos para uma PR separada (esta).

Os arquivos afetados são:
- `src/transformacao/movimentacao_estoque_pkg/calculo_saldos.py`
- `src/transformacao/movimentacao_estoque_pkg/movimentacao_estoque.py`
- `src/transformacao/rastreabilidade_produtos/_produtos_final_impl.py`
- `src/transformacao/rastreabilidade_produtos/fatores_conversao.py`

## Decision

Decidimos aplicar as docstrings pendentes a estes arquivos `read-only`. A mudança é estritamente aditiva e de documentação, sem qualquer alteração na lógica funcional ou fiscal.

Para garantir a integridade:
1. Validaremos que o `git diff` não contém remoções de linhas de código (`grep ^-[^-]`).
2. Executaremos o `differential harness` para garantir que as 5 chaves invariantes fiscais permanecem inalteradas.

## Consequences

Os arquivos read-only estarão melhor documentados, facilitando a manutenção futura e o entendimento do pipeline fiscal, sem comprometer a estabilidade do sistema.
