---
applyTo: "src/**/*.py,sql/**/*.sql"
---

# Pipeline Instructions — audit_pyside

## Papel
- O pipeline em `src/` é a fonte principal da regra analítica e fiscal.
- Preserve a ordem do pipeline oficial em `src/orquestrador_pipeline.py`.
- Ao alterar uma etapa, avalie efeito em todas as etapas descendentes.

## Organização
- Prefira a implementação no módulo temático correto, em vez de espalhar regra em wrappers.
- Respeite a estrutura existente em `src/transformacao/`, incluindo `tabelas_base`, `*_pkg`, auxiliares e módulos de rastreabilidade.
- Evite arquivos “faz tudo”.

## Regras de dados
- Preserve lineage e chaves de ligação.
- Não altere schema sem avaliar consumidores, migração e reprocessamento.
- Não perca ajustes manuais em reprocessamentos.
- Use Polars de forma explícita e auditável.

## Validação esperada
- testes unitários para regra crítica
- testes de integração para encadeamento
- validação de schema
- validação de cálculo
- validação de reprocessamento quando aplicável
