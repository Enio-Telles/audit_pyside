# AGENTS.md — src/transformacao

Estas instruções valem para toda a árvore `src/transformacao/`.

## Papel desta área
Aqui vive a regra principal de transformação, harmonização, agrupamento, conversão e cálculo.
Prefira implementar a regra no módulo temático correto em vez de espalhá-la em wrappers ou pontos laterais.

## Regras específicas
- Preserve a ordem e as dependências do pipeline oficial.
- Ao alterar uma etapa, avalie efeito em todas as etapas descendentes.
- Não altere schema sem avaliar consumidores, migração e reprocessamento.
- Preserve lineage e chaves de ligação.
- Não perca ajustes manuais em reprocessamentos.
- Use Polars de forma explícita e auditável.
- Evite helpers obscuros para regra crítica.

## Mudanças sensíveis nesta área
Dê atenção extra para:
- agrupamento de produtos
- conversão de unidades
- movimentação de estoque
- cálculos mensais
- cálculos anuais
- deduplicação
- joins críticos

## Validação esperada
Quando aplicável:
- testes unitários para regra crítica
- testes de integração para encadeamento
- validação de schema
- validação de cálculo
- validação de reprocessamento
