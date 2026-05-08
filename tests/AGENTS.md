# AGENTS.md — tests

Estas instruções valem para toda a árvore `tests/`.

## Objetivo
Os testes devem proteger:
- corretude fiscal
- rastreabilidade
- compatibilidade de schema
- regressões em estoque e cálculos
- preservação de ajustes manuais

## Prioridades
Cubra quando aplicável:
- movimentação de estoque
- cálculos mensais
- cálculos anuais
- conversão de unidades
- agrupamento de produtos
- integração GUI/serviços em pontos críticos

## Regras
- prefira testes pequenos e determinísticos
- nomeie cenários de forma explícita
- cubra casos de borda e regressão
- não dependa de estado implícito quando puder isolar

## Ao escrever ou alterar testes
- deixe claro o cenário fiscal/operacional protegido
- prefira fixtures simples
- evite acoplamento desnecessário à estrutura interna
