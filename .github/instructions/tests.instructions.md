---
applyTo: "tests/**/*.py,test_*.py,**/*test*.py"
---

# Test Instructions — audit_pyside

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
