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

## Prioridades
Sempre que fizer sentido, cubra:
- movimentacao_estoque
- calculos_mensais
- calculos_anuais
- conversão de unidades
- agrupamento de produtos
- preservação de ajustes manuais

## Regras
- Prefira testes pequenos, claros e determinísticos.
- Nomeie cenários de forma explícita.
- Cubra casos de borda e regressão.
- Não depender de estado local implícito quando isso puder ser isolado.
