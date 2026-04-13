# Processamento e Cálculos Fiscais

Este documento detalha o funcionamento interno dos algoritmos de cálculo do sistema.

## 1. Movimentação de Estoque (`mov_estoque`)

A tabela de movimentação de estoque consolida dados do SPED (C170, Bloco H) e Notas Fiscais (NFe, NFCe).

### Ordem de Processamento
As movimentações são ordenadas por produto (`id_agrupado`) e cronologia, respeitando a sequência:
1.  `0 - ESTOQUE INICIAL` (Abertura em 01/01)
2.  `1 - ENTRADA`
3.  `2 - SAIDAS`
4.  `3 - ESTOQUE FINAL` (Auditado em 31/12)

### Cálculos de Saldo e Custo Médio
O cálculo é **anual**. O saldo é zerado e o custo médio reiniciado ao mudar o ano.
- **Saldo Físico:** Entradas somam, saídas subtraem. Se o saldo ficar negativo em uma saída, a diferença vira `entr_desac_anual`.
- **Custo Médio:** Calculado pelo método ponderado móvel em cada entrada. As saídas baixam o estoque pelo custo médio vigente.

## 2. Fatores de Conversão

O sistema identifica disparidades entre a unidade de medida de compra (ex: CX com 12) e a unidade de venda/inventário (ex: UN).
- O fator é calculado comparando movimentações do mesmo produto sob unidades diferentes.
- O campo `q_conv` na movimentação de estoque é sempre `Quantidade Original * Fator`.

## 3. Cálculos Mensais (`aba_mensal`)

Esta camada resume a movimentação detalhada por mês, focando em auditoria tributária.
- **PME (Preço Médio de Entrada):** Média ponderada das entradas válidas do mês.
- **PMS (Preço Médio de Saída):** Média ponderada das saídas válidas do mês.
- **ICMS Entradas Desacobertadas:** Calculado quando há entradas desacobertadas identificadas no fluxo de estoque, aplicando a alíquota interna e, se aplicável, o MVA (ajustado ou original) sobre a base estimada.

## 4. Cálculos Anuais (`aba_anual`)

Visão consolidada para auditoria de balanço anual.
- **Saídas Calculadas:** `Estoque Inicial + Entradas - Estoque Final`.
- **Saídas Desacobertadas:** `max(0, Saldo Calculado Final - Estoque Final Declarado)`.
- **ICMS Saídas Desac.:** Estimativa de imposto devido sobre mercadorias que saíram sem documento fiscal, aplicando a alíquota de referência apenas se o produto não estiver sob o regime de Substituição Tributária (ST) no período.

## 5. Regras de ST (Substituição Tributária)

O sistema cruza dinamicamente o `co_sefin_agr` com a base oficial da SEFIN (`sitafe_produto_sefin_aux.parquet`), respeitando a **vigência temporal** de cada pauta para decidir:
- Se o ICMS deve ser calculado ou zerado.
- Qual MVA aplicar.
- Qual a alíquota interna de referência.
