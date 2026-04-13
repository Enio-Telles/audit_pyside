# Processo de Cálculo da `mov_estoque`

Este documento descreve como a tabela `mov_estoque_<cnpj>.parquet` calcula, para cada linha, os campos:

- `q_conv`
- `saldo_estoque_anual`
- `entr_desac_anual`
- `custo_medio_anual`

Todos os cálculos são anuais. Em outras palavras, o processamento é reiniciado a cada combinação de:

- `id_agrupado`
- ano da movimentação

## 1. Ordem de processamento

As movimentações são ordenadas por produto e por data, respeitando a sequência lógica:

1. `0 - ESTOQUE INICIAL`
2. `1 - ENTRADA`
3. `2 - SAIDAS`
4. `3 - ESTOQUE FINAL`

Depois disso, o cálculo é executado linha a linha dentro de cada ano.

## 2. Quantidade convertida

`q_conv` representa a quantidade da linha convertida para a unidade de referência:

```text
q_conv = Qtd * fator
```

A linha é zerada para fins de cálculo quando:

- `excluir_estoque = true`
- `mov_rep = true`
- a NF tem `infprot_cstat` diferente de `100` ou `150`
- a linha é inventário fora da abertura anual de `01/01`

Quando `mov_rep` não existir na origem da movimentação, o sistema trata esse campo como `false`.

Para saídas, o valor assinado usado internamente no saldo é:

```text
__q_conv_sinal__ = -q_conv
```

Para entradas e estoque inicial anual:

```text
__q_conv_sinal__ = q_conv
```

## 3. Saldo de estoque anual

O `saldo_estoque_anual` é acumulado cronologicamente:

- entradas e estoque inicial anual somam no saldo
- saídas subtraem do saldo
- estoque final não altera o saldo, apenas audita

Se o saldo ficar negativo em uma saída:

- a diferença absoluta vira `entr_desac_anual`
- o saldo físico volta para zero

## 4. Entradas desacobertadas

`entr_desac_anual` pode surgir de duas formas:

### 4.1. Saída sem saldo suficiente

Quando uma saída faz o saldo físico ficar negativo:

```text
entr_desac_anual = abs(saldo_negativo)
```

Depois disso, o estoque do ano é reiniciado para zero naquele ponto.

### 4.2. Auditoria do estoque final anual

Nas linhas `3 - ESTOQUE FINAL` posicionadas em `31/12`, o sistema compara:

- saldo calculado até a linha
- quantidade declarada no inventário final

Se o inventário declarado for maior que o saldo calculado:

```text
entr_desac_anual = estoque_final_declarado - saldo_calculado
```

Essa auditoria não altera `saldo_estoque_anual`. Ela apenas evidencia a insuficiência de entradas para suportar o estoque final declarado.

## 5. Custo médio anual

`custo_medio_anual` é calculado em cada linha da `mov_estoque`.

O valor de referência da linha é `preco_item`, que representa o valor total das mercadorias daquela movimentação. O custo unitário de entrada é derivado de:

```text
custo_unitario_linha = preco_item / q_conv
```

Na prática, o cálculo é feito pelo saldo financeiro acumulado.

### 5.1. Entradas e estoque inicial anual

Em entradas válidas, o sistema soma:

- quantidade no saldo físico
- `preco_item` no saldo financeiro

Em seguida recalcula:

```text
custo_medio_anual = saldo_financeiro / saldo_estoque_anual
```

### 5.2. Saídas

Nas saídas, o `preco_item` da própria linha não é usado para valorar a baixa.

A baixa é feita pelo custo médio vigente antes da saída:

```text
valor_baixa = q_conv * custo_medio_vigente
saldo_financeiro = saldo_financeiro - valor_baixa
```

Depois disso, o custo médio é recalculado sobre o saldo remanescente.

### 5.3. Estoque final anual

O estoque final anual não altera nem o saldo físico nem o financeiro. Por isso:

- `saldo_estoque_anual` permanece o mesmo
- `custo_medio_anual` permanece o mesmo

### 5.4. Devoluções

Quando a movimentação for identificada como devolução, por exemplo com `finnfe = 4` e/ou marcações de CFOP como `dev_simples`, `dev_venda`, `dev_compra` ou `dev_ent_simples`, ela entra no cálculo cronológico normalmente, desde que o CFOP não esteja com `excluir_estoque = true`.

- Devolução de venda (`1 - ENTRADA`): volta para o estoque sem mudar o `custo_medio_anual`. Para isso, o saldo financeiro da devolução retorna pelo custo médio vigente antes da linha, e não pelo `preco_item` de retorno.
- Devolução de compra (`2 - SAIDAS`): sai do estoque pelo custo médio vigente, também sem alterar o `custo_medio_anual`.

Em resumo: devoluções podem alterar o saldo físico, mas não formam nova média de custo.

## 6. Reset anual

Como o agrupamento é anual, os acumuladores abaixo sempre reiniciam ao mudar o ano:

- saldo físico
- saldo financeiro
- entradas desacobertadas
- custo médio

Se existir inventário de abertura em `01/01`, ele entra como a base do novo ano.

Inventários intermediários são ignorados no cálculo anual:

- `0 - ESTOQUE INICIAL` só entra no cálculo quando a data é `01/01`
- `3 - ESTOQUE FINAL` só entra como auditoria quando a data é `31/12`

Fora dessas datas, a linha fica neutra para:

- `saldo_estoque_anual`
- `entr_desac_anual`
- `custo_medio_anual`

## 7. Resumo operacional

Para cada linha da `mov_estoque`, o sistema:

1. ordena a movimentação no fluxo anual do produto
2. decide se a linha entra ou não no cálculo
3. converte a quantidade para `q_conv`
4. atualiza saldo físico
5. atualiza saldo financeiro
6. calcula `custo_medio_anual`
7. registra eventual `entr_desac_anual`
