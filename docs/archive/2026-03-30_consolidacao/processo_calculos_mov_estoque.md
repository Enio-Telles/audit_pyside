# Processo de Calculo da `mov_estoque`

Este documento descreve como a tabela `mov_estoque_<cnpj>.parquet` calcula, para cada linha, os campos:

- `fonte`
- `q_conv`
- `saldo_estoque_anual`
- `entr_desac_anual`
- `custo_medio_anual`

Todos os calculos sao anuais. Em outras palavras, o processamento reinicia a cada combinacao de:

- `id_agrupado`
- ano da movimentacao

## 1. Fonte da linha

A coluna `fonte` identifica a origem fisica ou derivada da linha na `mov_estoque`.

Valores esperados:

- `c170`: linha vinda do SPED C170
- `nfe`: linha vinda da NFe
- `nfce`: linha vinda da NFCe
- `bloco_h`: linha de inventario vinda do Bloco H
- `gerado`: linha sintetica criada pela rotina anual

Observacoes importantes:

- a etapa transacional aplica um filtro de direcao por origem antes dos eventos anuais:
  - `c170` contribui apenas com `1 - ENTRADA`
  - `nfe` contribui apenas com `2 - SAIDAS`
  - `nfce` contribui apenas com `2 - SAIDAS`
- linhas de inventario reais do Bloco H sao convertidas para `3 - ESTOQUE FINAL`, mas preservam `fonte = bloco_h`
- linhas sinteticas como `3 - ESTOQUE FINAL gerado`, `0 - ESTOQUE INICIAL gerado` e os estoques iniciais derivados do fechamento anual recebem `fonte = gerado`

## 2. Ordem de processamento

As movimentacoes sao ordenadas por produto e por data, respeitando a sequencia logica:

1. `0 - ESTOQUE INICIAL`
2. `1 - ENTRADA`
3. `2 - SAIDAS`
4. `3 - ESTOQUE FINAL`

Depois disso, o calculo e executado linha a linha dentro de cada ano.

## 3. Quantidade convertida

`q_conv` representa a quantidade da linha convertida para a unidade de referencia:

```text
q_conv = abs(Qtd) * abs(fator)
```

Na pratica, o valor base da linha e montado primeiro e depois pode ser neutralizado.

### 3.1. Quando `q_conv` vira zero

A linha e zerada para fins de calculo quando ocorrer pelo menos uma das condicoes abaixo:

- `excluir_estoque = true`
- `mov_rep = true`
- `infprot_cstat` diferente de `100` ou `150`
- estoque inicial fora de `01/01`
- estoque final fora de `31/12`
- base da linha igual a zero, por exemplo `Qtd = 0`

Quando `mov_rep` nao existir na origem da movimentacao, o sistema trata esse campo como `false`.

As linhas neutralizadas com `q_conv = 0` tambem devem ser tratadas como neutras para as medias de preco calculadas nas camadas mensal e anual.

### 3.2. Valor assinado usado no saldo

Para saidas, o valor assinado usado internamente no saldo e:

```text
__q_conv_sinal__ = -q_conv
```

Para entradas e estoque inicial anual:

```text
__q_conv_sinal__ = q_conv
```

### 3.3. Observacao sobre estoque final anual

No comportamento atual, a linha `3 - ESTOQUE FINAL` nao carrega a quantidade auditada em `q_conv`.

- `q_conv` da linha `3 - ESTOQUE FINAL` permanece `0`
- a quantidade declarada para auditoria fica em `__qtd_decl_final_audit__`

Ou seja, o estoque final anual participa da auditoria, nao do saldo fisico direto.

## 4. Saldo de estoque anual

O `saldo_estoque_anual` e acumulado cronologicamente:

- entradas e estoque inicial anual somam no saldo
- saidas subtraem do saldo
- estoque final nao altera o saldo, apenas audita

Se o saldo ficar negativo em uma saida:

- a diferenca absoluta vira `entr_desac_anual`
- o saldo fisico volta para zero

## 5. Entradas desacobertadas

Na tabela `mov_estoque`, `entr_desac_anual` surge em uma única situação:

### 5.1. Saida sem saldo suficiente

Quando uma saida faz o saldo fisico ficar negativo:

```text
entr_desac_anual = abs(saldo_negativo)
```

Depois disso, o estoque do ano e reiniciado para zero naquele ponto.

As linhas `3 - ESTOQUE FINAL` e `3 - ESTOQUE FINAL gerado` permanecem neutras para `entr_desac_anual`, mesmo quando `__qtd_decl_final_audit__` for maior que o saldo calculado.

*(Nota: a comparação de saldo físico versus o estoque final declarado para fins de identificar omissão de entrada não é registrada na coluna `entr_desac_anual` da tabela analítica, sendo exclusividade da totalização na aba Anual via `estoque_final_desacob`.)*

## 6. Custo medio anual

`custo_medio_anual` e calculado em cada linha da `mov_estoque`.

O valor de referencia da linha e `preco_item`, que representa o valor total das mercadorias daquela movimentacao. O custo unitario de entrada e derivado de:

```text
custo_unitario_linha = preco_item / q_conv
```

Na pratica, o calculo e feito pelo saldo financeiro acumulado.

### 6.1. Entradas e estoque inicial anual

Em entradas validas, o sistema soma:

- quantidade no saldo fisico
- `preco_item` no saldo financeiro

Em seguida recalcula:

```text
custo_medio_anual = saldo_financeiro / saldo_estoque_anual
```

### 6.2. Saidas

Nas saidas, o `preco_item` da propria linha nao e usado para valorar a baixa.

A baixa e feita pelo custo medio vigente antes da saida:

```text
valor_baixa = q_conv * custo_medio_vigente
saldo_financeiro = saldo_financeiro - valor_baixa
```

Depois disso, o custo medio e recalculado sobre o saldo remanescente.

### 6.3. Estoque final anual

O estoque final anual nao altera nem o saldo fisico nem o financeiro. Por isso:

- `saldo_estoque_anual` permanece o mesmo
- `custo_medio_anual` permanece o mesmo

### 6.4. Devolucoes

Quando a movimentacao for identificada como devolucao, por exemplo com `finnfe = 4` e/ou marcacoes de CFOP como `dev_simples`, `dev_venda`, `dev_compra` ou `dev_ent_simples`, ela entra no calculo cronologico normalmente, desde que o CFOP nao esteja com `excluir_estoque = true`.

- devolucao de venda (`1 - ENTRADA`): volta para o estoque e recalcula o `custo_medio_anual` usando o `preco_item` de retorno
- devolucao de compra (`2 - SAIDAS`): sai do estoque pelo custo medio vigente, sem usar o `preco_item` da linha para formar nova media

## 7. Reset anual

Como o agrupamento e anual, os acumuladores abaixo sempre reiniciam ao mudar o ano:

- saldo fisico
- saldo financeiro
- entradas desacobertadas
- custo medio

Se existir inventario de abertura em `01/01`, ele entra como a base do novo ano.

Inventarios intermediarios sao ignorados no calculo anual:

- `0 - ESTOQUE INICIAL` so entra no calculo quando a data e `01/01`
- `3 - ESTOQUE FINAL` so entra como auditoria quando a data e `31/12`

Fora dessas datas, a linha fica neutra para:

- `saldo_estoque_anual`
- `entr_desac_anual`
- `custo_medio_anual`

## 8. Diagnostico do caso `37671507000187` / `id_agrupado_20`

No ambiente atual, o recorte principal analisado apresentou:

- `243` linhas totais
- `203` linhas com `q_conv = 0`

Distribuicao dos zeros observados:

- `178` linhas com `mov_rep = true`
- `11` linhas com `infprot_cstat` invalido
- `1` linha com `excluir_estoque = true`
- `11` linhas com base zero, concentradas em eventos gerados e linhas neutras de inventario

Leitura pratica do caso:

- o principal zerador hoje nao e fator de conversao nem falta de unidade
- o principal zerador e o espelhamento entre C170, NFe e NFCe capturado por `mov_rep`
- isso torna a coluna `fonte` essencial para auditoria, porque permite identificar de qual trilha cada linha veio antes da neutralizacao

## 9. Resumo operacional

Para cada linha da `mov_estoque`, o sistema:

1. identifica a `fonte` da linha
2. ordena a movimentacao no fluxo anual do produto
3. decide se a linha entra ou nao no calculo
4. converte a quantidade para `q_conv`
5. atualiza saldo fisico
6. atualiza saldo financeiro
7. calcula `custo_medio_anual`
8. registra eventual `entr_desac_anual`
