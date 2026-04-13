# Processo dos Cálculos Anuais

Este documento descreve a implementação prática da `aba_anual_<cnpj>.parquet`, gerada a partir da `mov_estoque_<cnpj>.parquet`.

## Fonte de dados

O cálculo anual não reconstrói a movimentação do zero. Ele reaproveita a tabela `mov_estoque`, que já contém:

- `q_conv`
- `entr_desac_anual`
- `saldo_estoque_anual`
- `preco_item`
- `Vl_item`
- `it_pc_interna`
- `co_sefin_agr`
- `descr_padrao`
- `unid_ref`
- `__qtd_decl_final_audit__`

A camada anual resume esses dados por:

- `id_agrupado`
- `ano`

O ano é derivado da data efetiva da movimentação:

- `Dt_e_s`, quando existir
- caso contrário, `Dt_doc`

## Colunas geradas

As principais colunas da `aba_anual` são:

- `ano`
- `id_agregado`
- `descr_padrao`
- `unid_ref`
- `ST`
- `estoque_inicial`
- `entradas`
- `saidas`
- `estoque_final`
- `saidas_calculadas`
- `saldo_final`
- `entradas_desacob`
- `saidas_desacob`
- `estoque_final_desacob`
- `pme`
- `pms`
- `aliq_interna`
- `ICMS_saidas_desac`
- `ICMS_estoque_desac`

## Regras de agregação física

Os quantitativos anuais são calculados assim:

- `estoque_inicial`: soma de `q_conv` das linhas `0 - ESTOQUE INICIAL`
- `entradas`: soma de `q_conv` das linhas `1 - ENTRADA`
- `saidas`: soma de `q_conv` das linhas `2 - SAIDA`
- `estoque_final`: soma de `__qtd_decl_final_audit__` das linhas `3 - ESTOQUE FINAL`
- `entradas_desacob`: soma anual de `entr_desac_anual`
- `saldo_final`: último `saldo_estoque_anual` do ano, ordenado por `ordem_operacoes`

Observacao importante:

- `entradas_desacob` continua vindo apenas das linhas da `mov_estoque` que já chegam com `entr_desac_anual > 0`
- `3 - ESTOQUE FINAL` não cria `entr_desac_anual`; ele apenas informa o inventário declarado em `__qtd_decl_final_audit__`

Com isso:

- `saidas_calculadas = estoque_inicial + entradas - estoque_final`
- `saidas_desacob = max(0, estoque_final - saldo_final)`
- `estoque_final_desacob = max(0, saldo_final - estoque_final)`
- o valor minimo de `estoque_final_desacob` e zero
- os dois campos ficam mutuamente exclusivos por construcao

## PME e PMS do ano

Os preços médios anuais usam apenas movimentos válidos:

- excluem linhas marcadas como devolução simples em `dev_simples`
- excluem linhas marcadas em `excluir_estoque`
- excluem linhas neutralizadas com `q_conv <= 0`

As fórmulas são:

- `pme = soma(valor das entradas válidas) / soma(qtd das entradas válidas)`
- `pms = soma(valor das saídas válidas) / soma(qtd das saídas válidas)`

O valor unitário usado na agregação é:

- `preco_item`, quando existir
- senão, `Vl_item`

## Regra anual de ST

O cálculo do ICMS anual depende da vigência de ST no ano para o `co_sefin_agr` do produto.

O processo:

- cruza `co_sefin_agr` da base anual com `it_co_sefin` de `sitafe_produto_sefin_aux.parquet`
- converte `it_da_inicio` e `it_da_final` para datas
- mantém apenas vigências que tenham interseção com o ano analisado
- monta a coluna `ST` com os períodos efetivos do ano
- define `__tem_st_ano__ = True` se existir ao menos um período com `it_in_st = 'S'`

Também é obtida a alíquota anual de referência:

- `aliq_interna = __aliq_ref__`, quando vier da referência SEFIN
- fallback para `aliq_interna_mov`, que é o `it_pc_interna` da última movimentação do ano
- fallback final para `0.0`

## Cálculo de `ICMS_saidas_desac`

Esta coluna estima o ICMS devido sobre saídas desacobertadas.

### 1. Base de cálculo da saída

Se `pms > 0`:

- `Base_saida = saidas_desacob * pms`

Se `pms = 0`:

- `Base_saida = saidas_desacob * pme * 1.30`

O fator `1.30` é o fallback hoje implementado no código para ausência de `pms`.

### 2. Aplicação da alíquota

- `aliq_factor = aliq_interna / 100`

Então:

- `ICMS_saidas_desac = Base_saida * aliq_factor`

### 3. Regra de bloqueio por ST

Se `__tem_st_ano__ = True`:

- `ICMS_saidas_desac = 0`

Ou seja, basta haver um período anual de ST para zerar essa coluna.

## Cálculo de `ICMS_estoque_desac`

Esta coluna estima o ICMS devido sobre o estoque final desacobertado.

### 1. Base de cálculo do estoque

Se `pms > 0`:

- `Base_estoque = estoque_final_desacob * pms`

Se `pms = 0`:

- `Base_estoque = estoque_final_desacob * pme * 1.30`

### 2. Aplicação da alíquota

Usa a mesma alíquota anual do cálculo anterior:

- `aliq_factor = aliq_interna / 100`

Então:

- `ICMS_estoque_desac = Base_estoque * aliq_factor`

### 3. Regra de ST

Ao contrário de `ICMS_saidas_desac`, esta coluna não é zerada por ST.

Ou seja:

- a presença de `__tem_st_ano__ = True` não bloqueia `ICMS_estoque_desac`
- o cálculo continua normalmente com a base de estoque e `aliq_interna`

## Resumo operacional do ICMS anual

Para cada `id_agrupado + ano`:

1. calcula-se `pme`, `pms`, `saidas_desacob` e `estoque_final_desacob`
2. cruza-se o produto com `sitafe_produto_sefin_aux.parquet`
3. identifica-se se houve ST em qualquer período do ano
4. resolve-se `aliq_interna`
5. se houver ST no ano, zera apenas `ICMS_saidas_desac`
6. se não houver ST:
   - usa `pms` quando disponível
   - usa fallback `pme * 1.30` quando `pms = 0`
   - aplica `aliq_interna / 100`
7. `ICMS_estoque_desac` é calculado independentemente de ST

## Arredondamento

O código arredonda:

- quantidades e saldos para 4 casas decimais
- `pme`, `pms`, `aliq_interna`, `ICMS_saidas_desac` e `ICMS_estoque_desac` para 2 casas decimais

## Saída gerada

O resultado é salvo em:

- `dados/CNPJ/<cnpj>/analises/produtos/aba_anual_<cnpj>.parquet`

## Script responsável

- `src/transformacao/calculos_anuais.py`
