# Processo dos Cálculos Mensais

Este documento descreve a implementação prática da `aba_mensal_<cnpj>.parquet`, gerada a partir da `mov_estoque_<cnpj>.parquet`.

## Fonte de dados

O cálculo mensal não reconstrói o fluxo de estoque do zero. Ele reaproveita a tabela `mov_estoque`, que já contém:

- `q_conv`
- `entr_desac_anual`
- `saldo_estoque_anual`
- `custo_medio_anual`
- `preco_item`
- `Aliq_icms`
- `it_in_st`
- `it_pc_mva`
- `it_in_mva_ajustado`

Como `saldo_estoque_anual` e `custo_medio_anual` já são calculados cronologicamente linha a linha e reiniciam a cada ano, a camada mensal apenas resume esses resultados por `ano + mes + id_agrupado`.

## Chave de agrupamento

A `aba_mensal` é agregada por:

- `id_agrupado`
- `ano`
- `mes`

O campo de data efetiva do mês é:

- `Dt_e_s`, quando existir
- caso contrário, `Dt_doc`

## Colunas geradas

As principais colunas são:

- `ano`
- `mes`
- `id_agregado`
- `descr_padrao`
- `unids_mes`
- `unids_ref_mes`
- `ST`
- `it_in_st`
- `valor_entradas`
- `qtd_entradas`
- `pme_mes`
- `valor_saidas`
- `qtd_saidas`
- `pms_mes`
- `MVA`
- `MVA_ajustado`
- `entradas_desacob`
- `ICMS_entr_desacob`
- `saldo_mes`
- `custo_medio_mes`
- `valor_estoque`

## Regras de cálculo

### 1. Entradas e saídas do mês

- `valor_entradas`: soma de `preco_item` das linhas `1 - ENTRADA`
- `qtd_entradas`: soma de `q_conv` das linhas `1 - ENTRADA`
- `valor_saidas`: soma do valor absoluto de `preco_item` das linhas `2 - SAIDAS`
- `qtd_saidas`: soma do valor absoluto de `q_conv` das linhas `2 - SAIDAS`

Esses quatro campos refletem a movimentação bruta do mês.

### 2. PME e PMS do mês

As médias do mês desconsideram:

- devoluções
- linhas com `excluir_estoque = true`
- linhas neutralizadas com `q_conv <= 0`

Na implementação, devolução é identificada por qualquer uma das condições abaixo:

- `dev_simples = true`
- `dev_venda = true`
- `dev_compra = true`
- `dev_ent_simples = true`
- `finnfe = 4`

Então:

- `pme_mes = soma(preco_item das entradas válidas) / soma(q_conv das entradas válidas)`
- `pms_mes = soma(abs(preco_item) das saídas válidas) / soma(abs(q_conv) das saídas válidas)`

### 3. Entradas desacobertadas

- `entradas_desacob` é a soma mensal de `entr_desac_anual`

Ou seja, a camada mensal apenas resume os eventos já identificados no fluxo cronológico da `mov_estoque`.

### 4. Saldo e custo médio do mês

- `saldo_mes`: último `saldo_estoque_anual` do mês, ordenado por `ordem_operacoes`
- `custo_medio_mes`: último `custo_medio_anual` do mês, ordenado por `ordem_operacoes`
- `valor_estoque = saldo_mes * custo_medio_mes`

Essa abordagem garante coerência entre a visão detalhada da movimentação e a visão resumida mensal.

## ST mensal e ICMS sobre entradas desacobertadas

A validação de ST da `aba_mensal` não usa apenas o `it_in_st` da última linha da `mov_estoque`.
Ela cruza `co_sefin_agr` com `sitafe_produto_sefin_aux.parquet` e verifica se o mês sob análise
tem sobreposição com o intervalo `[it_da_inicio, it_da_final]`.

Com esse cruzamento, a tabela mensal passa a expor:

- `ST`: histórico dos períodos vigentes no mês
- `MVA`: igual a `it_pc_mva` da última movimentação do mês, mas somente quando houver ST vigente no mês; caso contrário fica `null`
- `MVA_ajustado`: fica `null` quando `it_in_mva_ajustado = 'N'`; quando `it_in_mva_ajustado = 'S'`, recebe o MVA ajustado calculado pela fórmula abaixo, também somente quando houver ST vigente no mês
- `__tem_st_mes__`: flag interna usada no cálculo

O `ICMS_entr_desacob` só é calculado quando:

- existe ST vigente no mês
- `entradas_desacob > 0`

### Fórmula implementada

Se `pms_mes > 0`:

- `ICMS_entr_desacob = pms_mes * entradas_desacob * (aliq_mes / 100)`

Se `pms_mes = 0`:

- `ICMS_entr_desacob = pme_mes * entradas_desacob * (aliq_mes / 100) * MVA`

Onde:

- `aliq_mes = it_pc_interna` da última movimentação do mês
- `ALQ_inter = Aliq_icms` da última movimentação do mês
- `MVA_orig = it_pc_mva / 100` na fórmula
- `MVA` exibido na tabela é o valor bruto de `it_pc_mva`
- `MVA usado no cálculo` é:
- `it_pc_mva / 100`, quando `it_in_mva_ajustado = 'N'`
- `[ ((1 + MVA_orig) * (1 - ALQ_inter)) / (1 - ALQ_interna) ] - 1`, quando `it_in_mva_ajustado = 'S'`
- `MVA_ajustado` exibido na tabela só recebe valor no segundo caso

Observação:

- `it_in_st` continua sendo preservado como atributo vindo da `mov_estoque`
- porém a decisão de calcular `ICMS_entr_desacob` usa a vigência mensal da referência SEFIN

## Saída gerada

O resultado é salvo em:

- `dados/CNPJ/<cnpj>/analises/produtos/aba_mensal_<cnpj>.parquet`

## Integração no pipeline

A `aba_mensal` agora é gerada:

- como etapa própria do pipeline (`calculos_mensais`)
- após recálculo de `mov_estoque`
- junto com a regeneração de tabelas derivadas da agregação
