# Tabela Mensal

Este documento consolida as regras da `aba_mensal_<cnpj>.parquet`, gerada por `src/transformacao/calculos_mensais.py` e pela implementação em `src/transformacao/calculos_mensais_pkg/`.

## Papel da tabela

A tabela mensal resume a `mov_estoque` por produto e mês, sem recalcular o saldo cronológico do zero. Ela reutiliza os resultados já materializados na movimentação detalhada.

## Chave de agrupamento

A agregação é feita por:

- `id_agrupado`
- `ano`
- `mes`

Na saída, `id_agrupado` é exposto como `id_agregado`.

A data efetiva do mês segue:

- `Dt_e_s`, quando existir;
- senão `Dt_doc`.

## Principais colunas

- `valor_entradas` e `qtd_entradas`
- `valor_saidas` e `qtd_saidas`
- `pme_mes` e `pms_mes`
- `entradas_desacob`
- `ICMS_entr_desacob`
- `saldo_mes`
- `custo_medio_mes`
- `valor_estoque`
- `ST`, `MVA` e `MVA_ajustado`

## Regras de agregação física

Entradas e saídas do mês:

- `valor_entradas`: soma de `preco_item` das linhas `1 - ENTRADA`;
- `qtd_entradas`: soma de `q_conv` das linhas `1 - ENTRADA`;
- `valor_saidas`: soma do valor absoluto de `preco_item` das linhas `2 - SAIDAS`;
- `qtd_saidas`: soma do valor absoluto de `q_conv` das linhas `2 - SAIDAS`.

Entradas desacobertadas:

```text
entradas_desacob = soma mensal de entr_desac_anual
```

Ou seja, a mensal apenas resume eventos já detectados na `mov_estoque`.

## Médias do mês

`pme_mes` e `pms_mes` usam somente movimentos válidos. Ficam fora:

- devoluções identificadas por `dev_simples`, `dev_venda`, `dev_compra`, `dev_ent_simples` ou `finnfe = 4`;
- linhas com `excluir_estoque = true`;
- linhas neutralizadas com `q_conv <= 0`.

Fórmulas:

```text
pme_mes = soma(valor das entradas válidas) / soma(qtd das entradas válidas)
pms_mes = soma(valor das saídas válidas) / soma(qtd das saídas válidas)
```

## Saldo e valor de estoque

A visão mensal aproveita o fechamento cronológico já calculado:

- `saldo_mes`: último `saldo_estoque_anual` do mês;
- `custo_medio_mes`: último `custo_medio_anual` do mês;
- `valor_estoque = saldo_mes * custo_medio_mes`.

## ST mensal e ICMS de entrada desacobertada

A regra de ST não usa apenas a última linha da `mov_estoque`. O processo cruza `co_sefin_agr` com `sitafe_produto_sefin_aux.parquet` e mantém os períodos cuja vigência intersecta o mês analisado.

Campos relevantes:

- `ST`: histórico textual dos períodos de ST do mês;
- `__tem_st_mes__`: flag interna;
- `MVA`: `it_pc_mva` da última movimentação válida do mês, apenas quando há ST;
- `MVA_ajustado`: preenchido somente quando `it_in_mva_ajustado = 'S'`.

`ICMS_entr_desacob` só é calculado quando:

- há ST no mês;
- `entradas_desacob > 0`.

Fórmula implementada:

```text
se pms_mes > 0:
    ICMS_entr_desacob = pms_mes * entradas_desacob * (aliq_mes / 100)
senão:
    ICMS_entr_desacob = pme_mes * entradas_desacob * (aliq_mes / 100) * MVA_efetivo
```

Onde `MVA_efetivo` é:

- `it_pc_mva / 100`, quando `it_in_mva_ajustado = 'N'`;
- `[((1 + MVA_orig) * (1 - ALQ_inter)) / (1 - ALQ_interna)] - 1`, quando `it_in_mva_ajustado = 'S'`.

## Arredondamento

- quantidades e saldos: 4 casas;
- valores monetários: 2 casas;
- `MVA_ajustado`: 6 casas.

## Saída gerada

```text
dados/CNPJ/<cnpj>/analises/produtos/aba_mensal_<cnpj>.parquet
```
