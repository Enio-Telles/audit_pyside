# Tabela Mensal

## Identificação Fiscal (SITAFE)

Toda a lógica de tributação e agregação mensal baseia-se no `co_sefin` identificado na etapa inicial do pipeline. O sistema consulta as tabelas oficiais do SITAFE (`dados/referencias/CO_SEFIN/`) priorizando o match em `CEST + NCM` e utilizando `CEST` ou `NCM` isolados apenas como fallback. Isso garante integridade na aplicação das alíquotas mensais.

Este documento consolida as regras da `aba_mensal_<cnpj>.parquet`, gerada por `src/transformacao/calculos_mensais_pkg/calculos_mensais.py`.

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

## Campos da Tabela

### Identificação e Agrupamento

| Campo           | Tipo        | Descrição                                                      |
|-----------------|-------------|----------------------------------------------------------------|
| `ano`           | `int`       | Ano civil do movimento                                         |
| `mes`           | `int`       | Mês do movimento (1-12)                                        |
| `id_agregado`   | `str`       | Chave mestra de agrupamento do produto (ex: `PROD_MSTR_00001`) |
| `descr_padrao`  | `str`       | Descrição padrão normalizada do agrupamento                    |
| `unids_mes`     | `list[str]` | Lista de unidades de medida usadas no mês                      |
| `unids_ref_mes` | `list[str]` | Lista de unidades de referência usadas no mês                  |

### Entradas e Saídas

| Campo            | Tipo    | Descrição                                                               |
|------------------|---------|-------------------------------------------------------------------------|
| `valor_entradas` | `float` | Soma de `preco_item` das linhas `1 - ENTRADA`                           |
| `qtd_entradas`   | `float` | Soma de `q_conv` das linhas `1 - ENTRADA`                               |
| `pme_mes`        | `float` | Preço médio de entrada: `valor_entradas_validas / qtd_entradas_validas` |
| `valor_saidas`   | `float` | Soma do valor absoluto de `preco_item` das linhas `2 - SAIDAS`          |
| `qtd_saidas`     | `float` | Soma do valor absoluto de `q_conv` das linhas `2 - SAIDAS`              |
| `pms_mes`        | `float` | Preço médio de saída: `valor_saidas_validas / qtd_saidas_validas`       |

### Saldos e Estoque

| Campo             | Tipo    | Descrição                           |
|-------------------|---------|-------------------------------------|
| `saldo_mes`       | `float` | Último `saldo_estoque_anual` do mês |
| `custo_medio_mes` | `float` | Último `custo_medio_anual` do mês   |
| `valor_estoque`   | `float` | `saldo_mes * custo_medio_mes`       |

### Entradas Desacobertadas e ICMS

| Campo               | Tipo    | Descrição                                                                  |
|---------------------|---------|----------------------------------------------------------------------------|
| `entradas_desacob`  | `float` | Soma mensal de `entr_desac_anual`                                          |
| `ICMS_entr_desacob` | `float` | ICMS calculado sobre entradas desacobertadas (apenas quando há ST vigente) |

### Substituição Tributária (ST)

| Campo          | Tipo    | Descrição                                                                              |
|----------------|---------|----------------------------------------------------------------------------------------|
| `ST`           | `str`   | Histórico textual dos períodos de ST do mês                                            |
| `it_in_st`     | `str`   | Flag "S"/"N" de sujeito a ST                                                           |
| `MVA`          | `float` | Percentual MVA (`it_pc_mva`) da última movimentação válida do mês, apenas quando há ST |
| `MVA_ajustado` | `float` | MVA ajustado pela fórmula, quando `it_in_mva_ajustado = 'S'`                           |

### Campos por Período de Inventário (sufixo `_periodo`)

| Campo                       | Tipo    | Descrição                                      |
|-----------------------------|---------|------------------------------------------------|
| `entradas_desacob_periodo`  | `float` | Soma de `entr_desac_periodo` no mês            |
| `ICMS_entr_desacob_periodo` | `float` | ICMS sobre entradas desacobertadas por período |
| `saldo_mes_periodo`         | `float` | Último `saldo_estoque_periodo` do mês          |
| `custo_medio_mes_periodo`   | `float` | Último `custo_medio_periodo` do mês            |
| `valor_estoque_periodo`     | `float` | `saldo_mes_periodo * custo_medio_mes_periodo`  |

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

## Médias do mês (PME e PMS)

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

**Nota sobre arredondamento para cálculo de ICMS:**

As médias usadas na base do ICMS (`__pme_mes_icms__` e `__pms_mes_icms__`) são arredondadas para 2 casas decimais antes da multiplicação, evitando diferença entre o valor exibido e o valor efetivamente calculado.

## Arredondamento

| Categoria                                                                                   | Casas Decimais |
|---------------------------------------------------------------------------------------------|----------------|
| Quantidades e saldos (`qtd_entradas`, `qtd_saidas`, `saldo_mes`, `entradas_desacob`)        | 4              |
| Valores monetários (`valor_entradas`, `valor_saidas`, `valor_estoque`, `ICMS_entr_desacob`) | 2              |
| Preços médios (`pme_mes`, `pms_mes`, `custo_medio_mes`, `MVA`)                              | 4              |
| MVA ajustado (`MVA_ajustado`)                                                               | 6              |

## Saída gerada

```text
dados/CNPJ/<cnpj>/analises/produtos/aba_mensal_<cnpj>.parquet
```

## Período de Inventário

A tabela mensal inclui campos com sufixo `_periodo` que refletem o cálculo por período de inventário (campo `periodo_inventario` da `mov_estoque`). Isso permite auditoria independente por período fiscal customizado, não apenas ano civil.

Os campos `_periodo` seguem as mesmas regras dos campos anuais, mas são recalculados a cada `0 - ESTOQUE INICIAL` dentro de um `id_agrupado`:

- `entradas_desacob_periodo`: soma de `entr_desac_periodo` no mês;
- `saldo_mes_periodo`: último `saldo_estoque_periodo` do mês;
- `custo_medio_mes_periodo`: último `custo_medio_periodo` do mês;
- `valor_estoque_periodo`: `saldo_mes_periodo * custo_medio_mes_periodo`;
- `ICMS_entr_desacob_periodo`: ICMS sobre entradas desacobertadas por período.
