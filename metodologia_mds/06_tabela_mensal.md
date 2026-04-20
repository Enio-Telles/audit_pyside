# Tabela mensal

Este documento consolida as regras da **tabela_mensal\_<cnpj>.parquet**, gerada a partir da `movimentacao_estoque` pelo módulo de cálculos mensais. A nomenclatura foi revisada para manter coerência com as demais camadas do pipeline.

## Identificação fiscal (SITAFE)

Cada produto agregado é vinculado a um código `co_sefin` com base no match por `CEST + NCM`, `CEST` ou `NCM`. Este vínculo ocorre na etapa `item_unidades` e é reaproveitado aqui para calcular alíquotas, ST e MVA.

## Papel da tabela

A tabela mensal resume a movimentação de cada `id_produto_agrupado` em cada mês civil, sem recalcular o saldo cronológico do zero. Ela reutiliza os saldos e custos já materializados na `movimentacao_estoque`.

## Chaves de agrupamento

A agregação é feita por:

- `ano` e `mes` (mês civil da operação);
- `id_produto_agrupado`.

No resultado, estes campos são expostos como `ano`, `mes` e `id_produto_agrupado`.

## Campos principais

### Identificação e agrupamento

| Campo                 | Tipo        | Descrição |
|-----------------------|-------------|-----------|
| `ano`                 | int         | Ano civil do movimento. |
| `mes`                 | int         | Mês do movimento (1–12). |
| `id_produto_agrupado` | str         | Chave mestra de agrupamento do produto. |
| `descricao_padrao`    | str         | Descrição padrão normalizada do grupo. |
| `unidades_mes`        | list[str]   | Lista de unidades de medida usadas no mês. |
| `unidades_referencia_mes` | list[str] | Lista de unidades de referência utilizadas no mês. |

### Entradas e saídas

| Campo                       | Tipo    | Descrição |
|-----------------------------|---------|-----------|
| `valor_entradas`            | float   | Soma de `preco_item` das linhas `1 – ENTRADA`. |
| `quantidade_entradas`       | float   | Soma de `quantidade_fisica` das entradas. |
| `preco_medio_entradas_mes`  | float   | `valor_entradas_validas / quantidade_entradas_validas`. |
| `valor_saidas`              | float   | Soma do valor absoluto de `preco_item` das linhas `2 – SAIDA`. |
| `quantidade_saidas`         | float   | Soma do valor absoluto de `quantidade_fisica` das saídas. |
| `preco_medio_saidas_mes`    | float   | `valor_saidas_validas / quantidade_saidas_validas`. |

### Saldos e estoque

| Campo                  | Tipo    | Descrição |
|------------------------|---------|-----------|
| `saldo_mes`            | float   | Último `saldo_estoque_anual` do mês. |
| `custo_medio_mes`      | float   | Último `custo_medio_anual` do mês. |
| `valor_estoque`        | float   | `saldo_mes * custo_medio_mes`. |

### Entradas desacobertas e ICMS

| Campo                          | Tipo    | Descrição |
|-------------------------------|---------|-----------|
| `entradas_desacobertas`       | float   | Soma mensal de divergências de entrada (`entr_desac_anual`). |
| `icms_entradas_desacobertas`  | float   | ICMS calculado sobre entradas desacobertas (apenas quando há ST vigente). |

### Substituição tributária (ST)

| Campo               | Tipo   | Descrição |
|---------------------|--------|-----------|
| `ST`                | str    | Histórico textual dos períodos de ST do mês. |
| `sujeito_a_st`      | str    | Flag "S"/"N" indicando se o produto está sujeito a ST. |
| `MVA`               | float  | Percentual de margem de valor agregado (`it_pc_mva`) da última movimentação válida do mês (quando houver ST). |
| `MVA_ajustado`      | float  | MVA ajustado conforme legislação, quando aplicável. |

### Campos por período de inventário

Para auditoria mais granular, a tabela mensal inclui campos com sufixo `_periodo` que refletem os cálculos por período de inventário dentro do mês:

| Campo                             | Tipo    | Descrição |
|-----------------------------------|---------|-----------|
| `entradas_desacobertas_periodo`   | float   | Soma de divergências de entrada (`entr_desac_periodo`) no mês. |
| `icms_entradas_desacobertas_periodo` | float | ICMS de entradas desacobertas por período. |
| `saldo_mes_periodo`               | float   | Último `saldo_estoque_periodo` do mês. |
| `custo_medio_mes_periodo`         | float   | Último `custo_medio_periodo` do mês. |
| `valor_estoque_periodo`           | float   | `saldo_mes_periodo * custo_medio_mes_periodo`. |

## Médias do mês

Os preços médios (`preco_medio_entradas_mes` e `preco_medio_saidas_mes`) são calculados usando apenas movimentos válidos. Excluem‑se devoluções (identificadas por CFOPs de devolução ou `finnfe = 4`), linhas marcadas como `excluir_estoque = true` e linhas com `quantidade_fisica <= 0`. A fórmula é:

```
preco_medio_entradas_mes = soma(valor das entradas válidas) / soma(quantidade das entradas válidas)
preco_medio_saidas_mes   = soma(valor das saídas válidas)   / soma(quantidade das saídas válidas)
```

## ST e ICMS de entradas desacobertas

O ICMS sobre entradas desacobertas é calculado somente se houver ST vigente no mês e se `entradas_desacobertas > 0`. A fórmula implementada é:

```
if preco_medio_saidas_mes > 0:
    icms_entradas_desacobertas = preco_medio_saidas_mes * entradas_desacobertas * (aliq_mes / 100)
else:
    icms_entradas_desacobertas = preco_medio_entradas_mes * entradas_desacobertas * (aliq_mes / 100) * mva_efetivo
```

onde `mva_efetivo` é calculado a partir do MVA original (`it_pc_mva`) e das alíquotas interestaduais e internas, de acordo com a legislação.

## Arredondamento

| Categoria                                    | Casas decimais |
|----------------------------------------------|----------------|
| Quantidades e saldos (`quantidade_entradas`, `quantidade_saidas`, `saldo_mes`, `entradas_desacobertas`) | 4 |
| Valores monetários (`valor_entradas`, `valor_saidas`, `valor_estoque`, `icms_entradas_desacobertas`)   | 2 |
| Preços médios (`preco_medio_entradas_mes`, `preco_medio_saidas_mes`, `custo_medio_mes`, `MVA`)         | 4 |
| MVA ajustado (`MVA_ajustado`)                                                                          | 6 |

## Saída gerada

O arquivo gerado é salvo em `dados/CNPJ/<cnpj>/analises/produtos/tabela_mensal_<cnpj>.parquet`.

Com estas nomenclaturas e regras, a tabela mensal fornece um resumo claro da movimentação, do preço médio e do ICMS sobre entradas desacobertas, integrando‑se de forma consistente às camadas anual e de períodos.