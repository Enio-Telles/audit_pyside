# Tabela de períodos

Este documento descreve a geração da **tabela_periodos\_<cnpj>.parquet**, que resume a auditoria por **período de inventário** e produto agregado. A nomenclatura foi atualizada para refletir as novas convenções de quantidade e agrupamento.

## Identificação fiscal (SITAFE)

A fim de aplicar corretamente alíquotas e regras de substituição tributária (ST), cada produto agregado é vinculado a um código interno `co_sefin` mediante consulta às tabelas oficiais do SITAFE. A precedência de match é: (1) `CEST + NCM`; (2) `CEST`; (3) `NCM`. A identificação ocorre na etapa de **item_unidades**.

## Objetivo da tabela

Para cada `id_produto_agrupado` e período de inventário, a tabela confronta:

- saldo inicial do período;
- entradas e saídas físicas no período;
- estoque final declarado no inventário daquele período;
- saldo final calculado pelo fluxo cronológico;
- divergências (entradas ou saídas desacobertadas);
- bases e valores de ICMS presumidos.

Ao contrário da tabela anual (que utiliza o ano civil), aqui o **período de inventário** é definido pelo campo `periodo_inventario` da `movimentacao_estoque`, incrementado a cada inventário inicial. Isso permite auditoria fiscal em períodos customizados.

## Campos principais

### Identificação e período

| Campo                | Tipo   | Descrição |
|----------------------|--------|-----------|
| `codigo_periodo`     | int    | Código sequencial do período de inventário. Alias de `periodo_inventario`. |
| `periodo_label`      | str    | Intervalo de datas no formato `DD/MM/AAAA até DD/MM/AAAA`. |
| `id_produto_agrupado`| str    | Identificador do produto agregado. |
| `descricao_padrao`   | str    | Descrição padronizada do grupo. |
| `unidade_referencia` | str    | Unidade de referência definida na conversão de unidades. |

### Quantidades físicas

| Campo                       | Tipo    | Descrição |
|-----------------------------|---------|-----------|
| `estoque_inicial`           | float   | Soma de `quantidade_fisica` das linhas `0 – ESTOQUE INICIAL`. |
| `entradas`                  | float   | Soma de `quantidade_fisica` das linhas `1 – ENTRADA`. |
| `saidas`                    | float   | Soma do valor absoluto de `quantidade_fisica` das linhas `2 – SAIDA`. |
| `estoque_final_declarado`   | float   | Soma de `estoque_final_declarado` das linhas `3 – ESTOQUE FINAL`. |
| `entradas_desacobertas`     | float   | Soma de divergências de entrada (`entr_desac_periodo`) no período. |
| `saldo_final_calculado`     | float   | Último `saldo_estoque_periodo` calculado pelo fluxo. |

### Divergências

| Campo                  | Tipo  | Fórmula |
|------------------------|-------|---------|
| `saidas_calculadas`    | float | `estoque_inicial + entradas + entradas_desacobertas − estoque_final_declarado` |
| `saidas_desacobertas`  | float | `max(estoque_final_declarado − saldo_final_calculado, 0)` |
| `estoque_desacoberto`  | float | `max(saldo_final_calculado − estoque_final_declarado, 0)` |

Os campos `saidas_desacobertas` e `estoque_desacoberto` são mutuamente exclusivos: apenas um deles pode ser positivo em um mesmo período.

### Preços médios e alíquotas

| Campo             | Tipo  | Descrição |
|-------------------|-------|-----------|
| `pme`             | float | Preço médio de entrada do período (`valor_entradas_validas / quantidade_entradas_validas`). |
| `pms`             | float | Preço médio de saída do período (`valor_saidas_validas / quantidade_saidas_validas`). |
| `aliquota_interna`| float | Alíquota interna de ICMS conforme SITAFE ou fallback para a última alíquota movimentada. |
| `ST`              | str   | Histórico textual de períodos de substituição tributária vigentes no período. |

### Bases e ICMS

A base de saída é calculada da seguinte forma:

* se `pms > 0`: `base_saida = saidas_desacobertas * pms`;
* caso contrário: `base_saida = saidas_desacobertas * pme * 1.30`.

A base de estoque é calculada como:

* se `pms > 0`: `base_estoque = estoque_desacoberto * pms`;
* caso contrário: `base_estoque = estoque_desacoberto * pme * 1.30`.

O ICMS presumido é obtido multiplicando a base pela alíquota interna:

* `icms_saidas_desacobertas = base_saida * (aliquota_interna / 100)`;
* `icms_estoque_desacoberto = base_estoque * (aliquota_interna / 100)`.

Se houver ST vigente (`ST` não vazio), o ICMS sobre saídas desacobertas é zerado; o ICMS sobre estoque desacoberto permanece.

## Arredondamento

* Quantidades e saldos: quatro casas decimais.
* Valores monetários e alíquotas: duas casas decimais.
* Preços médios: quatro casas decimais.

## Diferenças em relação à tabela anual

| Aspecto                | Tabela anual         | Tabela de períodos |
|------------------------|----------------------|--------------------|
| Unidade de agrupamento | Ano civil (`ano`)    | Período de inventário (`codigo_periodo`) |
| Campos de saldo        | sufixo `_anual`      | sufixo `_periodo` |
| Granularidade          | Uma linha por produto por ano | Uma linha por produto por período |
| Uso ideal              | Auditoria anual consolidada | Auditoria fiscal por períodos customizados |

## Saída gerada

O arquivo gerado é salvo em `dados/CNPJ/<cnpj>/analises/produtos/tabela_periodos_<cnpj>.parquet`.

Ao seguir estas nomenclaturas e fórmulas, a tabela de períodos fornece uma visão coerente das divergências de estoque e da base de cálculo do ICMS por intervalo de inventário.