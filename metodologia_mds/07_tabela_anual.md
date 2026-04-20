# Tabela anual
<a id="mds-07-tabela-anual"></a>

Este documento consolida as regras da **tabela_anual\_<cnpj>.parquet**, gerada pelo módulo de cálculos anuais. A nomenclatura segue o padrão adotado nas demais camadas para garantir consistência e legibilidade.

## Identificação fiscal (SITAFE)

Tal como nas demais tabelas, cada `id_produto_agrupado` é vinculado a um `co_sefin` com base nas tabelas de referência da SEFIN. O match é feito por `CEST + NCM`, `CEST` ou `NCM`, em ordem de prioridade.

## Objetivo da tabela

A tabela anual resume a auditoria de cada produto agregado por ano civil, confrontando:

* estoque inicial;
* entradas e saídas físicas;
* estoque final declarado;
* saldo final calculado pelo fluxo cronológico;
* divergências (entradas, saídas e estoque final desacobertos);
* bases e valores de ICMS presumidos.

## Campos principais

### Identificação e agrupamento

| Campo                 | Tipo | Descrição |
|-----------------------|------|-----------|
| `ano`                 | int  | Ano civil da movimentação. |
| `id_produto_agrupado` | str  | Identificador do produto agregado. |
| `descricao_padrao`    | str  | Descrição padronizada do grupo. |
| `unidade_referencia`  | str  | Unidade de referência adotada na conversão de unidades. |

### Quantidades físicas

| Campo                         | Tipo  | Descrição |
|-------------------------------|-------|-----------|
| `estoque_inicial`             | float | Soma de `quantidade_fisica` das linhas `0 – ESTOQUE INICIAL` ao longo do ano. |
| `entradas`                    | float | Soma de `quantidade_fisica` das linhas `1 – ENTRADA`. |
| `saidas`                      | float | Soma do valor absoluto de `quantidade_fisica` das linhas `2 – SAIDA`. |
| `estoque_final_declarado`     | float | Soma de `estoque_final_declarado` das linhas `3 – ESTOQUE FINAL` ao longo do ano. |
| `entradas_desacobertas`       | float | Soma anual de divergências de entrada (`entr_desac_anual`). |
| `saldo_final_calculado`       | float | Último `saldo_estoque_anual` calculado no ano. |

### Divergências

| Campo                         | Tipo  | Fórmula |
|-------------------------------|-------|---------|
| `saidas_calculadas`           | float | `estoque_inicial + entradas + entradas_desacobertas − estoque_final_declarado` |
| `saidas_desacobertas`         | float | `max(estoque_final_declarado − saldo_final_calculado, 0)` |
| `estoque_final_desacoberto`   | float | `max(saldo_final_calculado − estoque_final_declarado, 0)` |

### Preços médios e alíquotas

| Campo              | Tipo  | Descrição |
|--------------------|-------|-----------|
| `pme`              | float | Preço médio de entrada anual (`valor_entradas_validas / quantidade_entradas_validas`). |
| `pms`              | float | Preço médio de saída anual (`valor_saidas_validas / quantidade_saidas_validas`). |
| `aliquota_interna` | float | Alíquota interna de ICMS segundo SITAFE ou último valor movimentado. |
| `ST`               | str   | Histórico textual de períodos de ST vigentes no ano. |

### Bases e ICMS

A base de saída e a base de estoque são calculadas com as mesmas fórmulas da tabela de períodos, substituindo a granularidade anual. O ICMS presumido se obtém multiplicando a base pela alíquota interna; se houver ST, o ICMS de saídas desacobertas é zerado.

## Arredondamento

* Quantidades e saldos: 4 casas decimais.
* `pme`, `pms`, `aliquota_interna`, `icms_saidas_desacobertas` e `icms_estoque_final_desacoberto`: 2 casas decimais.

## Saída gerada

O arquivo resultante é salvo em `dados/CNPJ/<cnpj>/analises/produtos/tabela_anual_<cnpj>.parquet`.

Com a padronização de nomenclaturas e a aplicação das fórmulas acima, a tabela anual oferece uma visão consolidada e auditável do comportamento de estoque e da tributação presumida ao longo do ano.