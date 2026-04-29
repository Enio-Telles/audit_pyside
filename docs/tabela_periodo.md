# Tabela de Períodos

## Identificação Fiscal (SITAFE)

Para garantir a acurácia fiscal na auditoria por períodos de inventário, o sistema utiliza o `co_sefin` resolvido via SITAFE. A identificação prioriza a combinação `CEST + NCM` (tabela `sitafe_cest_ncm.parquet`), recorrendo a `CEST` ou `NCM` individualmente apenas se necessário. Isso assegura que as alíquotas e regras de ressarcimento sejam aplicadas corretamente em cada intervalo.

Este documento consolida as regras da `aba_periodos_<cnpj>.parquet`, gerada por `src/transformacao/calculos_periodo_pkg/calculos_periodo.py`.

## Papel da tabela

A tabela de períodos resume a auditoria por `id_agrupado` e **período de inventário**, confrontando:

- estoque inicial do período;
- entradas e saídas declaradas no período;
- estoque final declarado no período;
- saldo final calculado pelo fluxo cronológico;
- reflexos de ICMS sobre saídas e estoque desacobertados.

Diferente da tabela anual (que usa ano civil como unidade), a tabela de períodos usa o campo `periodo_inventario` da `mov_estoque`, que é incrementado a cada `0 - ESTOQUE INICIAL`. Isso permite auditoria fiscal por períodos customizados, não restritos ao calendário.

Na saída, `id_agrupado` é exposto como `id_agregado` e `periodo_inventario` como `cod_per`.

## Base de cálculo

A tabela de períodos reaproveita a `mov_estoque`, principalmente:

- `q_conv`
- `entr_desac_periodo`
- `saldo_estoque_periodo`
- `preco_item` / `Vl_item`
- `it_pc_interna`
- `co_sefin_agr`
- `descr_padrao`
- `unid_ref`
- `__qtd_decl_final_audit__`
- `periodo_inventario`

## Quantitativos por período

Agregações físicas:

- `estoque_inicial`: soma de `q_conv` das linhas `0 - ESTOQUE INICIAL`;
- `entradas`: soma de `q_conv` das linhas `1 - ENTRADA`;
- `saidas`: soma de `q_conv` das linhas `2 - SAIDAS`;
- `estoque_final`: soma de `__qtd_decl_final_audit__` das linhas `3 - ESTOQUE FINAL`;
- `entradas_desacob`: soma de `entr_desac_periodo` no período;
- `saldo_final`: último `saldo_estoque_periodo` do período.

Importante:

- `3 - ESTOQUE FINAL` não cria `entradas_desacob`;
- ele só informa o inventário declarado para auditoria do período.

## Fórmulas principais

```text
saidas_calculadas = estoque_inicial + entradas + entradas_desacob - estoque_final
saidas_desacob = max(estoque_final - saldo_final, 0)
estoque_final_desacob = max(saldo_final - estoque_final, 0)
```

`saidas_desacob` e `estoque_final_desacob` são mutuamente exclusivos por construção. Se um deles é positivo, o outro fica zerado.

## PME e PMS do período

As médias do período usam movimentos válidos, excluindo:

- devoluções identificadas por `dev_simples`;
- linhas com `excluir_estoque = true`;
- linhas com `q_conv <= 0`.

Fórmulas:

```text
pme = soma(valor das entradas válidas) / soma(qtd das entradas válidas)
pms = soma(valor das saídas válidas) / soma(qtd das saídas válidas)
```

O valor unitário da agregação usa `preco_item` e, na falta dele, `Vl_item`.

## Regra de ST por período

O módulo cruza `co_sefin_agr` com `sitafe_produto_sefin_aux.parquet` e mantém vigências que intersectam o período analisado (definido por `__data_inicio__` e `__data_fim__`).

Campos relevantes:

- `ST`: histórico textual dos períodos de ST;
- `__tem_st_per__`: flag interna indicando se há ST vigente no período;
- `aliq_interna`: prioridade para a alíquota da referência SEFIN, com fallback para a última alíquota da movimentação.

## ICMS por período

Base de saída:

```text
se pms > 0:
    base_saida = saidas_desacob * pms
senão:
    base_saida = saidas_desacob * pme * 1.30
```

Base de estoque:

```text
se pms > 0:
    base_estoque = estoque_final_desacob * pms
senão:
    base_estoque = estoque_final_desacob * pme * 1.30
```

Aplicação da alíquota:

```text
aliq_factor = aliq_interna / 100
ICMS_saidas_desac = base_saida * aliq_factor
ICMS_estoque_desac = base_estoque * aliq_factor
```

Regra de ST vigente no código:

- se `__tem_st_per__ = true`, `ICMS_saidas_desac = 0`;
- `ICMS_estoque_desac` não é zerado por ST.

## Campos da Tabela

### Identificação e Período

| Campo | Tipo | Descrição |
|---|---|---|
| `cod_per` | `int` | Código do período de inventário (alias de `periodo_inventario`) |
| `periodo_label` | `str` | Rótulo do período no formato `"DD/MM/YYYY até DD/MM/YYYY"` |
| `id_agregado` | `str` | Chave mestra de agrupamento do produto |
| `descr_padrao` | `str` | Descrição padrão normalizada do agrupamento |
| `unid_ref` | `str` | Unidade de referência sugerida |

### Quantitativos Físicos

| Campo | Tipo | Descrição |
|---|---|---|
| `estoque_inicial` | `float` | Soma de `q_conv` das linhas `0 - ESTOQUE INICIAL` |
| `entradas` | `float` | Soma de `q_conv` das linhas `1 - ENTRADA` |
| `saidas` | `float` | Soma de `q_conv` das linhas `2 - SAIDAS` |
| `estoque_final` | `float` | Soma de `__qtd_decl_final_audit__` das linhas `3 - ESTOQUE FINAL` |
| `saidas_calculadas` | `float` | `estoque_inicial + entradas + entradas_desacob - estoque_final` |
| `saldo_final` | `float` | Último `saldo_estoque_periodo` do período |

### Divergências

| Campo | Tipo | Descrição |
|---|---|---|
| `entradas_desacob` | `float` | Soma de `entr_desac_periodo` no período |
| `saidas_desacob` | `float` | `max(estoque_final - saldo_final, 0)` — saídas sem cobertura de estoque |
| `estoque_final_desacob` | `float` | `max(saldo_final - estoque_final, 0)` — estoque físico acima do declarado |

### Preços Médios e Alíquotas

| Campo | Tipo | Descrição |
|---|---|---|
| `pme` | `float` | Preço médio de entrada do período |
| `pms` | `float` | Preço médio de saída do período |
| `aliq_interna` | `float` | Alíquota interna de ICMS (SEFIN ou fallback da movimentação) |

### Substituição Tributária e ICMS

| Campo | Tipo | Descrição |
|---|---|---|
| `ST` | `str` | Histórico textual dos períodos de ST vigentes |
| `ICMS_saidas_desac` | `float` | ICMS sobre saídas desacobertadas (zerado se ST vigente) |
| `ICMS_estoque_desac` | `float` | ICMS sobre estoque final desacobertado |

## Arredondamento

| Categoria | Casas Decimais |
|---|---|
| Quantidades e saldos (`estoque_inicial`, `entradas`, `saidas`, `estoque_final`, `saidas_calculadas`, `saldo_final`, `entradas_desacob`, `saidas_desacob`, `estoque_final_desacob`) | 4 |
| Valores e alíquotas (`pme`, `pms`, `ICMS_saidas_desac`, `ICMS_estoque_desac`, `aliq_interna`) | 2 |

## Diferenças entre tabela anual e tabela de períodos

| Aspecto | Tabela Anual | Tabela de Períodos |
|---|---|---|
| Unidade de agrupamento | Ano civil (`ano`) | Período de inventário (`periodo_inventario`) |
| Campos de saldo | `*_anual` | `*_periodo` |
| Granularidade | Uma linha por `id_agrupado` por ano | Uma linha por `id_agrupado` por período de inventário |
| Uso ideal | Auditoria anual consolidada | Auditoria por períodos fiscais customizados |

## Saída gerada

```text
dados/CNPJ/<cnpj>/analises/produtos/aba_periodos_<cnpj>.parquet
```
