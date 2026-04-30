# Tabelas Analíticas — Campos, Relações e Fórmulas

Esta página documenta `calculos_mensais`, `calculos_anuais` e o contrato esperado de `calculos_periodos`.

Essas tabelas resumem a `movimentacao_estoque` para auditoria, relatórios e apuração fiscal.

---

## 1. `aba_mensal_<cnpj>.parquet` / `calculos_mensais`

### Papel

Resume a movimentação por produto, ano e mês. Não recalcula o saldo cronológico: reaproveita o saldo e custo médio já calculados em `movimentacao_estoque`.

### Relações

| Origem | Destino | Campos | Como interpretar |
|---|---|---|---|
| `movimentacao_estoque` | `calculos_mensais` | `id_agrupado`, `Dt_e_s`, `Dt_doc`, `q_conv`, `preco_item` | Agrega movimentos por produto/mês. |
| `movimentacao_estoque` | `calculos_mensais` | `saldo_estoque_anual`, `custo_medio_anual` | Último saldo/custo do mês vira fechamento mensal. |
| `calculos_mensais` | relatórios/GUI | `id_agregado`, `ano`, `mes` | Base da aba mensal. |

### Campos

| Campo | Tipo | Explicação | Relação/Fórmula |
|---|---|---|---|
| `id_agregado` | `str` | Alias analítico de `id_agrupado`. | Liga relatório ao produto agrupado. |
| `ano` | `int` | Ano civil do movimento. | Derivado de `Dt_e_s` ou `Dt_doc`. |
| `mes` | `int` | Mês do movimento, de 1 a 12. | Derivado de `Dt_e_s` ou `Dt_doc`. |
| `descr_padrao` | `str` | Descrição padrão do produto. | Vem da movimentação/produto final. |
| `unid_ref` | `str` | Unidade de referência. | Vem de fatores/movimentação. |
| `valor_entradas` | `float` | Valor total das entradas do mês. | Soma de `preco_item` das entradas. |
| `qtd_entradas` | `float` | Quantidade convertida de entradas. | Soma de `q_conv` das entradas. |
| `valor_saidas` | `float` | Valor total das saídas do mês. | Soma de `abs(preco_item)` das saídas. |
| `qtd_saidas` | `float` | Quantidade convertida de saídas. | Soma de `abs(q_conv)` das saídas. |
| `pme_mes` | `float` | Preço médio de entrada mensal. | Entradas válidas: valor / quantidade. |
| `pms_mes` | `float` | Preço médio de saída mensal. | Saídas válidas: valor / quantidade. |
| `entradas_desacob` | `float` | Entradas desacobertadas no mês. | Soma mensal de `entr_desac_anual`. |
| `ICMS_entr_desacob` | `float` | ICMS sobre entradas desacobertadas. | Fórmula fiscal mensal. |
| `saldo_mes` | `float` | Saldo físico no fim do mês. | Último `saldo_estoque_anual` do mês. |
| `custo_medio_mes` | `float` | Custo médio no fim do mês. | Último `custo_medio_anual` do mês. |
| `valor_estoque` | `float` | Valor do estoque no fim do mês. | `saldo_mes * custo_medio_mes`. |
| `ST` | `str` | Histórico/indicação de ST no mês. | Vem do cruzamento SITAFE. |
| `MVA` | `float` | MVA original aplicável. | Usada na apuração quando há ST. |
| `MVA_ajustado` | `float` | MVA ajustada. | Calculada quando a regra fiscal exigir ajuste. |

### Fórmulas

Data efetiva:

```text
data_efetiva = Dt_e_s se existir
senão data_efetiva = Dt_doc
```

Período:

```text
ano = ano(data_efetiva)
mes = mes(data_efetiva)
```

Entradas:

```text
valor_entradas = soma(preco_item onde Tipo_operacao = '1 - ENTRADA')
qtd_entradas = soma(q_conv onde Tipo_operacao = '1 - ENTRADA')
```

Saídas:

```text
valor_saidas = soma(abs(preco_item) onde Tipo_operacao = '2 - SAIDAS')
qtd_saidas = soma(abs(q_conv) onde Tipo_operacao = '2 - SAIDAS')
```

Preços médios:

```text
pme_mes = soma(valor das entradas válidas) / soma(qtd das entradas válidas)
pms_mes = soma(valor das saídas válidas) / soma(qtd das saídas válidas)
```

Fechamento do mês:

```text
saldo_mes = último saldo_estoque_anual do mês
custo_medio_mes = último custo_medio_anual do mês
valor_estoque = saldo_mes * custo_medio_mes
```

ICMS sobre entradas desacobertadas:

```text
se entradas_desacob > 0 e há ST no mês:
    se pms_mes > 0:
        ICMS_entr_desacob = pms_mes * entradas_desacob * (aliq_mes / 100)
    senão:
        ICMS_entr_desacob = pme_mes * entradas_desacob * (aliq_mes / 100) * MVA_efetivo
senão:
    ICMS_entr_desacob = 0
```

MVA ajustada:

```text
MVA_ajustado = ((1 + MVA_orig) * (1 - ALQ_inter) / (1 - ALQ_interna)) - 1
```

---

## 2. `aba_anual_<cnpj>.parquet` / `calculos_anuais`

### Papel

Resume a auditoria anual por produto, confrontando estoque inicial, entradas, saídas, estoque final declarado, saldo calculado e possíveis divergências tributáveis.

### Relações

| Origem | Destino | Campos | Como interpretar |
|---|---|---|---|
| `movimentacao_estoque` | `calculos_anuais` | `id_agrupado`, ano, `q_conv`, `__qtd_decl_final_audit__` | Agrega quantitativos anuais. |
| `movimentacao_estoque` | `calculos_anuais` | `saldo_estoque_anual`, `entr_desac_anual`, `custo_medio_anual` | Usa resultado cronológico já calculado. |
| SITAFE/classificação | `calculos_anuais` | `co_sefin_agr`, vigências, ST, alíquota | Define regra fiscal anual. |
| `calculos_anuais` | relatórios/GUI | `id_agregado`, `ano` | Base da aba anual. |

### Campos

| Campo | Tipo | Explicação | Relação/Fórmula |
|---|---|---|---|
| `id_agregado` | `str` | Alias analítico de `id_agrupado`. | Chave do produto no relatório. |
| `ano` | `int` | Ano civil. | Derivado das datas da movimentação. |
| `descr_padrao` | `str` | Descrição padrão do produto. | Vem do agrupamento/movimentação. |
| `unid_ref` | `str` | Unidade de referência. | Unidade comum das quantidades. |
| `estoque_inicial` | `float` | Estoque inicial do ano. | Soma de `q_conv` de estoque inicial. |
| `entradas` | `float` | Entradas do ano. | Soma de `q_conv` das entradas. |
| `saidas` | `float` | Saídas do ano. | Soma de `q_conv` das saídas. |
| `estoque_final` | `float` | Estoque final declarado. | Soma de `__qtd_decl_final_audit__`. |
| `entradas_desacob` | `float` | Entradas desacobertadas do ano. | Soma de `entr_desac_anual`. |
| `saldo_final` | `float` | Saldo calculado ao final do ano. | Último `saldo_estoque_anual` do ano. |
| `saidas_calculadas` | `float` | Saída calculada pela equação de estoque. | `estoque_inicial + entradas + entradas_desacob - estoque_final`. |
| `saidas_desacob` | `float` | Saídas sem cobertura de entrada. | `max(estoque_final - saldo_final, 0)`. |
| `estoque_final_desacob` | `float` | Estoque calculado maior que declarado. | `max(saldo_final - estoque_final, 0)`. |
| `pme` | `float` | Preço médio de entrada anual. | Entradas válidas: valor / quantidade. |
| `pms` | `float` | Preço médio de saída anual. | Saídas válidas: valor / quantidade. |
| `ST` | `str` | Histórico/indicação de ST no ano. | Vem do cruzamento SITAFE. |
| `aliq_interna` | `float` | Alíquota interna aplicável. | Usada para ICMS. |
| `ICMS_saidas_desac` | `float` | ICMS sobre saídas desacobertadas. | Pode ser zerado por ST. |
| `ICMS_estoque_desac` | `float` | ICMS sobre estoque final desacobertado. | Não é zerado automaticamente por ST. |

### Fórmulas

Quantitativos:

```text
estoque_inicial = soma(q_conv onde Tipo_operacao = '0 - ESTOQUE INICIAL')
entradas = soma(q_conv onde Tipo_operacao = '1 - ENTRADA')
saidas = soma(q_conv onde Tipo_operacao = '2 - SAIDAS')
estoque_final = soma(__qtd_decl_final_audit__ onde Tipo_operacao = '3 - ESTOQUE FINAL')
entradas_desacob = soma(entr_desac_anual)
saldo_final = último saldo_estoque_anual do ano
```

Equação de estoque:

```text
saidas_calculadas = estoque_inicial + entradas + entradas_desacob - estoque_final
```

Divergências:

```text
saidas_desacob = max(estoque_final - saldo_final, 0)
estoque_final_desacob = max(saldo_final - estoque_final, 0)
```

As duas divergências são mutuamente exclusivas:

```text
se saidas_desacob > 0:
    estoque_final_desacob = 0

se estoque_final_desacob > 0:
    saidas_desacob = 0
```

Preços médios:

```text
pme = soma(valor das entradas válidas) / soma(qtd das entradas válidas)
pms = soma(valor das saídas válidas) / soma(qtd das saídas válidas)
```

Base de ICMS para saídas desacobertadas:

```text
se pms > 0:
    base_saida = saidas_desacob * pms
senão:
    base_saida = saidas_desacob * pme * 1.30
```

Base de ICMS para estoque desacobertado:

```text
se pms > 0:
    base_estoque = estoque_final_desacob * pms
senão:
    base_estoque = estoque_final_desacob * pme * 1.30
```

ICMS:

```text
ICMS_saidas_desac = base_saida * (aliq_interna / 100)
ICMS_estoque_desac = base_estoque * (aliq_interna / 100)
```

Regra de ST:

```text
se há ST vigente no ano:
    ICMS_saidas_desac = 0
```

---

## 3. `aba_periodos_<cnpj>.parquet` / `calculos_periodos`

### Papel

Recorta a movimentação por períodos de inventário ou janelas definidas. A documentação técnica atual cita essa tabela, mas o schema detalhado ainda precisa ser consolidado em `docs/tabelas/calculos_periodos.md`.

### Relações esperadas

| Origem | Destino | Campos | Como interpretar |
|---|---|---|---|
| `movimentacao_estoque` | `calculos_periodos` | `id_agrupado`, data, saldo, quantidades | Recorta movimentos dentro de uma janela. |
| `calculos_periodos` | relatórios/GUI | produto + período | Base para aba por período. |

### Campos esperados

> Atenção: os campos abaixo são o contrato esperado/conceitual. Validar com o Parquet real do CNPJ e consolidar no documento técnico canônico.

| Campo | Tipo esperado | Explicação | Relação/Fórmula |
|---|---|---|---|
| `id_agregado` ou `id_agrupado` | `str` | Produto agrupado. | Liga período ao produto. |
| `periodo_inicio` | `date` | Data inicial do recorte. | Filtro inferior da janela. |
| `periodo_fim` | `date` | Data final do recorte. | Filtro superior da janela. |
| `entradas` | `float` | Entradas no período. | Soma de entradas filtradas pela janela. |
| `saidas` | `float` | Saídas no período. | Soma de saídas filtradas pela janela. |
| `saldo_inicio` | `float` | Saldo no início do período. | Último saldo antes ou no início da janela. |
| `saldo_fim` | `float` | Saldo no fim do período. | Último saldo dentro da janela. |
| `entradas_desacob` | `float` | Entradas desacobertadas no período. | Soma de `entr_desac_anual` no recorte. |
| `estoque_inicial_periodo` | `float` | Estoque inicial do recorte, quando aplicável. | Pode vir de inventário ou saldo anterior. |
| `estoque_final_periodo` | `float` | Estoque final declarado/observado, quando aplicável. | Pode vir de inventário. |

### Fórmulas conceituais

Filtro do período:

```text
movimentos_periodo = movimentos onde periodo_inicio <= data_efetiva <= periodo_fim
```

Entradas e saídas:

```text
entradas = soma(q_conv onde Tipo_operacao = '1 - ENTRADA' dentro do período)
saidas = soma(q_conv onde Tipo_operacao = '2 - SAIDAS' dentro do período)
```

Saldo:

```text
saldo_fim = saldo_inicio + entradas - saidas + ajustes_do_periodo
```

Divergência conceitual:

```text
divergencia_periodo = estoque_final_periodo - saldo_fim
```

---

## Relação resumida das tabelas analíticas

```text
movimentacao_estoque.id_agrupado
  -> calculos_mensais.id_agregado
  -> relatórios mensais
```

```text
movimentacao_estoque.id_agrupado
  -> calculos_anuais.id_agregado
  -> relatórios anuais
```

```text
movimentacao_estoque.data_efetiva
  -> ano / mes / período
```

## Campos críticos

| Campo | Motivo |
|---|---|
| `id_agregado` | Preserva vínculo com `id_agrupado` nas saídas analíticas. |
| `ano`, `mes`, `periodo_inicio`, `periodo_fim` | Definem a janela fiscal da análise. |
| `estoque_inicial`, `entradas`, `saidas`, `estoque_final` | Equação física principal. |
| `saldo_final`, `saldo_mes` | Resultado calculado pelo estoque cronológico. |
| `entradas_desacob`, `saidas_desacob`, `estoque_final_desacob` | Métricas centrais de auditoria. |
| `pme`, `pms`, `pme_mes`, `pms_mes` | Bases de valorização. |
| `ST`, `MVA`, `MVA_ajustado`, `aliq_interna` | Determinam regra fiscal e ICMS. |
| `ICMS_entr_desacob`, `ICMS_saidas_desac`, `ICMS_estoque_desac` | Valores tributários apurados. |
