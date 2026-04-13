# Plano de Implementacao: Aba Anual de Movimentacao de Estoque

## 1. Objetivo

Criar uma agregacao anual ("aba anual") com base nos dados da movimentacao de estoque (`mov_estoque_<cnpj>.parquet`), consolidando saldos fisicos e financeiros, e calculando as repercussoes de ICMS sobre omissoes e estoques desacobertados.

## 2. Estrutura de Campos e Logica de Calculo

Os dados sao agrupados por **Ano** e **ID Agregado**.

### Campos de Identificacao e Movimentacao Fisica

| Campo | Descricao | Logica / Origem |
| :--- | :--- | :--- |
| **ano** | Ano de referencia | Calculado a partir da data de movimento. |
| **id_agregado** | ID do Produto | Identificador unico do grupo de produtos. |
| **descr_padrao** | Descricao | Descricao padronizada do item. |
| **ST** | Historico ST anual | Lista dos periodos de vigencia de `it_in_st` no ano, conforme `sitafe_produto_sefin_aux.parquet`, limitada ao intervalo anual analisado. |
| **estoque_inicial** | Saldo Inicial | Soma de `q_conv` onde Tipo = 0. |
| **entradas** | Entradas Totais | Soma de `q_conv` onde Tipo = 1. |
| **saidas** | Saidas Totais | Soma de `q_conv` onde Tipo = 2. |
| **estoque_final** | Estoque Inv. | Soma de `q_conv` onde Tipo = 3 (Bloco H). |
| **saidas_calculadas** | Saida Teorica | `estoque_inicial + entradas - estoque_final`. |
| **saldo_final** | Saldo Fisico | Saldo acumulado cronologicamente no sistema. |

### Campos de Omissao e Desacobertados

| Campo | Descricao | Logica / Formula |
| :--- | :--- | :--- |
| **entradas_desacob** | Omissao de Entrada | Soma das quantidades de entradas geradas para evitar saldo negativo. |
| **saidas_desacob** | Omissao de Saida | `max(0, saldo_final - estoque_final)`. |
| **estoque_final_desacob** | Estoque Ajustado | Corresponde ao `saldo_final` sistematico. |

### Logica de Preco Medio (PME / PMS)

| Campo | Descricao | Logica |
| :--- | :--- | :--- |
| **pme** | Preco Medio Entrada | `Soma(valor_item) / Soma(q_conv)` para entradas (Tipo 1), excluindo devolucoes simples e marcados para exclusao. |
| **pms** | Preco Medio Saida | `Soma(valor_item) / Soma(q_conv)` para saidas (Tipo 2), excluindo devolucoes simples e marcados para exclusao. |

## 3. Detalhamento dos Calculos de ICMS

> Os calculos de ICMS sobre saidas e estoque final desacobertado dependem da vigencia anual do status de Substituicao Tributaria (ST) em `sitafe_produto_sefin_aux.parquet`, usando `co_sefin_agr` / `it_co_sefin` e a sobreposicao do ano de analise com `it_da_inicio` e `it_da_final`.

### ICMS sobre Omissao de Saida (`ICMS_saidas_desac`)

Este campo calcula o debito estimado sobre as mercadorias que "sairam" do estabelecimento sem documentacao fiscal.

**Formula base:**

`ICMS_saidas_desac = Base_saidas * (aliq_interna / 100)`

Onde:

* `Base_saidas = saidas_desacob * pms`, quando `pms > 0`
* `Base_saidas = saidas_desacob * pme * 1,30`, quando `pms = 0`

Regra:

* Se existir qualquer vigencia anual com `it_in_st = 'S'`, o valor nao e calculado e fica zerado.
* Se nao houver ST no ano, calcula-se usando a `aliq_interna` vigente no registro SITAFE aplicavel ao ano; na falta dela, usa-se o fallback da movimentacao.

### ICMS sobre Estoque Final Desacobertado (`ICMS_estoque_desac`)

Representa o valor do ICMS correspondente ao estoque final que o sistema aponta como existente, mas que excede o inventario declarado.

**Formula base:**

`ICMS_estoque_desac = Base_estoque * (aliq_interna / 100)`

Onde:

* `Base_estoque = estoque_final_desacob * pms`, quando `pms > 0`
* `Base_estoque = estoque_final_desacob * pme * 1,30`, quando `pms = 0`

Regra:

* Segue a mesma regra da omissao de saida; se houver ST vigente no ano (`it_in_st = 'S'`), o valor e zerado.

### ICMS sobre Omissao de Entrada

Nao foi alterado por esta revisao. A regra acima se aplica apenas a `ICMS_saidas_desac` e `ICMS_estoque_desac`.

## 4. Fonte da Regra de ST

Para cada `co_sefin_agr` e `ano`:

* cruza-se a `aba_anual` com `sitafe_produto_sefin_aux.parquet`
* considera-se apenas registros cuja vigencia intersecta o ano analisado
* o campo `ST` passa a mostrar os periodos anuais efetivos
* basta haver um periodo com `it_in_st = 'S'` para zerar os calculos de saida e estoque desacobertado

## 5. Fontes de Dados e Execucao

* **Script**: `src/transformacao/calculos_anuais.py`
* **Origem principal**: `mov_estoque_<cnpj>.parquet`
* **Referencia fiscal**: `sitafe_produto_sefin_aux.parquet`
* **Saida**: `aba_anual_<cnpj>.parquet` em `analises/produtos`

## 6. Plano de Validacao

* **Conferencia de saldo**: `estoque_inicial + entradas - saidas + entradas_desacob - saidas_desacob == estoque_final`
* **Validacao ST**: produtos com qualquer periodo anual `it_in_st = 'S'` devem ter `ICMS_saidas_desac == 0` e `ICMS_estoque_desac == 0`
* **Fallback sem PMS**: quando `pms = 0`, os calculos devem usar `pme * 1,30`
* **Arredondamento**: quantidades com 4 casas decimais e valores monetarios com 2 casas
