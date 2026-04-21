# Aba mensal: contrato real de saida
<a id="mds-06-tabela-mensal"></a>

Este documento descreve a saida real hoje gerada por `calculos_mensais`.

## Nome do artefato

O arquivo gerado atualmente e:

`dados/CNPJ/<cnpj>/analises/produtos/aba_mensal_<cnpj>.parquet`

Logo, `tabela_mensal` e um nome conceitual; a saida materializada chama-se `aba_mensal`.

## Chaves e nomes expostos

A agregacao e por:

- `ano`
- `mes`
- `id_agrupado`

Na saida final, o identificador e exposto como `id_agregado`.

Campos principais de identificacao:

- `ano`
- `mes`
- `id_agregado`
- `descr_padrao`
- `unids_mes`
- `unids_ref_mes`
- `ST`
- `it_in_st`

## Entradas, saidas e estoque

Campos principais:

- `valor_entradas`
- `qtd_entradas`
- `pme_mes`
- `valor_saidas`
- `qtd_saidas`
- `pms_mes`
- `saldo_mes`
- `custo_medio_mes`
- `valor_estoque`

Campos adicionais por periodo de inventario:

- `entradas_desacob_periodo`
- `ICMS_entr_desacob_periodo`
- `saldo_mes_periodo`
- `custo_medio_mes_periodo`
- `valor_estoque_periodo`

## Regra de quantidade

A aba mensal prioriza `q_conv_fisica`. Se essa coluna nao existir em parquet legado, ela faz fallback derivando a quantidade fisica a partir de `q_conv`, zerando estoque final.

Isso evita que a quantidade declarada do inventario contamine:

- `qtd_entradas`
- `qtd_saidas`
- `pme_mes`
- `pms_mes`

## Regra de medias

A aba mensal exclui das medias:

- `dev_simples`
- `dev_venda`
- `dev_compra`
- `dev_ent_simples`
- `finnfe = 4`
- `excluir_estoque`
- linhas com `q_conv_fisica <= 0`

Isso vale tanto para `pme_mes` quanto para `pms_mes`.

## ST e MVA

Campos fiscais principais:

- `MVA`
- `MVA_ajustado`
- `entradas_desacob`
- `ICMS_entr_desacob`

Regras:

- sem ST vigente no mes, `ICMS_entr_desacob = 0`
- com ST vigente, o ICMS usa `pms_mes` arredondado quando houver saida valida
- sem `pms_mes`, cai para `pme_mes * aliquota * MVA efetivo`

## O que mudou na documentacao

Os nomes abaixo nao correspondem ao contrato literal atual e foram removidos como se fossem colunas reais:

- `preco_medio_entradas_mes`
- `preco_medio_saidas_mes`
- `sujeito_a_st`
- `unidades_referencia_mes`

Os nomes reais hoje sao:

- `pme_mes`
- `pms_mes`
- `it_in_st`
- `unids_ref_mes`

## Regra pratica

Ao descrever a camada mensal, use:

- `id_agregado` para a saida final
- `qtd_entradas` / `qtd_saidas`
- `pme_mes` / `pms_mes`
- `entradas_desacob` / `ICMS_entr_desacob`
- `saldo_mes` / `custo_medio_mes`
