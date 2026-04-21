# Aba anual: contrato real de saida
<a id="mds-07-tabela-anual"></a>

Este documento descreve a saida real hoje gerada por `calculos_anuais`.

## Nome do artefato

O arquivo gerado atualmente e:

`dados/CNPJ/<cnpj>/analises/produtos/aba_anual_<cnpj>.parquet`

Portanto, `tabela_anual` e um nome conceitual; o artefato real persiste como `aba_anual`.

## Chaves e nomes expostos

Campos principais de identificacao:

- `ano`
- `id_agregado` - alias de apresentacao de `id_agrupado`
- `descr_padrao`
- `unid_ref`
- `ST`

## Quantidades e saldos

Campos principais:

- `estoque_inicial`
- `entradas`
- `saidas`
- `estoque_final`
- `saidas_calculadas`
- `saldo_final`
- `entradas_desacob`
- `saidas_desacob`
- `estoque_final_desacob`

Semantica importante:

- `estoque_final` vem da soma anual de `__qtd_decl_final_audit__`
- essa soma considera todos os estoques finais existentes no ano, nao apenas 31/12
- `saldo_final` vem do ultimo `saldo_estoque_anual` em `ordem_operacoes`

## Formula de divergencia

O runtime aplica:

`saidas_calculadas = estoque_inicial + entradas + entradas_desacob - estoque_final`

Depois:

- `saidas_desacob = max(estoque_final - saldo_final, 0)`
- `estoque_final_desacob = max(saldo_final - estoque_final, 0)`

## Medias

Campos principais:

- `pme`
- `pms`
- `aliq_interna`

A implementacao anual atual usa `q_conv_fisica` quando presente; se a coluna faltar, deriva a quantidade fisica a partir de `q_conv`, zerando estoque final.

## Observacao importante sobre filtros de media

A camada anual hoje exclui das medias principalmente:

- `dev_simples`
- `excluir_estoque`
- linhas com quantidade fisica menor ou igual a zero

Ela nao aplica hoje o mesmo conjunto ampliado de devolucao da camada mensal. Essa diferenca precisa permanecer explicita na documentacao para nao criar falsa sensacao de uniformidade.

## ICMS e ST

Campos principais:

- `ICMS_saidas_desac`
- `ICMS_estoque_desac`

Regra:

- se houver ST ativo no ano, `ICMS_saidas_desac` zera
- `ICMS_estoque_desac` continua calculado mesmo com ST

Base monetaria:

- se `pms > 0`, usa `pms` arredondado a 2 casas
- senao, usa `pme * 1.30`

## O que deixou de ser ambiguo

Esta revisao corrige quatro ambiguidades recorrentes:

1. `estoque_final` na saida anual nao e o saldo sistemico; e o estoque declarado consolidado
2. `saldo_final` e o ultimo saldo calculado do ano
3. `id_agregado` e alias de apresentacao, nao a chave interna do pipeline
4. os nomes literais da saida sao `estoque_final`, `saldo_final`, `ICMS_saidas_desac` e `ICMS_estoque_desac`, e nao suas variantes conceituais longas

## Regra pratica

Ao descrever a aba anual, use os nomes reais da saida:

- `id_agregado`
- `unid_ref`
- `estoque_final`
- `saldo_final`
- `saidas_desacob`
- `estoque_final_desacob`
- `ICMS_saidas_desac`
- `ICMS_estoque_desac`
