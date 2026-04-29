# Aba de periodos: contrato real de saida
<a id="mds-05-tabela-periodos"></a>

Este documento descreve a saida real hoje gerada por `calculos_periodo`.

## Nome do artefato

O arquivo gerado atualmente e:

`dados/CNPJ/<cnpj>/analises/produtos/aba_periodos_<cnpj>.parquet`

Portanto, `tabela_periodos` e um nome conceitual; a saida real materializada chama-se `aba_periodos`.

## Chaves e nomes expostos

As principais chaves da saida sao:

- `cod_per` - alias de `periodo_inventario`
- `periodo_label`
- `id_agregado` - alias de apresentacao para `id_agrupado`
- `descr_padrao`
- `unid_ref`

Nao e correto documentar essa camada como se ela expusesse hoje:

- `codigo_periodo`
- `id_produto_agrupado`
- `unidade_referencia`

Esses nomes podem ser usados como descricao conceitual, mas nao como contrato literal da saida.

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

Semantica:

- `estoque_final` vem de `__qtd_decl_final_audit__`
- `saldo_final` vem do ultimo `saldo_estoque_periodo` no periodo
- `saidas_desacob` e `estoque_final_desacob` sao cenarios opostos

## Formula de divergencia

O runtime aplica:

`saidas_calculadas = estoque_inicial + entradas + entradas_desacob - estoque_final`

E depois:

- `saidas_desacob = max(estoque_final - saldo_final, 0)`
- `estoque_final_desacob = max(saldo_final - estoque_final, 0)`

## Medias e ICMS

Campos principais:

- `pme`
- `pms`
- `aliq_interna`
- `ST`
- `ICMS_saidas_desac`
- `ICMS_estoque_desac`

Base monetaria:

- se `pms > 0`, usa `pms` arredondado a 2 casas
- senao, usa `pme * 1.30`

Se houver ST vigente no periodo:

- `ICMS_saidas_desac` e zerado
- `ICMS_estoque_desac` permanece calculado

## Delimitacao do periodo

`periodo_label` e montado a partir da menor e da maior data efetiva (`Dt_e_s` ou `Dt_doc`) presentes no grupo `id_agrupado + periodo_inventario`.

Ou seja: a aba nao usa "periodo fiscal abstrato"; ela usa o recorte efetivamente materializado em `mov_estoque`.

## Observacao importante sobre medias

A implementacao atual de periodos filtra medias com foco em:

- `dev_simples`
- `excluir_estoque`

Isso significa que a camada de periodos hoje nao reaplica todo o conjunto de flags de devolucao usado na camada mensal. Essa diferenca precisa permanecer documentada como estado real, e qualquer unificacao futura deve ser tratada como mudanca funcional.

## Regra pratica

Ao documentar `aba_periodos`, use os nomes reais da saida:

- `cod_per`
- `id_agregado`
- `unid_ref`
- `estoque_final`
- `saldo_final`
- `ICMS_saidas_desac`
- `ICMS_estoque_desac`
