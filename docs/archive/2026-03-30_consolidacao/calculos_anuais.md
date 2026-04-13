# Relatorio Anual - Metodologia e Composicao

O Relatorio Anual (Aba Anual) consolida a auditoria por produto em cada ano civil, comparando o fluxo declarado de mercadorias com o saldo fisico calculado pelo sistema.

## Indicadores principais

- `estoque_inicial`: quantidade declarada em `01/01`
- `entradas`: soma anual das entradas
- `saidas`: soma anual das saidas declaradas
- `estoque_final`: quantidade declarada em `31/12`
- `entradas_desacobertadas`: omissoes de entrada detectadas no calculo cronologico anual
- `saldo_final`: saldo fisico calculado ao final do ano

## Regras de auditoria

- `saidas_calculadas = estoque_inicial + entradas + entradas_desacobertadas - estoque_final`
- `saidas_desacobertadas = max(0, estoque_final - saldo_final)`
- `estoque_final_desacoberto = max(0, saldo_final - estoque_final)`

Os campos `saidas_desacobertadas` e `estoque_final_desacoberto` sao mutuamente exclusivos por construcao: quando um deles e positivo, o outro permanece zero.

## Valores e ICMS

- `pme`: preco medio anual de entrada
- `pms`: preco medio anual de saida
- `ICMS_saidas_desacob = aliquota * saidas_desacobertadas * PMS`
- `ICMS_est_final_desacob = aliquota * estoque_final_desacoberto * PMS`

Quando `pms = 0`, o sistema aplica o fallback atual baseado em `pme * 1,30`.
