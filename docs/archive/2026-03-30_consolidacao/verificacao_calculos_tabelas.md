# Verificacao Dos Calculos Das Tabelas (mov_estoque, mensal e anual)

Data da verificacao: 26/03/2026  
CNPJ usado na validacao: `37671507000187`

## 1. Escopo verificado

Foram verificados os calculos das tabelas:

- `mov_estoque_<cnpj>.parquet`
- `aba_mensal_<cnpj>.parquet`
- `aba_anual_<cnpj>.parquet`

Com foco em:

- saldo e custo medio anual na `mov_estoque`
- ICMS mensal de entradas desacobertadas
- ICMS anual de saidas desacobertadas e estoque final desacobertado
- coerencia dos totais exibidos em `Produtos selecionados`

## 2. Formulas em uso no codigo

## 2.1. `mov_estoque`

- `q_conv = Qtd * fator`
- se `mov_rep = true`, `q_conv = 0`
- se `excluir_estoque = true`, `q_conv = 0`
- se `infprot_cstat` for diferente de `100` e `150`, `q_conv = 0`
- `saldo_estoque_anual`: acumulado cronologico por produto/ano
- `entr_desac_anual`: quando saida gera saldo negativo, registra a diferenca e zera saldo
- `custo_medio_anual`: custo medio movel por saldo financeiro/saldo fisico

Regra atual de devolucao:

- devolucao nao altera `custo_medio_anual`
- devolucao de venda retorna quantidade pelo custo medio vigente (nao pelo `preco_item` da devolucao)

## 2.2. `aba_mensal`

- `ICMS_entr_desacob` so e calculado quando ha ST no mes (`__tem_st_mes__ = true`) e `entradas_desacob > 0`
- se `pms_mes > 0`:
  - `ICMS_entr_desacob = pms_mes * entradas_desacob * (aliq_mes/100)`
- senao:
  - `ICMS_entr_desacob = pme_mes * entradas_desacob * (aliq_mes/100) * MVA_mes`
- `MVA`: `it_pc_mva` quando ha ST no mes, nulo fora de ST
- `MVA_ajustado`: so quando `it_in_mva_ajustado = 'S'`

## 2.3. `aba_anual`

- `saidas_calculadas = estoque_inicial + entradas + entradas_desacob - estoque_final`
- `ICMS_saidas_desac`:
  - se ano com ST (`__tem_st_ano__ = true`), forcado para `0`
  - senao:
    - base saida = `saidas_desacob * pms` (ou `saidas_desacob * pme * 1.30` se `pms = 0`)
    - `ICMS_saidas_desac = base_saida * (aliq_interna/100)`
- `ICMS_estoque_desac` (sem bloqueio por ST):
  - base estoque = `estoque_final_desacob * pms` (ou `estoque_final_desacob * pme * 1.30` se `pms = 0`)
  - `ICMS_estoque_desac = base_estoque * (aliq_interna/100)`

## 3. Resultados da verificacao (dados reais)

Volumes:

- `mov_estoque`: `9.607` linhas
- `mensal`: `4.873` linhas
- `anual`: `1.942` linhas

Consistencia encontrada:

1. `saidas_calculadas` anual bate com a formula
- diferenca maxima: `0.0001`
- linhas com diferenca > `0.01`: `0`

2. `mov_rep` neutralizando `q_conv`
- linhas com `mov_rep = true`: `2.986`
- linhas com `mov_rep = true` e `q_conv != 0`: `0`

3. sanidade da `mov_estoque`
- `entr_desac_anual` negativa: `0` linhas
- `custo_medio_anual` negativo: `0` linhas

4. totais de ICMS (base do consolidado):
- soma mensal `ICMS_entr_desacob`: `5.205.836,04`
- soma anual `ICMS_saidas_desac`: `2.605,92`
- soma anual `ICMS_estoque_desac`: `2.316.919.079,69`

## 4. Sobre os valores muito altos em `total_ICMS_estoque_desac`

Os valores altos vistos na tela (ex.: `id_agrupado_14`) sao matematicamente coerentes com a formula atual porque:

- `ICMS_estoque_desac` depende de `estoque_final_desacob * pms * aliquota`
- em alguns grupos o `pms` esta muito alto (media na faixa de dezenas de milhares)
- isso amplia fortemente a base de calculo anual do estoque

Exemplo observado no dataset:

- `id_agrupado_14`: `tot_icms_estoque = 1.980.509.558,48`
- `sum_estoque_final_desacob = 58.163`
- `pms_medio = 32.035,91875`

## 5. Observacao sobre pequenas diferencas de centavos

Ao recomputar `ICMS_estoque_desac` diretamente do parquet final, aparecem poucas diferencas pequenas (ordem de centavos/unidades) porque o pipeline arredonda colunas intermediarias e finais em etapas diferentes.

Essas diferencas nao indicam quebra de formula; refletem efeito de arredondamento no valor salvo.

## 6. Conclusao

Para o CNPJ validado, os calculos das tabelas estao coerentes com as regras implementadas no codigo atual.  
Os valores altos em `total_ICMS_estoque_desac` decorrem principalmente da combinacao de `estoque_final_desacob` com `pms` elevado e aliquota interna, e nao de erro aritmetico isolado.
