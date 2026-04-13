# Tabela Anual

Este documento consolida as regras da `aba_anual_<cnpj>.parquet`, gerada por `src/transformacao/calculos_anuais.py` e pela implementação em `src/transformacao/calculos_anuais_pkg/`.

## Papel da tabela

A tabela anual resume a auditoria por `id_agrupado` e ano civil, confrontando:

- estoque inicial;
- entradas e saídas declaradas;
- estoque final declarado;
- saldo final calculado pelo fluxo cronológico;
- reflexos de ICMS sobre saídas e estoque desacobertados.

Na saída, `id_agrupado` é exposto como `id_agregado`.

## Base de cálculo

A anual reaproveita a `mov_estoque`, principalmente:

- `q_conv`
- `entr_desac_anual`
- `saldo_estoque_anual`
- `preco_item`
- `Vl_item`
- `it_pc_interna`
- `co_sefin_agr`
- `descr_padrao`
- `unid_ref`
- `__qtd_decl_final_audit__`

## Quantitativos anuais

Agregações físicas:

- `estoque_inicial`: soma de `q_conv` das linhas `0 - ESTOQUE INICIAL` (qualquer data);
- `entradas`: soma de `q_conv` das linhas `1 - ENTRADA`;
- `saidas`: soma de `q_conv` das linhas `2 - SAIDAS`;
- `estoque_final`: soma de `__qtd_decl_final_audit__` das linhas `3 - ESTOQUE FINAL` (qualquer data);
- `entradas_desacob`: soma anual de `entr_desac_anual`;
- `saldo_final`: último `saldo_estoque_anual` do ano.

Importante:

- `3 - ESTOQUE FINAL` não cria `entradas_desacob`;
- ele só informa o inventário declarado para auditoria anual.

**Nota:** As restrições anteriores de data (01/01 para estoque inicial, 31/12 para estoque final) foram removidas. Agora todo estoque inicial/final do ano é capturado para auditoria.

## Fórmulas principais

```text
saidas_calculadas = estoque_inicial + entradas + entradas_desacob - estoque_final
saidas_desacob = max(estoque_final - saldo_final, 0)
estoque_final_desacob = max(saldo_final - estoque_final, 0)
```

`saidas_desacob` e `estoque_final_desacob` são mutuamente exclusivos por construção. Se um deles é positivo, o outro fica zerado.

## PME e PMS do ano

As médias anuais usam movimentos válidos, excluindo:

- devoluções identificadas por `dev_simples`;
- linhas com `excluir_estoque = true`;
- linhas com `q_conv <= 0`.

Fórmulas:

```text
pme = soma(valor das entradas válidas) / soma(qtd das entradas válidas)
pms = soma(valor das saídas válidas) / soma(qtd das saídas válidas)
```

O valor unitário da agregação usa `preco_item` e, na falta dele, `Vl_item`.

## Regra anual de ST

O módulo cruza `co_sefin_agr` com `sitafe_produto_sefin_aux.parquet` e mantém vigências que intersectam o ano analisado.

Campos relevantes:

- `ST`: histórico textual dos períodos anuais;
- `__tem_st_ano__`: flag interna;
- `aliq_interna`: prioridade para a alíquota da referência SEFIN, com fallback para a última alíquota da movimentação.

## ICMS anual

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

- se `__tem_st_ano__ = true`, `ICMS_saidas_desac = 0`;
- `ICMS_estoque_desac` não é zerado por ST.

## Arredondamento

- quantidades e saldos: 4 casas;
- `pme`, `pms`, `aliq_interna`, `ICMS_saidas_desac` e `ICMS_estoque_desac`: 2 casas.

## Saída gerada

```text
dados/CNPJ/<cnpj>/analises/produtos/aba_anual_<cnpj>.parquet
```
