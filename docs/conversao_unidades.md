# Conversão de Unidades

Este documento consolida as regras do arquivo `fatores_conversao_<cnpj>.parquet`, gerado por `src/transformacao/fatores_conversao.py`.

## Objetivo

Padronizar quantidades e valores de um mesmo produto em uma unidade de referência (`unid_ref`), preservando ajustes manuais e permitindo uso consistente do fator nas etapas posteriores do pipeline.

## Fontes de entrada

O cálculo usa principalmente:

- `item_unidades_<cnpj>.parquet`
- `produtos_final_<cnpj>.parquet`

Campos relevantes:

- `descricao`, `unid`, `compras`, `vendas`, `qtd_compras`, `qtd_vendas`
- `id_agrupado`, `descricao_normalizada`, `descr_padrao`, `unid_ref_sugerida`

## Vínculo com o produto agrupado

A base `item_unidades` é normalizada e ligada a `produtos_final` por `descricao_normalizada`.

Se não houver vínculo entre as bases, a rotina salva uma saída vazia. Esse comportamento preserva o contrato do pipeline sem inventar fatores sem base mínima.

## Preço médio por unidade

Para cada `id_agrupado + unid`, o processo calcula:

- `compras_total`
- `qtd_compras_total`
- `vendas_total`
- `qtd_vendas_total`
- `qtd_mov_total`

Com isso, produz:

```text
preco_medio_compra = compras_total / qtd_compras_total
preco_medio_venda = vendas_total / qtd_vendas_total
```

## Escolha do preço-base

Prioridade:

1. `preco_medio_compra`
2. fallback para `preco_medio_venda`
3. ausência de preço utilizável

O parquet registra a origem:

- `COMPRA`
- `VENDA`
- `SEM_PRECO`

Além disso, o processo mantém logs auxiliares para casos sem preço médio de compra.

## Escolha da unidade de referência

Prioridade manual:

- se `unid_ref_sugerida` existir em `produtos_final`, ela vira a referência do produto.

Fallback automático:

- maior `qtd_mov_total`;
- em empate, maior `qtd_compras_total`.

Assim, a definição final é:

```text
unid_ref = unid_ref_manual ou unid_ref_auto
```

## Cálculo do fator

Depois de definida a `unid_ref`, o processo localiza o preço da unidade de referência dentro do próprio produto e calcula:

```text
fator = preco_medio_base / preco_unid_ref
```

Se `preco_unid_ref <= 0`, o fallback é `1.0`.

Interpretação:

- `fator > 1`: a unidade da linha representa múltiplas unidades de referência;
- `fator < 1`: a unidade da linha representa fração da unidade de referência.

## Preservação de ajustes manuais

Esta é uma regra crítica do projeto.

Quando o usuário ajusta `unid_ref` ou `fator` na aba Conversão:

- o parquet de fatores passa a refletir a edição manual;
- reprocessamentos devem tentar preservar essas escolhas em vez de descartá-las;
- a nova `unid_ref` recalcula os fatores do produto a partir do preço médio disponível da unidade escolhida.

Se a nova unidade não tiver preço médio utilizável, o produto pode receber fallback `fator = 1.0`.

## Uso posterior do fator

O fator é consumido principalmente por:

- `c170_xml`
- `c176_xml`
- `movimentacao_estoque`

Uso típico:

```text
qtd_padronizada = quantidade_original * fator
valor_unitario_padronizado = valor_unitario_original / fator
```

## Saídas geradas

Arquivos principais:

- `fatores_conversao_<cnpj>.parquet`
- `log_sem_preco_medio_compra_<cnpj>.parquet`
- `log_sem_preco_medio_compra_<cnpj>.json`
