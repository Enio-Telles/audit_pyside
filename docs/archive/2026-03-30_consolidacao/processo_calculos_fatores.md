# Processo dos Cálculos de Fatores de Conversão

Este documento descreve como é gerado o arquivo `fatores_conversao_<cnpj>.parquet`, usado para padronizar quantidades e valores na `unid_ref` do produto.

## Script responsável

- `src/transformacao/fatores_conversao.py`

## Fontes de entrada

O processo usa duas bases:

- `item_unidades_<cnpj>.parquet`
- `produtos_final_<cnpj>.parquet`

Da base `item_unidades`, o cálculo consome principalmente:

- `descricao`
- `unid`
- `compras`
- `vendas`
- `qtd_compras`
- `qtd_vendas`

Da base `produtos_final`, o cálculo consome principalmente:

- `id_agrupado`
- `descricao_normalizada`
- `descricao_final`
- `descr_padrao`
- `unid_ref_sugerida`

## 1. Normalização e vínculo com o produto agrupado

Primeiro, a descrição de `item_unidades` é normalizada:

- remove acentos
- converte para maiúsculas
- remove espaços excedentes

Essa descrição normalizada é usada para ligar `item_unidades` a `produtos_final` por `descricao_normalizada`.

O resultado desse vínculo produz uma base intermediária com:

- `id_agrupado`
- `descr_padrao`
- `unid`
- totais de compra e venda por unidade

Se não houver vínculo entre as duas bases, o processo salva uma saída vazia.

## 2. Preço médio por unidade

Depois do vínculo, o cálculo agrupa por:

- `id_agrupado`
- `descr_padrao`
- `unid`

Para cada unidade do produto, calcula:

- `compras_total = soma(compras)`
- `qtd_compras_total = soma(qtd_compras)`
- `vendas_total = soma(vendas)`
- `qtd_vendas_total = soma(qtd_vendas)`
- `qtd_mov_total = soma(qtd_compras) + soma(qtd_vendas)`

Com isso, produz dois preços médios:

- `preco_medio_compra = compras_total / qtd_compras_total`, quando `qtd_compras_total > 0`
- `preco_medio_venda = vendas_total / qtd_vendas_total`, quando `qtd_vendas_total > 0`

## 3. Escolha do preço-base

Para cada `id_agrupado + unid`, o processo define:

- `preco_medio_base = preco_medio_compra`, se existir
- fallback para `preco_medio_venda`, se não houver preço de compra
- se não houver nenhum dos dois, o preço-base fica nulo

Também grava a origem desse preço:

- `origem_preco = "COMPRA"` quando veio de compra
- `origem_preco = "VENDA"` quando veio do fallback de venda
- `origem_preco = "SEM_PRECO"` quando não existe preço utilizável

## 4. Escolha da unidade de referência (`unid_ref`)

A `unid_ref` é definida em duas etapas.

### 4.1. Prioridade manual

Se `produtos_final` tiver `unid_ref_sugerida` preenchida para o `id_agrupado`, ela vira a `unid_ref_manual`.

### 4.2. Fallback automático

Se não houver unidade manual, o processo escolhe automaticamente a unidade com maior movimentação:

- ordena por `qtd_mov_total` decrescente
- em empate, usa `qtd_compras_total` decrescente
- pega a primeira unidade como `unid_ref_auto`

### 4.3. Definição final

A unidade final é:

- `unid_ref = unid_ref_manual`, quando existir
- senão, `unid_ref = unid_ref_auto`

## 5. Preço da unidade de referência

Depois de definida a `unid_ref`, o processo localiza, dentro do próprio produto, a linha cuja `unid` seja igual à `unid_ref`.

Para essa unidade, calcula:

- `preco_unid_ref = média(preco_medio_base)`

Na prática, esse é o preço usado como denominador para os fatores das demais unidades do mesmo produto.

## 6. Cálculo do fator

O fator é calculado para cada linha `id_agrupado + unid` pela fórmula:

- `fator = preco_medio_base / preco_unid_ref`, quando `preco_unid_ref > 0`
- `fator = 1.0`, caso contrário

### Interpretação

Esse fator é um coeficiente para converter a quantidade original da unidade da linha para a `unid_ref`.

Exemplo conceitual:

- se a unidade da linha custa 12 vezes a unidade de referência, então `fator = 12`
- se a unidade da linha custa metade da unidade de referência, então `fator = 0.5`

## 7. Estrutura da saída

O parquet final contém:

- `id_agrupado`
- `id_produtos`
- `descr_padrao`
- `unid`
- `unid_ref`
- `fator`
- `preco_medio`
- `origem_preco`

Onde:

- `id_produtos` é preenchido com o próprio `id_agrupado`
- `preco_medio` corresponde ao `preco_medio_base`

## 8. Logs auxiliares

O processo também gera um log com itens sem preço médio de compra:

- `log_sem_preco_medio_compra_<cnpj>.parquet`
- `log_sem_preco_medio_compra_<cnpj>.json`

Esse log separa:

- itens com fallback para preço de venda
- itens sem preço de compra e sem preço de venda

## 9. Uso posterior do fator

Depois de gerado, o `fator` é usado nas etapas seguintes para padronizar movimentações na `unid_ref`, especialmente em:

- `movimentacao_estoque.py`
- `c176_xml.py`
- `enriquecimento_fontes.py`

O uso típico é:

- `qtd_padronizada = quantidade_original * fator`

Ou, no caso de valores unitários:

- `valor_unitario_padronizado = valor_unitario_original / fator`

## 10. Observação sobre edições manuais na aba Conversão

Na interface, o usuário pode editar:

- `unid_ref`
- `fator`

Essas edições sobrescrevem diretamente o parquet de fatores.

Quando a `unid_ref` é alterada pelo painel da aba Conversão, a tela recalcula os fatores daquele produto usando:

- `fator = preco_medio / novo_preco_ref`, quando a nova unidade tem `preco_medio` válido
- `fator = 1.0` para o produto, quando não existe `preco_medio` válido para a nova unidade escolhida
