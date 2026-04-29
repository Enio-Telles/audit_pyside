# Conversao de unidades: regra atual, heuristica e rastreabilidade
<a id="mds-03-conversao-unidades"></a>

Este documento consolida a regra de conversao de unidades que o runtime realmente executa hoje.

## Objetivo

Levar unidades comerciais diferentes do mesmo `id_agrupado` para uma unidade de referencia comum, sem perder:

- override manual;
- origem do fator;
- capacidade de reconciliar reprocessamentos.

## Artefato canonico

O artefato principal e `fatores_conversao_<cnpj>.parquet`.

Colunas centrais hoje:

- `id_agrupado`
- `id_produtos`
- `descr_padrao`
- `unid`
- `unid_ref`
- `unid_ref_override`
- `fator`
- `fator_override`
- `fator_manual`
- `unid_ref_manual`
- `fator_origem`
- `preco_medio`
- `origem_preco`

## Escolha da unidade de referencia

Precedencia real no runtime:

1. `unid_ref_override`
2. `unid_ref_sugerida` vinda de `produtos_final`
3. `unid_ref_auto` derivada da unidade com maior movimentacao

O campo final persistido e `unid_ref`.

## Calculo do fator

Precedencia real:

1. `fator_override`
2. `fator_calc`
3. fallback

Hoje `fator_calc` ainda e baseado em razao de preco medio da unidade frente a `unid_ref`. Portanto, a implementacao atual ainda nao pode ser descrita como exclusivamente fisica.

## Origem do fator

Na tabela canonica de fatores, o runtime registra:

- `manual`
- `preco`
- `fallback_sem_preco`
- `fallback_sem_preco_ref`

Esse ponto e importante: o texto antigo apresentava `preco` como excecao residual, mas no codigo atual a heuristica por preco ainda e parte normal do fluxo.

## Overrides manuais

O pipeline preserva e reconcilia overrides manuais antigos sempre que possivel.

Isso inclui:

- reaproveitamento de `unid_ref_manual`
- reaproveitamento de `fator_manual`
- log de reconciliacao quando um agrupamento muda
- preservacao de overrides orfaos quando necessario para nao apagar ajuste manual sem rastreio

## Vinculo entre item_unidades e produto agregado

Na conversao de unidades, o vinculo do item ao grupo segue a mesma hierarquia de rastreabilidade do agrupamento:

1. `map_produto_agrupado`
2. fallback para `produtos_final`
3. descarte / auditoria quando a descricao for ambigua

Isso significa que `map_produto_agrupado_<cnpj>.parquet` tem prioridade real sobre uma `descricao_normalizada` ambigua em `produtos_final`.

## Alias conceituais usados pelo service MDS

O `MovimentacaoService.apply_conversion_factors()` expõe um envelope conceitual com:

- `fator_conversao`
- `fator_conversao_origem`
- `unidade_referencia`

Mas ele faz isso sem romper o contrato legado:

- `fator` continua existindo e e atualizado com o valor efetivo
- `unid_ref` continua sendo a coluna consumida pelo pipeline

Esse service e uma camada de compatibilidade e clareza; nao substitui a tabela canonica de fatores.

## Como documentar sem ambiguidade

Use a seguinte formulacao:

- objetivo metodologico: preferir equivalencia fisica e preservar override manual;
- estado real atual: ainda existe derivacao por preco medio com `fator_origem = preco`;
- contrato materializado: `fator` e `unid_ref`;
- aliases conceituais: `fator_conversao` e `unidade_referencia`.
