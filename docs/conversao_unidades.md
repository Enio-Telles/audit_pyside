# Conversão de Unidades

Este documento consolida as regras do arquivo `fatores_conversao_<cnpj>.parquet`.

## Objetivo

Padronizar quantidades e valores de um mesmo produto em uma unidade de referência (`unid_ref`), preservando ajustes manuais e permitindo uso consistente do fator nas etapas posteriores do pipeline.

## Fontes de entrada

O cálculo usa principalmente:

- `item_unidades_<cnpj>.parquet`
- `produtos_final_<cnpj>.parquet`
- `descricao_produtos_<cnpj>.parquet`
- `map_produto_agrupado_<cnpj>.parquet`

## Vínculo com o produto agrupado

O vínculo preferencial passa a ser:

1. `descricao_produtos` -> `map_produto_agrupado`
2. só depois fallback para `produtos_final`

Com isso, a conversão fica ancorada na camada canônica de agrupamento e não apenas em um join direto com `produtos_final`.

## Escolha da unidade de referência

Prioridade final:

```text
unid_ref = unid_ref_override ou unid_ref_sugerida ou unid_ref_auto
```

Onde:

- `unid_ref_override`: override humano preservado;
- `unid_ref_sugerida`: sugestão da camada de agrupamento;
- `unid_ref_auto`: escolha automática por volume movimentado.

## Cálculo do fator

Depois de definida a `unid_ref`, o processo calcula:

```text
fator = preco_medio_base / preco_unid_ref
```

Se `preco_unid_ref` estiver ausente ou inválido, o fallback é `1.0`.

## Campos de override

O parquet de saída deve preservar explicitamente:

- `unid_ref_override`
- `fator_override`

Esses campos armazenam a decisão manual original, enquanto `unid_ref` e `fator` refletem o valor efetivo aplicado após as regras de precedência.

## Classificação do fator

Além do valor numérico `fator`, o arquivo inclui `fator_origem` com os seguintes valores:

- `manual`
- `preco`
- `fallback_sem_preco`
- `fallback_sem_preco_ref`

## Reconciliação após reprocessamento

Em reprocessamentos:

- overrides manuais devem ser preservados;
- remapeamentos automáticos só podem ocorrer quando a correspondência com o agrupamento atual for única;
- casos ambíguos devem ser descartados com log de auditoria.

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
