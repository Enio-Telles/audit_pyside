# Catálogo de Tabelas e Campos

Esta página é o mapa central das tabelas Parquet do `audit_pyside`. Ela explica a função de cada tabela, como as tabelas se relacionam e onde consultar o dicionário campo a campo com fórmulas.

## Fluxo principal do pipeline

```text
tb_documentos
  -> item_unidades
  -> itens
  -> descricao_produtos
  -> produtos_final
  -> fontes_produtos
  -> fatores_conversao
  -> c170_xml / c176_xml
  -> movimentacao_estoque
  -> calculos_mensais / calculos_anuais / calculos_periodos
```

## Páginas do catálogo

| Página | Tabelas cobertas |
|---|---|
| [Tabelas Base — Campos e Fórmulas](Tabelas-Base-Campos-e-Formulas) | `tb_documentos`, `item_unidades`, `itens`, `descricao_produtos` |
| [Tabelas de Agrupamento — Campos e Fórmulas](Tabelas-de-Agrupamento-Campos-e-Formulas) | `produtos_final`, `fontes_produtos`, `fatores_conversao` |
| [Tabelas de Estoque e Enriquecimento — Campos e Fórmulas](Tabelas-de-Estoque-e-Enriquecimento-Campos-e-Formulas) | `c170_xml`, `c176_xml`, `movimentacao_estoque` |
| [Tabelas Analíticas — Campos e Fórmulas](Tabelas-Analiticas-Campos-e-Formulas) | `calculos_mensais`, `calculos_anuais`, `calculos_periodos` |

## Relações principais entre tabelas

| Relação | Campo de ligação | Explicação |
|---|---|---|
| Documento -> item | `chave_acesso`, `num_doc`, `num_item` | Liga o documento fiscal aos itens que compõem o documento. |
| Item -> descrição | `descricao`, `descricao_normalizada`, `id_descricao` | Agrupa descrições equivalentes para preparar o agrupamento de produtos. |
| Descrição -> produto agrupado | `id_descricao`, `id_agrupado_base`, `id_agrupado` | Transforma descrições normalizadas em produto fiscal consolidado. |
| Fonte operacional -> produto agrupado | `codigo_fonte`, fallback por `descricao_normalizada` | Permite vincular linhas de SPED/XML/NF-e/NFC-e ao produto agrupado. |
| Produto agrupado -> fatores | `id_agrupado`, `unid`, `unid_ref` | Define a conversão de cada unidade para a unidade de referência. |
| C170/C176 -> estoque | `id_agrupado`, `qtd_conv`, `unid_ref`, `fator` | Alimenta a movimentação cronológica de estoque. |
| Estoque -> análises | `id_agrupado`, `ano`, `mes`, `periodo_inventario` | Gera resumos mensal, anual e por período. |

## Campos críticos do fio de auditoria

| Campo | Onde aparece | Por que é crítico |
|---|---|---|
| `id_linha_origem` | `itens`, `c170_xml`, `c176_xml`, `movimentacao_estoque` | Permite voltar da linha analítica para a linha de origem. |
| `codigo_fonte` | fontes operacionais e `fontes_produtos` | Chave preferencial para vincular uma fonte operacional ao produto agrupado. |
| `id_descricao` | `produtos_final` e mapas de agrupamento | Identifica a descrição normalizada usada no agrupamento. |
| `id_agrupado_base` | `produtos_final` | Guarda o agrupamento automático original antes de ajuste manual. |
| `id_agrupado` | camadas agrupadas, estoque e análises | Chave mestra do produto consolidado. |
| `id_agregado` | tabelas analíticas | Alias de saída para `id_agrupado`. |
| `q_conv` | estoque e análises | Quantidade convertida observada na linha. |
| `q_conv_fisica` | `movimentacao_estoque` | Quantidade convertida que altera o saldo físico. |
| `__qtd_decl_final_audit__` | `movimentacao_estoque` e análises | Quantidade declarada de estoque final usada apenas para auditoria. |

## Fórmulas globais

### Conversão de quantidade

```text
qtd_conv = qtd_original * fator
```

Em movimentação de estoque, a forma operacional usa valores absolutos para evitar sinal duplicado:

```text
q_conv = abs(Qtd) * abs(fator)
```

### Conversão de valor unitário

```text
valor_unitario_conv = valor_unitario_original / fator
```

### Quantidade física vs quantidade auditada

```text
q_conv       = quantidade convertida observada na linha
q_conv_fisica = quantidade convertida que movimenta o saldo físico
```

Para estoque final declarado:

```text
q_conv pode ficar preenchido para auditoria
q_conv_fisica = 0
__qtd_decl_final_audit__ = quantidade declarada no inventário
```

### Saída que deixaria saldo negativo

```text
saldo_projetado = saldo_estoque_anterior - qtd_saida

se saldo_projetado < 0:
    entr_desac = abs(saldo_projetado)
    saldo_estoque = 0
senão:
    entr_desac = 0
    saldo_estoque = saldo_projetado
```

### Custo médio móvel

```text
custo_medio = saldo_financeiro / saldo_estoque
```

Entradas válidas aumentam saldo físico e financeiro. Saídas válidas reduzem o estoque pelo custo médio vigente, sem recalcular a média pelo valor de venda da própria saída.

### Divergências anuais e por período

```text
saidas_calculadas = estoque_inicial + entradas + entradas_desacob - estoque_final
saidas_desacob = max(estoque_final - saldo_final, 0)
estoque_final_desacob = max(saldo_final - estoque_final, 0)
```

`saidas_desacob` e `estoque_final_desacob` são mutuamente exclusivos por construção: quando um é positivo, o outro deve ficar zerado.

### ICMS sobre divergências

Quando existe preço médio de saída:

```text
base = quantidade_desacoberta * pms
```

Quando não existe preço médio de saída:

```text
base = quantidade_desacoberta * pme * 1.30
```

Aplicação da alíquota:

```text
ICMS = base * (aliq_interna / 100)
```

Regra de substituição tributária nas saídas desacobertadas:

```text
se há ST vigente:
    ICMS_saidas_desac = 0
```

O ICMS sobre estoque final desacobertado não é zerado automaticamente pela existência de ST.

## Observação importante sobre schema real

As páginas abaixo documentam os campos canônicos e os campos já descritos nos documentos técnicos do repositório. Algumas fontes Parquet podem conter colunas adicionais herdadas da origem, colunas auxiliares de debug, flags internas ou campos temporários. Quando isso acontecer, o schema real deve ser validado diretamente no arquivo Parquet gerado para o CNPJ analisado.
