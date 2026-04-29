# fatores_conversao

## Visao Geral

Tabela que calcula os fatores de conversao entre diferentes unidades de medida de um mesmo produto, padronizando quantidades e valores para uma unidade de referencia (`unid_ref`).

## Funcao de Geracao

```python
def calcular_fatores_conversao(cnpj: str, pasta_cnpj: Path | None = None) -> bool
```

Modulo de entrada: `src/transformacao/fatores_conversao.py`

Implementacao canonica: `src/transformacao/rastreabilidade_produtos/fatores_conversao.py`

## Dependencias

- **Depende de**: `item_unidades`, `produtos_final`
- **E dependencia de**: `c170_xml`, `c176_xml`, `movimentacao_estoque`

Dependencias canonicas auxiliares:

- `map_produto_agrupado_<cnpj>.parquet`
- `produtos_agrupados_<cnpj>.parquet`

## Fontes de Entrada

- `item_unidades_<cnpj>.parquet`
- `produtos_final_<cnpj>.parquet`
- `fatores_conversao_<cnpj>.parquet` existente, quando houver, para preservar overrides

## Objetivo

Calcular coeficientes que permitem converter quantidades de qualquer unidade de medida para uma unidade de referencia comum, possibilitando soma e comparacao de movimentacoes de produtos que usam unidades diferentes.

## Principais Colunas

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `id_agrupado` | str | Chave do produto agrupado |
| `id_produtos` | str | Alias historico preenchido com o proprio `id_agrupado` |
| `descr_padrao` | str | Descricao padrao do produto |
| `unid` | str | Unidade de medida da linha |
| `unid_ref` | str | Unidade de referencia do produto |
| `unid_ref_override` | str | Override manual preservado quando existir |
| `fator` | float | Fator efetivo de conversao |
| `fator_override` | float | Override manual preservado quando existir |
| `fator_manual` | bool | Indica se o fator efetivo veio de override manual |
| `unid_ref_manual` | bool | Indica se a unidade de referencia efetiva veio de override manual |
| `preco_medio` | float | Preco medio base da unidade |
| `origem_preco` | str | Origem do preco base: `COMPRA`, `VENDA` ou `SEM_PRECO` |
| `fator_origem` | str | Origem do fator: `manual`, `preco`, `fallback_sem_preco` ou `fallback_sem_preco_ref` |

`fator_conversao_origem` nao e coluna canonica desta tabela. Quando aparecer, trata-se de alias conceitual gerado em camadas auxiliares, como `src/metodologia_mds/service.py`.

## Regras de Processamento

### Vinculo do produto

Prioridade operacional:

1. chave fisica `id_item_unid -> id_agrupado`, quando o artefato de vinculo existir;
2. fallback por `descricao_normalizada`.

Se uma descricao for ambigua no vinculo por descricao, o runtime gera auditoria e descarta o match ambiguo.

### Escolha da unidade de referencia

Prioridade:

1. override manual preservado;
2. `unid_ref_sugerida` vinda de `produtos_final`;
3. unidade com maior movimentacao agregada.

### Escolha do preco base

Prioridade:

1. `preco_medio_compra`
2. fallback para `preco_medio_venda`
3. `SEM_PRECO`

### Calculo do fator

```text
fator = preco_medio_base / preco_unid_ref
```

Se nao houver preco utilizavel para a unidade de referencia, o fluxo cai em fallback controlado.

## Preservacao de Ajustes Manuais

Regra critica:

- overrides existentes em `fatores_conversao_<cnpj>.parquet` sao reconciliados com o agrupamento atual;
- `unid_ref` manual e `fator` manual devem sobreviver a reprocessamentos;
- colunas ausentes em artefatos antigos sao preenchidas de forma tolerante antes da reconciliacao.

## Uso Posterior do Fator

O fator e consumido por:

- `c170_xml`
- `c176_xml`
- `movimentacao_estoque`

Uso tipico:

```text
qtd_padronizada = quantidade_original * fator
valor_unitario_padronizado = valor_unitario_original / fator
```

## Saidas Geradas

Principal:

```text
dados/CNPJ/<cnpj>/analises/produtos/fatores_conversao_<cnpj>.parquet
```

Logs auxiliares:

```text
dados/CNPJ/<cnpj>/analises/produtos/log_sem_preco_medio_compra_<cnpj>.parquet
dados/CNPJ/<cnpj>/analises/produtos/log_sem_preco_medio_compra_<cnpj>.json
dados/CNPJ/<cnpj>/analises/produtos/log_reconciliacao_overrides_fatores_<cnpj>.parquet
dados/CNPJ/<cnpj>/analises/produtos/log_reconciliacao_overrides_fatores_<cnpj>.json
dados/CNPJ/<cnpj>/analises/produtos/audit_descricao_ambigua_fatores_<cnpj>.parquet
```

## Notas

- se nao houver vinculacao entre `item_unidades` e a camada canonica de agrupamento, a saida pode ser vazia;
- a camada viva usa `produtos_final` como entrada e `map_produto_agrupado` / `produtos_agrupados` como suporte canonico de vinculo;
- o log de itens sem preco de compra ajuda a identificar problemas de dados;
- fatores sao essenciais para somar movimentacoes de unidades diferentes.
