# fatores_conversao

## Visão Geral

Tabela que calcula os fatores de conversão entre diferentes unidades de medida de um mesmo produto, padronizando quantidades e valores para uma unidade de referência (`unid_ref`).

## Função de Geração

```python
def calcular_fatores_conversao(cnpj: str, pasta_cnpj: Path | None = None) -> bool
```

Módulo: `src/transformacao/fatores_conversao.py`

## Dependências

- **Depende de**: `fontes_produtos`, `item_unidades`, `produtos_final`
- **É dependência de**: `c170_xml`, `c176_xml`, `movimentacao_estoque`

## Fontes de Entrada

- `item_unidades_<cnpj>.parquet`
- `produtos_final_<cnpj>.parquet`

## Objetivo

Calregar coeficientes que permitem converter quantidades de qualquer unidade de medida para uma unidade de referência comum, possibilitando soma e comparação de movimentações de produtos que usam unidades diferentes (ex: CX, UN, KG, LT).

## Principais Colunas

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id_agrupado` | str | Chave do produto agrupado |
| `id_produtos` | str | Identificador do produto (igual a `id_agrupado`) |
| `descr_padrao` | str | Descrição padrão do produto |
| `unid` | str | Unidade de medida da linha |
| `unid_ref` | str | Unidade de referência do produto |
| `fator` | float | Fator de conversão: `qtd_padronizada = qtd_original * fator` |
| `preco_medio` | float | Preço médio base do produto |
| `origem_preco` | str | Origem do preço: `COMPRA`, `VENDA` ou `SEM_PRECO` |
| `fator_origem` | str | Origem do fator de conversão: `manual` (override aplicado pelo usuário), `fallback` (sem preço de referência disponível), `preco` (fator calculado a partir do preço médio) |

## Regras de Processamento

### Preço Médio por Unidade

Para cada combinação `id_agrupado + unid`:

```
compras_total = soma(compras)
qtd_compras_total = soma(qtd_compras)
vendas_total = soma(vendas)
qtd_vendas_total = soma(qtd_vendas)
qtd_mov_total = qtd_compras_total + qtd_vendas_total

preco_medio_compra = compras_total / qtd_compras_total  (se qtd_compras_total > 0)
preco_medio_venda = vendas_total / qtd_vendas_total    (se qtd_vendas_total > 0)
```

### Escolha do Preço-Base

Prioridade:

1. `preco_medio_compra` (preferencial)
2. Fallback para `preco_medio_venda`
3. `SEM_PRECO` se nenhum estiver disponível

### Escolha da Unidade de Referência

**Prioridade manual:**
- Se `unid_ref_sugerida` existir em `produtos_final`, ela vira `unid_ref`

**Fallback automático:**
- Unidade com maior `qtd_mov_total`
- Em empate, maior `qtd_compras_total`

### Cálculo do Fator

```
preco_unid_ref = preço_medio_base da unidade de referência
fator = preco_medio_base / preco_unid_ref   (se preco_unid_ref > 0)
fator = 1.0                                  (caso contrário)
```

**Interpretação:**
- `fator > 1`: a unidade da linha representa múltiplas unidades de referência
- `fator < 1`: a unidade da linha representa fração da unidade de referência

## Preservação de Ajustes Manuais

**Regra crítica:** Quando o usuário ajusta `unid_ref` ou `fator` na interface:

- O parquet reflete a edição manual
- Reprocessamentos preservam essas escolhas
- Nova `unid_ref` recalcula fatores a partir do preço médio disponível
- Se a nova unidade não tiver preço utilizável, recebe `fator = 1.0`

## Uso Posterior do Fator

O fator é consumido por:

- **c170_xml**, **c176_xml**: padronizar quantidades e valores
- **movimentacao_estoque**: converter para unidade de referência

Uso típico:

```
qtd_padronizada = quantidade_original * fator
valor_unitario_padronizado = valor_unitario_original / fator
```

## Saídas Geradas

**Principal:**
```
dados/CNPJ/<cnpj>/analises/produtos/fatores_conversao_<cnpj>.parquet
```

**Logs auxiliares:**
```
dados/CNPJ/<cnpj>/analises/produtos/log_sem_preco_medio_compra_<cnpj>.parquet
dados/CNPJ/<cnpj>/analises/produtos/log_sem_preco_medio_compra_<cnpj>.json
```

## Notas

- Se não houver vínculo entre `item_unidades` e `produtos_final`, salva saída vazia
- O log de itens sem preço de compra ajuda a identificar problemas de dados
- Fatores são essenciais para somar movimentações de unidades diferentes
 - A tabela agora inclui a coluna `fator_origem`, que classifica como cada `fator` foi obtido: `manual`, `fallback` ou `preco`.
 - Se não houver vínculo entre `item_unidades` e `produtos_final`, salva saída vazia
 - O log de itens sem preço de compra ajuda a identificar problemas de dados
 - Fatores são essenciais para somar movimentações de unidades diferentes
