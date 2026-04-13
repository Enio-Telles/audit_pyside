# calculos_mensais

## Visão Geral

Tabela que resume a movimentação de estoque por produto e mês, reutilizando os resultados já materializados na `mov_estoque` sem recalcular o saldo cronológico.

## Função de Geração

```python
def gerar_calculos_mensais(cnpj: str, pasta_cnpj: Path | None = None) -> bool
```

Módulo: `src/transformacao/calculos_mensais.py` (wrapper)  
Implementação: `src/transformacao/calculos_mensais_pkg/calculos_mensais.py`

## Dependências

- **Depende de**: `movimentacao_estoque`
- **É dependência de**: nenhuma (tabela de saída analítica)

## Fontes de Entrada

- `mov_estoque_<cnpj>.parquet`

## Objetivo

Agregação mensal da movimentação detalhada, calculando:

- Totais de entradas e saídas por mês
- Preço médio de entrada (PME) e saída (PMS) do mês
- ICMS sobre entradas desacobertadas
- Saldo e valor de estoque no fim do mês
- Indicadores de ST e MVA

## Principais Colunas

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id_agregado` | str | Chave do produto agrupado (renomeado de `id_agrupado`) |
| `ano` | int | Ano civil |
| `mes` | int | Mês (1-12) |
| `descr_padrao` | str | Descrição padrão do produto |
| `unid_ref` | str | Unidade de referência |
| `valor_entradas` | float | Soma de `preco_item` das entradas |
| `qtd_entradas` | float | Soma de `q_conv` das entradas |
| `valor_saidas` | float | Soma de `preco_item` das saídas |
| `qtd_saidas` | float | Soma de `q_conv` das saídas |
| `pme_mes` | float | Preço médio de entrada do mês |
| `pms_mes` | float | Preço médio de saída do mês |
| `entradas_desacob` | float | Soma mensal de `entr_desac_anual` |
| `ICMS_entr_desacob` | float | ICMS sobre entradas desacobertadas |
| `saldo_mes` | float | Último `saldo_estoque_anual` do mês |
| `custo_medio_mes` | float | Último `custo_medio_anual` do mês |
| `valor_estoque` | float | `saldo_mes * custo_medio_mes` |
| `ST` | str | Histórico de períodos de ST do mês |
| `MVA` | float | MVA original |
| `MVA_ajustado` | float | MVA ajustado (6 casas decimais) |

## Regras de Processamento

### Chave de Agregação

Agregação por:

- `id_agrupado`
- `ano`
- `mes`

Data efetiva do mês:

- `Dt_e_s`, quando existir
- Senão `Dt_doc`

### Totais Físicos

**Entradas:**
```
valor_entradas = soma(preco_item) das linhas "1 - ENTRADA"
qtd_entradas = soma(q_conv) das linhas "1 - ENTRADA"
```

**Saídas:**
```
valor_saidas = soma(|preco_item|) das linhas "2 - SAIDAS"
qtd_saidas = soma(|q_conv|) das linhas "2 - SAIDAS"
```

**Entradas desacobertadas:**
```
entradas_desacob = soma mensal de entr_desac_anual
```

### Médias do Mês

Excluem-se movimentos inválidos:

- Devoluções: `dev_simples`, `dev_venda`, `dev_compra`, `dev_ent_simples` ou `finnfe = 4`
- Linhas com `excluir_estoque = true`
- Linhas com `q_conv <= 0`

**Fórmulas:**
```
pme_mes = soma(valor das entradas válidas) / soma(qtd das entradas válidas)
pms_mes = soma(valor das saídas válidas) / soma(qtd das saídas válidas)
```

### Saldo e Valor de Estoque

Aproveita o fechamento cronológico já calculado:

```
saldo_mes = último saldo_estoque_anual do mês
custo_medio_mes = último custo_medio_anual do mês
valor_estoque = saldo_mes * custo_medio_mes
```

### ST Mensal e ICMS

Cruza `co_sefin_agr` com `sitafe_produto_sefin_aux.parquet` e mantém períodos cuja vigência intersecta o mês.

**Campos:**
- `ST`: histórico textual dos períodos de ST
- `__tem_st_mes__`: flag interna
- `MVA`: `it_pc_mva` da última movimentação válida (apenas quando há ST)
- `MVA_ajustado`: preenchido quando `it_in_mva_ajustado = 'S'`

**ICMS sobre entradas desacobertadas:**

Calculado apenas quando há ST no mês e `entradas_desacob > 0`:

```
se pms_mes > 0:
    ICMS_entr_desacob = pms_mes * entradas_desacob * (aliq_mes / 100)
senão:
    ICMS_entr_desacob = pme_mes * entradas_desacob * (aliq_mes / 100) * MVA_efetivo
```

Onde `MVA_efetivo`:

- `it_pc_mva / 100`, quando `it_in_mva_ajustado = 'N'`
- `[((1 + MVA_orig) * (1 - ALQ_inter)) / (1 - ALQ_interna)] - 1`, quando `it_in_mva_ajustado = 'S'`

### Arredondamento

- Quantidades e saldos: 4 casas decimais
- Valores monetários: 2 casas decimais
- `MVA_ajustado`: 6 casas decimais

## Saída Gerada

```
dados/CNPJ/<cnpj>/analises/produtos/aba_mensal_<cnpj>.parquet
```

## Notas

- Não recalcula saldo cronológico (reaproveita da `mov_estoque`)
- Essencial para relatórios mensais e apurações tributárias
- O ICMS de entradas desacobertadas considera MVA ajustada quando aplicável
- `id_agregado` é o nome de saída para `id_agrupado`
