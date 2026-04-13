# movimentacao_estoque

## Visão Geral

Tabela cronológica e auditável do fluxo de mercadorias, consolidando C170, NFe, NFCe, inventário do Bloco H e linhas sintéticas. É a camada central de enriquecimento operacional do sistema.

## Função de Geração

```python
def gerar_movimentacao_estoque(cnpj: str, pasta_cnpj: Path | None = None) -> bool
```

Módulo: `src/transformacao/movimentacao_estoque.py` (wrapper)  
Implementação: `src/transformacao/movimentacao_estoque_pkg/movimentacao_estoque.py`

## Dependências

- **Depende de**: `c170_xml`, `c176_xml`
- **É dependência de**: `calculos_mensais`, `calculos_anuais`

## Fontes de Entrada

- `c170_xml_<cnpj>.parquet`
- `c176_xml_<cnpj>.parquet`
- Dados de NFe/NFCe processados
- Inventário Bloco H
- `fatores_conversao_<cnpj>.parquet`
- `produtos_final_<cnpj>.parquet`
- Classificações SEFIN (`sitafe_produto_sefin_aux.parquet`)

## Objetivo

Materializar o fluxo cronológico de estoque por produto e data, calculando:

- Saldo físico acumulado (`saldo_estoque_anual`)
- Entradas desacobertadas (`entr_desac_anual`)
- Custo médio móvel (`custo_medio_anual`)
- Classificações fiscais (ST, MVA, alíquotas)

## Principais Colunas

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id_agrupado` | str | Chave do produto agrupado |
| `fonte` | str | Origem: `c170`, `nfe`, `nfce`, `bloco_h`, `gerado` |
| `Tipo_operacao` | str | Tipo: `0 - ESTOQUE INICIAL`, `1 - ENTRADA`, `2 - SAIDAS`, `3 - ESTOQUE FINAL` |
| `Dt_e_s` / `Dt_doc` | date | Data da operação |
| `q_conv` | float | Quantidade convertida para `unid_ref` |
| `preco_item` | float | Valor total da linha |
| `Vl_item` | float | Valor unitário da linha |
| `saldo_estoque_anual` | float | Saldo físico acumulado |
| `entr_desac_anual` | float | Entradas desacobertadas (saídas sem saldo) |
| `custo_medio_anual` | float | Custo médio móvel |
| `__qtd_decl_final_audit__` | float | Quantidade declarada para auditoria (estoque final) |
| `ncm_padrao` | str | NCM padrão do produto |
| `cest_padrao` | str | CEST padrão do produto |
| `unid_ref` | str | Unidade de referência |
| `fator` | float | Fator de conversão |
| `co_sefin_final` | str | Código SEFIN final |
| `co_sefin_agr` | str | Código SEFIN agrupado |
| `it_pc_interna` | float | Alíquota interna |
| `it_in_st` | str | Indicador de ST |
| `it_pc_mva` | float | MVA original |
| `it_in_mva_ajustado` | str | Indicador de MVA ajustado |
| `it_pc_reducao` | float | Redução de base de cálculo |
| `it_in_reducao_credito` | str | Indicador de redução de crédito |

## Regras de Processamento

### Ordenação e Reinício Anual

Cálculo sequencial por `id_agrupado` e ano civil. Ordem lógica:

1. `0 - ESTOQUE INICIAL`
2. `1 - ENTRADA`
3. `2 - SAIDAS`
4. `3 - ESTOQUE FINAL`

Ao mudar o ano, reiniciam: saldo físico, saldo financeiro, custo médio, contador de entradas desacobertadas.

### Quantidade Convertida

```
q_conv = abs(Qtd) * abs(fator)
```

**Neutralizações** (`q_conv = 0`):

- `mov_rep = true`
- `excluir_estoque = true`
- `infprot_cstat` diferente de `100` ou `150`
- Base da linha igual a zero

### Saldo e Entradas Desacobertadas

- Entradas e estoque inicial somam no saldo
- Saídas baixam o saldo
- Estoque final apenas audita (não altera saldo)

Quando uma saída faria o saldo ficar negativo:

```
entr_desac_anual = abs(saldo_negativo)
saldo_estoque_anual = 0
```

### Estoque Final Auditado

Para linhas `3 - ESTOQUE FINAL`:

- `q_conv` permanece `0` (não impacta saldo)
- Quantidade declarada fica em `__qtd_decl_final_audit__`
- `saldo_estoque_anual` não muda
- `custo_medio_anual` não muda
- `entr_desac_anual` permanece `0`

### Custo Médio Anual

**Entradas válidas:**
- Somam quantidade e `preco_item` no saldo financeiro
- Recalculam `custo_medio_anual = saldo_financeiro / saldo_estoque_anual`

**Saídas válidas:**
- Baixam pelo custo médio vigente
- Não usam o valor da própria linha para formar nova média

**Estoque final:**
- Não altera saldo financeiro
- Não recalcula custo médio

### Classificação SITAFE

Cruza `co_sefin_agr` com `sitafe_produto_sefin_aux.parquet` e mantém vigências que intersectam o período. Usa a regra ativa mais recente caso não haja correspondência exata de data.

## Saída Gerada

```
dados/CNPJ/<cnpj>/analises/produtos/mov_estoque_<cnpj>.parquet
```

## Notas

- É a tabela mais importante para auditoria fiscal
- Preserva todas as colunas críticas para cruzamentos posteriores
- Base direta para as tabelas mensal e anual
- O custo médio é calculado cronologicamente, linha a linha
- Entradas desacobertadas indicam possíveis omissões de entrada
