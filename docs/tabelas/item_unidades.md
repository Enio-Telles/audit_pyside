# item_unidades

## Visão Geral

Tabela que detalha os itens dos documentos fiscais por unidade de medida, agregando informações de compras e vendas para cada combinação de produto e unidade.

## Função de Geração

```python
def gerar_item_unidades(cnpj: str, pasta_cnpj: Path | None = None) -> bool
```

Módulo: `src/transformacao/item_unidades.py`

## Dependências

- **Depende de**: `tb_documentos`
- **É dependência de**: `itens`

## Fontes de Entrada

- `tb_documentos_<cnpj>.parquet`

## Objetivo

Desdobrar os documentos fiscais em seus itens individuais, capturando informações detalhadas de compras e vendas por unidade de medida. Esta tabela é essencial para o cálculo posterior dos fatores de conversão entre unidades.

## Principais Colunas

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `descricao` | str | Descrição original do produto |
| `descricao_normalizada` | str | Descrição normalizada (sem acentos, maiúsculas) |
| `unid` | str | Unidade de medida do item (UN, CX, KG, etc.) |
| `compras` | float | Valor total das compras do item |
| `vendas` | float | Valor total das vendas do item |
| `qtd_compras` | float | Quantidade total comprada do item |
| `qtd_vendas` | float | Quantidade total vendida do item |
| `ncm` | str | Código NCM do produto |
| `cest` | str | Código CEST do produto |

## Regras de Processamento

- Normaliza a descrição do produto removendo acentos e convertendo para maiúsculas
- Agrega valores e quantidades por combinação de descrição + unidade
- Separa compras e vendas para cálculo de preços médios posteriores
- Preserva classificações fiscais (NCM, CEST) de cada item

## Saída Gerada

```
dados/CNPJ/<cnpj>/analises/produtos/item_unidades_<cnpj>.parquet
```

## Notas

- Esta tabela é fundamental para o cálculo de fatores de conversão
- A normalização de descrições permite o agrupamento de produtos equivalentes
- Cada linha representa um produto em uma unidade de medida específica
