# descricao_produtos

## Visão Geral

Tabela que padroniza e consolida as descrições dos produtos, preparando a base para o agrupamento e identificação de produtos equivalentes.

## Função de Geração

```python
def gerar_descricao_produtos(cnpj: str, pasta_cnpj: Path | None = None) -> bool
```

Módulo: `src/transformacao/descricao_produtos.py`

## Dependências

- **Depende de**: `itens`
- **É dependência de**: `produtos_final`

## Fontes de Entrada

- `itens_<cnpj>.parquet`

## Objetivo

Normalizar e consolidar descrições de produtos para permitir o agrupamento de itens equivalentes. Esta etapa é crucial para identificar produtos que são o mesmo item fiscal mas possuem descrições ligeiramente diferentes entre documentos ou emitentes.

## Principais Colunas

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `descricao_original` | str | Descrição original do produto |
| `descricao_normalizada` | str | Descrição normalizada (sem acentos, maiúsculas, sem espaços extras) |
| `ncm` | str | Código NCM associado |
| `cest` | str | Código CEST associado |
| `frequencia` | int | Número de ocorrências da descrição |
| `fontes` | list | Lista de fontes onde a descrição aparece |

## Regras de Processamento

- Remove acentos e caracteres especiais das descrições
- Converte para maiúsculas e remove espaços excedentes
- Agrupa descrições equivalentes por normalização textual
- Conta frequências para identificar descrições mais representativas
- Preserva classificações fiscais associadas a cada descrição

## Saída Gerada

```
dados/CNPJ/<cnpj>/analises/produtos/descricao_produtos_<cnpj>.parquet
```

## Notas

- Esta tabela é a base para o algoritmo de agrupamento de produtos (MDM)
- A qualidade da normalização impacta diretamente a precisão do agrupamento
- Descrições muito divergentes podem indicar produtos diferentes ou problemas de cadastro
