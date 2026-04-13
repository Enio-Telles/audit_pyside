# c176_xml

## Visão Geral

Tabela que enriquece os registros C176 do SPED (informações complementares do documento fiscal) com dados de XMLs e fatores de conversão, preparando-os para a movimentação de estoque.

## Função de Geração

```python
def gerar_c176_xml(cnpj: str, pasta_cnpj: Path | None = None) -> bool
```

Módulo: `src/transformacao/c176_xml.py`

## Dependências

- **Depende de**: `fatores_conversao`
- **É dependência de**: `movimentacao_estoque`

## Fontes de Entrada

- Registros C176 extraídos do SPED
- XMLs processados
- `fatores_conversao_<cnpj>.parquet`
- `fontes_produtos_<cnpj>.parquet`

## Objetivo

Processar registros C176 (informações de posse, estado de mercadorias, etc.) aplicando fatores de conversão e vinculando ao produto agrupado. Esta tabela complementa o C170 com informações adicionais do documento fiscal.

## Principais Colunas

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id_linha_origem` | str | Chave de rastreabilidade da linha original |
| `id_agrupado` | str | Chave do produto agrupado |
| `num_doc` | str | Número do documento fiscal |
| `num_item` | int | Número do item no documento |
| `descricao` | str | Descrição do produto |
| `qtd` | float | Quantidade original |
| `qtd_conv` | float | Quantidade convertida (`qtd * fator`) |
| `valor_unitario` | float | Valor unitário original |
| `valor_unitario_conv` | float | Valor unitário convertido |
| `unid` | str | Unidade original |
| `unid_ref` | str | Unidade de referência |
| `fator` | float | Fator de conversão aplicado |
| `fonte` | str | Origem do registro |

## Regras de Processamento

### Enriquecimento

Mesmo padrão do C170:

1. Linha original entra com `codigo_fonte`
2. Tabela ponte injeta `id_agrupado`
3. Tabela mestre injeta atributos padronizados
4. Tabela de fatores injeta `unid_ref` e `fator`

### Conversão de Unidades

```
qtd_conv = qtd * fator
valor_unitario_conv = valor_unitario / fator
```

### Neutralizações

Mesmas regras do C170:

- `mov_rep = true`
- `excluir_estoque = true`
- `infprot_cstat` inválido
- Base zero

## Saída Gerada

```
dados/CNPJ/<cnpj>/analises/produtos/c176_xml_<cnpj>.parquet
```

## Notas

- C176 complementa informações do C170 com dados adicionais do documento fiscal
- Participa da movimentação de estoque junto com C170
- Preserva rastreabilidade completa via `id_linha_origem`
- Usado em conjunto com `c170_xml` para compor entradas de estoque
