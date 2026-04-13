# c170_xml

## Visão Geral

Tabela que enriquece os registros C170 do SPED com dados complementares provenientes dos XMLs de NFe, aplicando fatores de conversão e vinculando ao produto agrupado.

## Função de Geração

```python
def gerar_c170_xml(cnpj: str, pasta_cnpj: Path | None = None) -> bool
```

Módulo: `src/transformacao/c170_xml.py`

## Dependências

- **Depende de**: `fatores_conversao`
- **É dependência de**: `movimentacao_estoque`

## Fontes de Entrada

- Registros C170 extraídos do SPED
- XMLs de NFe processados
- `fatores_conversao_<cnpj>.parquet`
- `fontes_produtos_<cnpj>.parquet`

## Objetivo

Integrar informações do SPED C170 com dados dos XMLs de NFe, aplicando fatores de conversão para padronizar unidades e vinculando cada registro ao `id_agrupado` correspondente. Esta tabela prepara os dados C170 para a movimentação de estoque.

## Principais Colunas

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id_linha_origem` | str | Chave de rastreabilidade: `reg_0000_id + num_doc + num_item` |
| `id_agrupado` | str | Chave do produto agrupado |
| `chave_acesso` | str | Chave de acesso da NFe |
| `num_doc` | str | Número do documento |
| `num_item` | int | Número do item no documento |
| `descricao` | str | Descrição do produto |
| `ncm` | str | Código NCM |
| `qtd` | float | Quantidade original |
| `qtd_conv` | float | Quantidade convertida (`qtd * fator`) |
| `valor_unitario` | float | Valor unitário original |
| `valor_unitario_conv` | float | Valor unitário convertido (`valor / fator`) |
| `unid` | str | Unidade original |
| `unid_ref` | str | Unidade de referência |
| `fator` | float | Fator de conversão aplicado |
| `fonte` | str | Origem: `c170` |

## Regras de Processamento

### Enriquecimento por LEFT JOIN

1. Linha original entra com `codigo_fonte`
2. Tabela ponte (`fontes_produtos`) injeta `id_agrupado`
3. Tabela mestre (`produtos_final`) injeta atributos padronizados
4. Tabela de fatores injeta `unid_ref` e `fator`

### Conversão de Unidades

```
qtd_conv = qtd * fator
valor_unitario_conv = valor_unitario / fator
```

### Neutralizações

Linhas são neutralizadas (`qtd_conv = 0`) quando:

- `mov_rep = true` (espelhamento entre C170 e NFe/NFCe)
- `excluir_estoque = true`
- `infprot_cstat` diferente de `100` ou `150`
- Base da linha igual a zero

## Saída Gerada

```
dados/CNPJ/<cnpj>/analises/produtos/c170_xml_<cnpj>.parquet
```

## Notas

- C170 participa da movimentação de estoque como `1 - ENTRADA`
- A coluna `mov_rep` identifica registros espelhados entre fontes
- Essencial para cruzamento entre SPED e XMLs de NFe
- Preserva rastreabilidade completa via `id_linha_origem`
