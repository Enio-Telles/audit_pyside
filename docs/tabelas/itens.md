# itens

## Visão Geral

Tabela que detalha os itens dos documentos fiscais com informações enriquecidas, servindo como ponte entre a estrutura de documentos e a classificação de produtos.

## Função de Geração

```python
def gerar_itens(cnpj: str, pasta_cnpj: Path | None = None) -> bool
```

Módulo: `src/transformacao/itens.py`

## Dependências

- **Depende de**: `item_unidades`
- **É dependência de**: `descricao_produtos`

## Fontes de Entrada

- `item_unidades_<cnpj>.parquet`
- `tb_documentos_<cnpj>.parquet`

## Objetivo

Consolidar as informações de itens com dados dos documentos fiscais, criando uma visão detalhada que será usada para descrição e classificação de produtos. Esta tabela enriquece os itens com contexto documental.

## Principais Colunas

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id_linha_origem` | str | Chave única da linha original (rastreabilidade) |
| `chave_acesso` | str | Chave de acesso da NFe/NFCe |
| `num_doc` | str | Número do documento fiscal |
| `num_item` | int | Número do item dentro do documento |
| `descricao` | str | Descrição do produto |
| `ncm` | str | Código NCM |
| `cest` | str | Código CEST |
| `cfop` | str | Código CFOP |
| `qtd` | float | Quantidade do item |
| `valor_unitario` | float | Valor unitário do item |
| `valor_total` | float | Valor total do item |
| `fonte` | str | Origem do registro (nfe, nfce, c170) |

## Regras de Processamento

- Preserva a chave de rastreabilidade `id_linha_origem` para auditoria
- Une informações de itens com contexto dos documentos fiscais
- Mantém classificações fiscais originais (NCM, CEST, CFOP)
- Preserva valores unitários e totais para cálculos fiscais

## Saída Gerada

```
dados/CNPJ/<cnpj>/analises/produtos/itens_<cnpj>.parquet
```

## Notas

- Esta tabela mantém o vínculo direto com os documentos fiscais originais
- É usada como base para a descrição e agrupamento de produtos
- A coluna `id_linha_origem` é parte do "fio de ouro" de rastreabilidade
