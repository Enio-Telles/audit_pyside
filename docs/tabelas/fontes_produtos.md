# fontes_produtos

## Visao Geral

Camada de enriquecimento que propaga `id_agrupado` e atributos consolidados de produto para as fontes brutas usadas depois por `c170_xml`, `c176_xml` e `movimentacao_estoque`.

## Funcao de Geracao

```python
def gerar_fontes_produtos(cnpj: str, pasta_cnpj: Path | None = None) -> bool
```

Modulo de entrada: `src/transformacao/fontes_produtos.py`

Implementacao canonica: `src/transformacao/rastreabilidade_produtos/fontes_produtos.py`

## Dependencias

- **Depende de**: `produtos_final`, `map_produto_agrupado`
- **E dependencia de**: `fatores_conversao`, `c170_xml`, `c176_xml`, `movimentacao_estoque`

## Fontes de Entrada

- `dados/CNPJ/<cnpj>/analises/produtos/produtos_final_<cnpj>.parquet`
- `dados/CNPJ/<cnpj>/analises/produtos/map_produto_agrupado_<cnpj>.parquet`
- fontes brutas em `dados/CNPJ/<cnpj>/arquivos_parquet/`

## Objetivo

Enriquecer cada fonte operacional com:

- `id_agrupado`
- `descr_padrao`
- `ncm_padrao`
- `cest_padrao`
- `co_sefin_agr`
- `unid_ref_sugerida`

O vinculo prioriza `codigo_fonte` quando possivel e usa `descricao_normalizada` apenas como fallback controlado.

## Principais Colunas de Saida

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `id_agrupado` | str | Chave do produto agrupado |
| `codigo_fonte` | str | Chave canonica da fonte quando presente |
| `descricao_normalizada` | str | Descricao normalizada usada no fallback |
| `descr_padrao` | str | Descricao padrao do grupo |
| `ncm_padrao` | str | NCM padrao do grupo |
| `cest_padrao` | str | CEST padrao do grupo |
| `co_sefin_agr` | str | Codigo SEFIN consolidado do grupo |
| `unid_ref_sugerida` | str | Unidade de referencia sugerida do grupo |

## Regras de Processamento

### Vinculo principal

- tenta vincular por `codigo_fonte`
- quando ha ambiguidade, registra aviso e mantem um unico match

### Fallback

- para linhas sem match por `codigo_fonte`, tenta `descricao_normalizada`
- linhas ainda sem `id_agrupado` sao registradas em log e removidas da saida `_agr`

### Rastreabilidade

- preserva colunas de rastreabilidade da fonte original quando existirem
- nao substitui a fonte bruta; gera artefatos enriquecidos paralelos

## Saidas Geradas

Nao ha um unico parquet `fontes_produtos_<cnpj>.parquet`.

A funcao gera artefatos por fonte em `dados/CNPJ/<cnpj>/arquivos_parquet/`, por exemplo:

- `c170_agr_<cnpj>.parquet`
- `bloco_h_agr_<cnpj>.parquet`
- `nfe_agr_<cnpj>.parquet`
- `nfce_agr_<cnpj>.parquet`

Logs auxiliares de faltantes sao gravados em `dados/CNPJ/<cnpj>/analises/produtos/`, por exemplo:

- `<fonte>_agr_sem_id_agrupado_<cnpj>.parquet`

## Notas

- A camada canonica de vinculo e `map_produto_agrupado_<cnpj>.parquet`
- `produtos_final` fornece os atributos consolidados que sao propagados para as fontes
- Esta etapa e parte do fio de ouro entre fonte operacional e tabela analitica
