# Tabelas do Fiscal Parquet Analyzer

Este diretorio contem a documentacao das principais tabelas geradas pelo pipeline fiscal.

## Visao Geral do Pipeline

O pipeline segue a ordem topologica definida em `src/orquestrador_pipeline.py`:

```text
tb_documentos
    ->
item_unidades
    ->
itens
    ->
descricao_produtos
    ->
produtos_final
    ->
fontes_produtos
    ->
fatores_conversao
    ->
    |-> c170_xml --|
    |-> c176_xml --|
               ->
      movimentacao_estoque
               ->
      calculos_mensais / calculos_anuais / calculos_periodos
```

## Tabelas de Base

| Tabela | Descricao | Dependencias |
|--------|-----------|--------------|
| [tb_documentos](tb_documentos.md) | Consolida documentos fiscais extraidos | Nenhuma |
| [item_unidades](item_unidades.md) | Detalha itens por unidade de medida | `tb_documentos` |
| [itens](itens.md) | Detalha itens com informacoes enriquecidas | `item_unidades` |
| [descricao_produtos](descricao_produtos.md) | Padroniza descricoes de produtos | `item_unidades`, `itens` |

## Tabelas de Agrupamento

| Tabela | Descricao | Dependencias |
|--------|-----------|--------------|
| [produtos_final](produtos_final.md) | Agrupamento mestre de produtos | `descricao_produtos`, `item_unidades` |
| [fontes_produtos](fontes_produtos.md) | Enriquecimento das fontes com `id_agrupado` e atributos do grupo | `produtos_final`, `map_produto_agrupado` |
| [fatores_conversao](fatores_conversao.md) | Fatores de conversao entre unidades | `item_unidades`, `produtos_final` |

## Tabelas de Enriquecimento

| Tabela | Descricao | Dependencias |
|--------|-----------|--------------|
| [c170_xml](c170_xml.md) | C170 enriquecido com XMLs e fatores | `fatores_conversao` |
| [c176_xml](c176_xml.md) | C176 enriquecido com XMLs e fatores | `fatores_conversao` |
| [movimentacao_estoque](movimentacao_estoque.md) | Fluxo cronologico de estoque | `c170_xml`, `c176_xml` |

## Tabelas Analiticas

| Tabela | Descricao | Dependencias |
|--------|-----------|--------------|
| [calculos_mensais](calculos_mensais.md) | Resumo mensal da movimentacao | `movimentacao_estoque` |
| [calculos_anuais](calculos_anuais.md) | Auditoria anual com ICMS | `movimentacao_estoque` |
| [calculos_periodos](calculos_periodo.md) | Recorte por periodos de inventario | `movimentacao_estoque` |

## Conceitos Fundamentais

### Golden Thread

O sistema preserva rastreabilidade pelo fio:

```text
linha original -> codigo_fonte -> id_descricao -> id_agrupado -> tabelas analiticas
```

### Chaves Centrais

| Chave | Descricao |
|-------|-----------|
| `id_item_unid` | Chave fisica da linha consolidada em `item_unidades` |
| `codigo_fonte` | Chave canonica da fonte operacional quando disponivel |
| `id_descricao` | Chave intermediaria de agrupamento por descricao |
| `id_agrupado` | Chave mestra do produto consolidado |

### Contrato de Funcoes

As funcoes de geracao seguem o contrato:

```python
def gerar_<etapa>(cnpj: str, pasta_cnpj: Path | None = None) -> bool
```

ou, no caso de fatores:

```python
def calcular_fatores_conversao(cnpj: str, pasta_cnpj: Path | None = None) -> bool
```

## Localizacao dos Arquivos

Camada analitica principal:

```text
dados/CNPJ/<cnpj>/analises/produtos/
```

Fontes enriquecidas `_agr`:

```text
dados/CNPJ/<cnpj>/arquivos_parquet/
```

## Notas Importantes

- ajustes manuais de agrupamento e conversao devem ser preservados em reprocessamentos
- `produtos_final` tem implementacao canonica em `_produtos_final_impl.py`; os demais entrypoints sao proxies legados
- `fontes_produtos` nao gera um unico parquet mestre; ela materializa uma saida `_agr` por fonte
- `fatores_conversao` usa a camada canonica de agrupamento e preserva overrides existentes
