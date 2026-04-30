# audit_pyside — Wiki

Porta de entrada da documentacao tecnica do projeto `audit_pyside`.

`audit_pyside` e uma aplicacao desktop de auditoria fiscal com interface PySide6,
pipeline Python/Polars e persistencia em Parquet.

## Paginas desta Wiki

| Pagina | O que encontrar |
|---|---|
| [Catalogo de Tabelas e Campos](Catalogo-de-Tabelas-e-Campos) | Mapa central das tabelas Parquet, relacoes e formulas globais |
| [Tabelas Base — Campos e Formulas](Tabelas-Base-Campos-e-Formulas) | `tb_documentos`, `item_unidades`, `itens`, `descricao_produtos` |
| [Tabelas de Agrupamento — Campos e Formulas](Tabelas-de-Agrupamento-Campos-e-Formulas) | `produtos_final`, `fontes_produtos`, `fatores_conversao` |
| [Tabelas de Estoque e Enriquecimento — Campos e Formulas](Tabelas-de-Estoque-e-Enriquecimento-Campos-e-Formulas) | `c170_xml`, `c176_xml`, `movimentacao_estoque` |
| [Tabelas Analiticas — Campos e Formulas](Tabelas-Analiticas-Campos-e-Formulas) | `calculos_mensais`, `calculos_anuais`, `calculos_periodos` |

## Fio de rastreabilidade (golden thread)

```text
linha original
  -> id_linha_origem
  -> codigo_fonte
  -> map_produto_agrupado
  -> id_descricao
  -> id_agrupado_base
  -> id_agrupado
  -> fatores_conversao
  -> movimentacao_estoque
  -> calculos_mensais / calculos_anuais / calculos_periodos
```

## Chaves invariantes

Esses campos nao podem ter semantica alterada sem teste explicito:

| Campo | Descricao |
|---|---|
| `id_agrupado` | Chave mestra do produto consolidado |
| `id_agregado` | Alias de saida para `id_agrupado` em tabelas analiticas |
| `q_conv` | Quantidade convertida observada na linha |
| `q_conv_fisica` | Quantidade convertida que altera saldo fisico |
| `__qtd_decl_final_audit__` | Quantidade declarada de estoque final para auditoria |

## Documentacao canonica versionada

A documentacao tecnica detalhada e versionada vive no repositorio:

- [`docs/README.md`](../blob/main/docs/README.md) — indice de documentacao tecnica
- [`docs/tabelas/*.md`](../tree/main/docs/tabelas) — contratos de campos Parquet
- [`docs/adr/*.md`](../tree/main/docs/adr) — decisoes arquiteturais aceitas (ADRs)
- [`AGENTS.md`](../blob/main/AGENTS.md) — regras para agentes/IA
- [`docs/wiki/`](../tree/main/docs/wiki) — versao versionada destas paginas wiki

## Fonte de gestao operacional

Roadmap, backlog, decisoes de prioridade e status das fases estao no Notion:
[Central de Gestao, Refatoracao e Manutencao — audit_pyside](https://www.notion.so/352edc8b7d5d818cbb3df26788100805)
