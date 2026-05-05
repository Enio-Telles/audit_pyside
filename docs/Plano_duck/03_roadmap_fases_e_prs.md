# Roadmap de Fases e PRs — DuckDB + GUI Paginada

Este documento detalha o sequenciamento da implementação da arquitetura dual Polars/DuckDB, otimização da GUI e o servidor MCP DuckDB.

## Visão Geral das Fases (E0–E8)

| Fase | Título | Objetivo | Prioridade |
|---|---|---|---|
| **E0** | Guard rails anti-travamento | Instrumentação e bloqueio de carregamento total de arquivos grandes | Alta (Crítico) |
| **E1** | DuckDB como backend GUI | Implementação do `DuckDBParquetService` e roteador hibrido | Alta |
| **E2** | Aba Consulta sem travar | Integração do backend DuckDB na Aba Consulta com projeção e paginação | Alta |
| **E3** | Abas especializadas paginadas | Paginação das abas de Movimentação, Entrada e Resumos Fiscais | Média |
| **E4** | Agregação escalável | Otimização da agregação de produtos para datasets multi-GB | Média |
| **E5** | Exportação streaming | Exportação direta via DuckDB (COPY) sem materialização em memória | Baixa |
| **E6** | MCP DuckDB | Servidor MCP para query de Parquet grandes via protocolo MCP | **Roadmap 2026** |
| **E7** | Parquet particionado | Suporte a Hive partitioning para otimização de busca temporal/ID | Baixa |
| **E8** | Escrita streaming | Escrita de resultados via `sink_parquet` para evitar picos de RAM | Baixa |

---

## Detalhamento das Fases

### Fase E0 — Guard rails anti-travamento
| PR | Branch | Objetivo |
|----|--------|----------|
| 0.1 | `perf/gui-large-parquet-instrumentation` | Instrumentar leitura de Parquet na GUI: `log_parquet_open`, threshold de 512MB. |
| 0.2 | `fix/gui-block-full-load-large-parquet` | Bloquear `pl.read_parquet()` para arquivos acima do threshold na GUI. |

### Fase E1 — DuckDB como backend de consulta da GUI
| PR | Branch | Objetivo |
|----|--------|----------|
| 1.1 | `feat/gui-duckdb-parquet-service` | Criar `DuckDBParquetService` (schema, count, page, export). |
| 1.2 | `perf/gui-auto-select-parquet-backend` | `ParquetQueryService` roteador (Polars vs DuckDB). |

### Fase E2 — Aba Consulta sem travar
| PR | Branch | Objetivo |
|----|--------|----------|
| 2.1 | `perf/gui-query-large-parquet-with-duckdb` | Aba Consulta usa DuckDB com projection pushdown. |
| 2.2 | `perf/gui-count-large-parquet-in-background` | Contagem de linhas assíncrona. |

### Fase E3 — Abas especializadas paginadas
| PR | Branch | Objetivo |
|----|--------|----------|
| 3.1 | `perf/gui-replace-full-parquet-async-loader` | Loader assíncrono por página e distinct. |
| 3.2 | `perf/gui-page-mov-estoque` | Aba mov_estoque paginada com filtros SQL. |
| 3.3 | `perf/gui-page-nfe-entrada` | Aba nfe_entrada paginada. |
| 3.4 | `perf/gui-page-stock-summary-tabs` | Paginação de abas mensal/anual/período. |

### Fase E4 — Agregação escalável
| PR | Branch | Objetivo |
|----|--------|----------|
| 4.1 | `perf/agregacao-page-produtos-agrupados` | Paginação das tabelas de agregação. |
| 4.2 | `perf/agregacao-selection-by-key` | Seleção persistente por `id_agrupado`. |
| 4.3 | `perf/agregacao-load-only-selected-ids` | Query direcionada apenas para IDs selecionados. |

### Fase E5 — Exportação grande sem DataFrame inteiro
| PR | Branch | Objetivo |
|----|--------|----------|
| 5.1 | `perf/export-duckdb-copy-large-results` | Exportar via `COPY TO ... (FORMAT PARQUET)`. |

### Fase E6 — MCP DuckDB
| PR | Branch | Objetivo |
|----|--------|----------|
| 6.1 | `feat/tools-duckdb-mcp` | Servidor MCP standalone para exploração de Parquets. |
| 6.2 | `test/large-parquet-mcp-benchmarks` | Validação de performance via protocolo MCP. |

### Fase E7 — Parquet particionado
| PR | Branch | Objetivo |
|----|--------|----------|
| 7.1 | `feat/parquet-query-file-or-dataset-dir` | Suporte a Hive Partitioning no `DuckDBParquetService`. |
| 7.2 | `feat/parquet-partition-mov-estoque` | Salvar mov_estoque particionado por `ano/id_bucket`. |

### Fase E8 — Escrita streaming
| PR | Branch | Objetivo |
|----|--------|----------|
| 8.1 | `perf/parquet-streaming-writes` | Implementar `salvar_para_parquet_streaming` via `sink_parquet`. |
