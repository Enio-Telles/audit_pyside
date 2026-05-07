# 03 — Roadmap de Fases (E0–E8)

O projeto está dividido em fases incrementais para garantir estabilidade.

## Fase 0 — Guard rails anti-travamento
- **E0.1:** Instrumentação de leitura de Parquet na GUI (threshold 512MB).
- **E0.2:** Bloqueio de leitura integral de arquivos gigantes.

## Fase 1 — DuckDB como backend de consulta
- **E1.1:** `DuckDBParquetService` isolado (count, page, distinct, export).
- **E1.2:** `ParquetQueryService` (roteador híbrido Polars/DuckDB).

## Fase 2 — Aba Consulta Paginada
- **E2.1:** Integração do serviço híbrido na Aba Consulta.
- **E2.2:** Contagem assíncrona de linhas em background.

## Fase 3 — Abas Especializadas
- **E3.1:** Paginação de `mov_estoque`.
- **E3.2:** Paginação de `nfe_entrada`.
- **E3.3:** Paginação de abas de resumo (mensal, anual, períodos).

## Fase 4 — Agregação Escalável
- **E4.1:** Paginação das tabelas de agregação.
- **E4.2:** Seleção persistente de IDs entre páginas.
- **E4.3:** Agregação via `WHERE id_agrupado IN (...)`.

## Fase 5 — Exportação Streaming
- **E5.1:** Export via DuckDB `COPY (...) TO ... FORMAT PARQUET`.
- **E5.2:** Bloqueio de Excel para volumes extremos.

## Fase 6 — Particionamento Físico
- **E6.1:** Suporte a Hive partitioning no serviço.
- **E6.2:** Particionamento de `mov_estoque` por `ano/bucket`.

## Fase 7 — Escrita Streaming
- **E7.1:** Uso de `sink_parquet` do Polars para reduzir pegada de memória na escrita.

## Fase 8 — MCP e Benchmarks
- **E8.1:** MCP DuckDB local para diagnóstico via agentes.
- **E8.2:** Benchmarks oficiais de 256MB, 1GB e 2GB.
