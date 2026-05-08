# 02 — Arquitetura Alvo Polars + DuckDB

## Desenho da Solucao

A aplicacao passa a operar em modo hibrido:

1. **Camada de Dados:** Arquivos Parquet (unico ou particionado Hive).
2. **Camada de Servico:** `ParquetQueryService` atua como fachada.
   - Se arquivo < 512MB: Usa Polars (leitura rapida em memoria).
   - Se arquivo >= 512MB: Usa DuckDB (lazy scanning com pushdown).
3. **Interface (UI):** Consome apenas paginas de dados (ex: 100 linhas por vez).

## Fluxo de Consulta
`UI -> Service -> DuckDB (SQL) -> Parquet -> Page -> UI`
