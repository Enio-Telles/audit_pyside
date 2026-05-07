# 02 — Arquitetura Alvo: Dual Polars + DuckDB

## Estratégia de Roteamento
A classe `ParquetQueryService` atua como um roteador inteligente:

1. **Arquivos <= 512 MB:** Usa Polars em memória para máxima velocidade de resposta em datasets que cabem no cache.
2. **Arquivos > 512 MB ou Diretórios:** Usa DuckDB com `scan_parquet` para aproveitar predicate pushdown e lazy evaluation, evitando OOM (Out of Memory).

## Configuração Parquet Recomendada
Para garantir interoperabilidade e performance:
```python
df.write_parquet(
    path,
    compression="zstd",
    row_group_size=200_000,   # Faixa: 100_000 a 1_000_000
)
```

Para streaming (LazyFrame):
```python
lf.sink_parquet(
    path,
    compression="zstd",
    row_group_size=200_000,
)
```

## Detecção de Particionamento
```python
source = str(path / "**/*.parquet") if path.is_dir() else str(path)
# DuckDB consegue ler o particionamento Hive automaticamente:
# conn.execute("SELECT * FROM read_parquet(?, hive_partitioning=true)", [source])
```

## Referências Técnicas
- [DuckDB Python API](https://duckdb.org/docs/api/python/overview)
- [DuckDB read_parquet](https://duckdb.org/docs/data/parquet/overview)
- [DuckDB Hive Partitioning](https://duckdb.org/docs/data/partitioning/hive_partitioning)
- [Polars sink_parquet](https://docs.pola.rs/api/python/stable/reference/lazyframe/index.html)
