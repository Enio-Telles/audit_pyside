## 2025-02-24 - [Avoid Redundant Polars DataFrame Conversions]
**Learning:** `executar_sql` fetched rows, converted them to a Polars DataFrame with inferred schema, and immediately dumped them back to Python dicts via `.to_dicts()`. This bypasses Polars' vectorization capabilities entirely, creating pure O(N) overhead for no benefit.
**Action:** When SQL results are only needed as Python dictionaries (e.g., for JSON APIs or UI models), construct and return the `[dict(zip(columns, row)) for row in rows]` directly. Only create a Polars DataFrame if vectorized operations will be performed on it.

## 2025-02-24 - [Avoid `partition_by(..., as_dict=True)` for Iteration]
**Learning:** In `04_produtos_final.py`, using `df.partition_by('__descricao_upper', as_dict=True)` to create dictionaries of DataFrames for row-by-row iteration in Polars creates huge numbers of tiny DataFrames. This leads to high overhead and OOM errors, especially on larger datasets.
**Action:** Replace `partition_by(as_dict=True)` with vectorized operations like `.join()` or `.group_by()`. If you must perform custom aggregations, group the data once or construct standard Python dictionaries of dictionaries instead of DataFrames.
## 2025-02-24 - [Avoid Repeated Operations in Frontend useMemo Loops]
**Learning:** In frontend components like `DataTable.tsx`, running string transformations (`.toLowerCase()`) and object iteration (`Object.entries`) *inside* the `rows.filter` callback creates an O(N*M) bottleneck, drastically reducing performance on large datasets.
**Action:** Always hoist static filter extraction and value normalization outside the row iteration loop in `useMemo` to prevent redundant allocations and loop overhead.
