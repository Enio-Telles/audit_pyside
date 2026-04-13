## 2024-04-03 - Avoid `partition_by` for row-by-row lookups
**Learning:** Using `partition_by(..., as_dict=True)` on a high-cardinality key (like item IDs) to optimize Python loop lookups creates tens of thousands of tiny DataFrames. This is a severe Polars anti-pattern that leads to high overhead and OOM crashes.
**Action:** Do not use `partition_by` to build dictionaries of DataFrames for row-by-row looping. Instead, hoist redundant `.filter()` calls inside the loop, or fully refactor to vectorized `.join()` or `.group_by()` operations.

## 2025-02-24 - [Avoid `partition_by(..., as_dict=True)` for Iteration]
**Learning:** In `04_produtos_final.py`, using `df.partition_by('__descricao_upper', as_dict=True)` to create dictionaries of DataFrames for row-by-row iteration in Polars creates huge numbers of tiny DataFrames. This leads to high overhead and OOM errors, especially on larger datasets.
**Action:** Replace `partition_by(as_dict=True)` with vectorized operations like `.join()` or `.group_by()`. If you must perform custom aggregations, group the data once or construct standard Python dictionaries of dictionaries instead of DataFrames.

## 2025-02-24 - [Avoid Repeated Operations in Frontend useMemo Loops]
**Learning:** In frontend components like `DataTable.tsx`, running string transformations (`.toLowerCase()`) and object iteration (`Object.entries`) *inside* the `rows.filter` callback creates an O(N*M) bottleneck, drastically reducing performance on large datasets.
**Action:** Always hoist static filter extraction and value normalization outside the row iteration loop in `useMemo` to prevent redundant allocations and loop overhead.

## 2025-02-24 - [Vectorize group_by Aggregations Instead of map_groups]
**Learning:** Using `df.group_by().map_groups(python_fn)` with `to_dicts()` and manual Python loops (even with iterative helpers) is significantly slower than native Polars vectorized aggregations. The Python GIL, Python object overhead, and row-by-row iteration prevent Polars from using its Rust engine.
**Action:** Replace `map_groups` with `group_by().agg([...])` using native Polars expressions (`drop_nulls`, `explode`, `concat_list`, `list.eval`, `list.unique`, `list.sort`). Use intermediate `_tmp_*` columns aggregated in the `agg` phase and combined/cleaned in a `with_columns` phase for complex multi-source list unions.
