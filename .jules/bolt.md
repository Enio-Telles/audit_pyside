## 2024-05-18 - Avoid List Comprehension with Polars Initialization
**Learning:** Initializing a Polars DataFrame by mapping a list of DB tuples into a list of dictionaries via a python list comprehension (e.g. `[dict(zip(cols, row)) for row in rows]`) is extremely slow and acts as a massive bottleneck for data ingestion.
**Action:** Always create a `pl.DataFrame` directly from the raw list of tuples using the `orient="row"` argument. It bypasses python dictionary creation entirely and runs an order of magnitude faster.

## 2024-05-24 - Pre-calculate map_elements outside loops and use partition_by
**Learning:** In Polars, `map_elements` is slow, but calling `map_elements` inside a `.filter` block *within a loop* over groups multiplies that overhead. In files like `produtos_agrupados.py` or `04_produtos_final.py` where a base DataFrame is queried per group, doing `.filter(pl.col(...).map_elements(...) == ...)` per iteration is O(N*M) and very slow. Additionally, `partition_by(..., as_dict=True)` is generally faster than multiple `filter` operations for O(1) group lookups.
**Action:** Extract expensive transformations like `.map_elements` (or `str.to_uppercase()`) out of loops. Compute them once on the base DataFrame. Then use `partition_by("col", as_dict=True)` to create an O(1) lookup dictionary of DataFrames to avoid repeatedly filtering the entire base DataFrame.
