## 2024-05-18 - Avoid List Comprehension with Polars Initialization
**Learning:** Initializing a Polars DataFrame by mapping a list of DB tuples into a list of dictionaries via a python list comprehension (e.g. `[dict(zip(cols, row)) for row in rows]`) is extremely slow and acts as a massive bottleneck for data ingestion.
**Action:** Always create a `pl.DataFrame` directly from the raw list of tuples using the `orient="row"` argument. It bypasses python dictionary creation entirely and runs an order of magnitude faster.

## 2024-05-24 - Pre-calculate map_elements outside loops and use partition_by
**Learning:** In Polars, `map_elements` is slow, but calling `map_elements` inside a `.filter` block *within a loop* over groups multiplies that overhead. In files like `produtos_agrupados.py` or `04_produtos_final.py` where a base DataFrame is queried per group, doing `.filter(pl.col(...).map_elements(...) == ...)` per iteration is O(N*M) and very slow. Additionally, `partition_by(..., as_dict=True)` is generally faster than multiple `filter` operations for O(1) group lookups.
**Action:** Extract expensive transformations like `.map_elements` (or `str.to_uppercase()`) out of loops. Compute them once on the base DataFrame. Then use `partition_by("col", as_dict=True)` to create an O(1) lookup dictionary of DataFrames to avoid repeatedly filtering the entire base DataFrame.
## 2026-04-23 - Native Polars Expression for Jaccard Similarity
**Learning:** Using `map_elements` to apply a custom Python function for string similarity calculation (e.g. splitting words and finding set intersections) is a major performance bottleneck in Polars. It forces row-by-row execution and defeats multi-threading.
**Action:** Always replace string tokenization and similarity logic with native Polars expressions using `str.split(" ")`, `list.eval(pl.element().filter(pl.element() != ""))` to drop empty strings, and `list.set_intersection()` to natively compute Jaccard similarity. This approach unlocks multithreaded Rust execution and drastically cuts processing time.
## 2026-04-28 - Polars map_elements and null handling
**Learning:** By default, Polars `map_elements` uses `skip_nulls=True`, silently returning `null` for `null` inputs without executing the python lambda function.
**Action:** When refactoring `map_elements` into native Polars expressions (e.g. `pl.when().then().otherwise()`), ensure you explicitly map `.is_null()` back to `null` to accurately recreate the default `map_elements` behavior, even if the legacy python lambda would have handled `None` differently (e.g., returning an empty list `[]`).
## 2024-05-03 - Jaccard Math Optimization
**Learning:** In highly called text-similarity functions (like those calculating Jaccard index), using Python's set union operator `a | b` creates a completely new set object in memory, taking $O(|A| + |B|)$ time. This is a huge bottleneck.
**Action:** Use the Inclusion-Exclusion principle `len(a) + len(b) - len(a & b)` whenever Jaccard is needed to avoid the memory allocation and set construction, which yielded an almost 50% speedup in local benchmarks.
## 2026-05-05 - [ID Generation Performance Optimization]
**Learning:** When a non-native Python function is strictly required in Polars (like generating a stable cryptographic hash via `hashlib.sha1` which Polars `.hash()` breaks), using `map_batches` with a list comprehension over the Series avoids the heavy element-wise boundary crossing overhead of `map_elements`, yielding a ~15% speedup.
**Action:** Prioritize `map_batches` over `map_elements` when UDFs cannot be vectorized into native Polars expressions.
