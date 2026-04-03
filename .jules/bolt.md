## 2024-04-03 - Avoid `partition_by` for row-by-row lookups
**Learning:** Using `partition_by(..., as_dict=True)` on a high-cardinality key (like item IDs) to optimize Python loop lookups creates tens of thousands of tiny DataFrames. This is a severe Polars anti-pattern that leads to high overhead and OOM crashes.
**Action:** Do not use `partition_by` to build dictionaries of DataFrames for row-by-row looping. Instead, hoist redundant `.filter()` calls inside the loop, or fully refactor to vectorized `.join()` or `.group_by()` operations.
