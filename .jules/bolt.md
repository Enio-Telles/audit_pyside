## 2024-04-03 - Avoid `partition_by` for row-by-row lookups
**Learning:** Using `partition_by(..., as_dict=True)` on a high-cardinality key (like item IDs) to optimize Python loop lookups creates tens of thousands of tiny DataFrames. This is a severe Polars anti-pattern that leads to high overhead and OOM crashes.
**Action:** Do not use `partition_by` to build dictionaries of DataFrames for row-by-row looping. Instead, hoist redundant `.filter()` calls inside the loop, or fully refactor to vectorized `.join()` or `.group_by()` operations.

## 2024-04-03 - Preserve `bolt.md` history when adding new learnings
**Learning:** This file should be treated as an append-only log of Bolt/Jules learnings so prior entries remain available for future context and decision tracking.
**Action:** When recording a new learning in `.jules/bolt.md`, keep all existing sections intact and append the new entry below them instead of replacing earlier content.
