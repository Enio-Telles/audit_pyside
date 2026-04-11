## 2025-02-24 - [Avoid Redundant Polars DataFrame Conversions]
**Learning:** `executar_sql` fetched rows, converted them to a Polars DataFrame with inferred schema, and immediately dumped them back to Python dicts via `.to_dicts()`. This bypasses Polars' vectorization capabilities entirely, creating pure O(N) overhead for no benefit.
**Action:** When SQL results are only needed as Python dictionaries (e.g., for JSON APIs or UI models), construct and return the `[dict(zip(columns, row)) for row in rows]` directly. Only create a Polars DataFrame if vectorized operations will be performed on it.
