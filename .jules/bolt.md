## 2024-04-09 - Polars native date extraction instead of map_elements
**Learning:** Polars `map_elements` invoking Python native `datetime.date` functions inside structures is remarkably slower (by roughly 6x in our benchmarks) than native `pl.date(year_col, month_col, day)` expressions. Additionally, calculating `__mes_fim__` can natively use `.dt.month_end()`, completely avoiding manual next-month calculations and extra temporary columns.
**Action:** Always refactor `map_elements` and lambdas converting struct columns to dates into direct calls of `pl.date()`. Leverage `.dt.month_end()` instead of manually incrementing months and subtracting days.
## 2024-04-10 - Native Polars text normalization
**Learning:** Native Polars expressions chaining  for accent removal and upper-casing is ~4x faster than Python  on .
**Action:** Never use  for string normalization; use a custom  chain.
## 2024-04-10 - Native Polars text normalization
**Learning:** Native Polars expressions chaining `str.replace_all` for accent removal and upper-casing is ~4x faster than Python `map_elements` on `remove_accents`.
**Action:** Never use `map_elements` for string normalization; use a custom `str.replace_all` chain.
