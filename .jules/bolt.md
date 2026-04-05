## 2024-05-30 - Refactor Parquet schema validation logic to prevent duplicated disk reads
 **Learning:** In Polars, `pl.scan_parquet().collect_schema()` performs disk I/O to read the parquet metadata. When immediately followed by a `pl.read_parquet()` call to load the entire dataset, we effectively hit the disk twice unnecessarily. By reading the file into a DataFrame first and validating the schema on the loaded DataFrame's `.columns`, we eliminate the duplicate read.
 **Action:** Prioritize loading data into a DataFrame and validating the DataFrame schema over using `scan_parquet().collect_schema()` when the file is going to be loaded fully into memory right after.

## 2024-03-31 - Optimize Recursive Nested List Flattening in id_agrupados
 **Learning:** When processing complex, deeply nested heterogeneous lists of lists and strings inside `polars` `map_groups` (specifically when Polars `list.eval` expressions fail on heterogeneous lengths/types), a clean iterative Python `while` stack pushing directly to a `set` drastically reduces memory allocations and call overhead compared to recursive function calls returning and extending lists.
 **Action:** Prioritize iterative algorithms targeting single mutable collections (like `set.add` inside a `while stack:`) over pure recursion returning temporary `list` buffers when flattening heavily nested structures in data pipelines.

## 2024-05-30 - Replace O(N*M) loop filter with partition_by
**Learning:** Using `polars.DataFrame.filter()` inside a Python loop can lead to severe O(N*M) performance bottlenecks when doing cross-dataset lookups, as each iteration performs a full scan of the dataframe. Using `partition_by(column, as_dict=True)` pre-computes a constant-time lookup dictionary mapping keys to their corresponding DataFrames, turning an O(N*M) operation into an O(N+M) operation. This reduces lookup times significantly (e.g. 0.43s -> 0.05s in test script).
**Action:** Always prefer `partition_by(..., as_dict=True)` over iterative `df.filter()` when querying or cross-referencing values inside a Python `for` or `while` loop.

## 2024-05-30 - Replace `pl.read_parquet(..., n_rows=0).schema` with `pl.read_parquet_schema(...)`
**Learning:** In Polars, using `pl.read_parquet(path, n_rows=0).schema` to read the metadata of a Parquet file incorrectly initializes the query engine and allocates an empty DataFrame, carrying unnecessary overhead. Using the specialized API `pl.read_parquet_schema(path)` natively parses the Parquet metadata header without these side effects.
**Action:** Always use `pl.read_parquet_schema(path)` when only the schema mapping of a Parquet file is needed.
