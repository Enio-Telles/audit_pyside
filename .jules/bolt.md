## 2024-05-30 - Refactor Parquet schema validation logic to prevent duplicated disk reads
 **Learning:** In Polars, `pl.scan_parquet().collect_schema()` performs disk I/O to read the parquet metadata. When immediately followed by a `pl.read_parquet()` call to load the entire dataset, we effectively hit the disk twice unnecessarily. By reading the file into a DataFrame first and validating the schema on the loaded DataFrame's `.columns`, we eliminate the duplicate read.
 **Action:** Prioritize loading data into a DataFrame and validating the DataFrame schema over using `scan_parquet().collect_schema()` when the file is going to be loaded fully into memory right after.

## 2024-03-31 - Optimize Recursive Nested List Flattening in id_agrupados
 **Learning:** When processing complex, deeply nested heterogeneous lists of lists and strings inside `polars` `map_groups` (specifically when Polars `list.eval` expressions fail on heterogeneous lengths/types), a clean iterative Python `while` stack pushing directly to a `set` drastically reduces memory allocations and call overhead compared to recursive function calls returning and extending lists.
 **Action:** Prioritize iterative algorithms targeting single mutable collections (like `set.add` inside a `while stack:`) over pure recursion returning temporary `list` buffers when flattening heavily nested structures in data pipelines.
