1. **Refactor aggregation loop in `AggregationService`**
   - The method `recalcular_todos_padroes` in `src/interface_grafica/services/aggregation_service.py` iterates row-by-row over dictionaries to merge aggregated group properties into `df_agrup`.
   - This involves partitioning a dataframe into tens of thousands of dictionaries (`df_base_mapped.partition_by(...)`), mapping lists, and executing logic in Python space which takes several seconds on larger datasets.
   - We will replace the manual `for` loop and `partition_by` logic with native Polars `join` operations combined with `with_columns`, resulting in significant speedups by using Rust backend aggregations.

2. **Pre-commit tasks**
   - Verify changes with pytest (`pnpm test` equivalent/`pytest`).
   - Run linter checks to ensure compliance with conventions.

3. **Submit the PR**
   - Create PR using the Bolt title conventions (⚡ Bolt: [...]) and requested template.
