1. **Optimize `pl.read_parquet(..., n_rows=0).schema`**
   - Replace `pl.read_parquet(path, n_rows=0).schema` with `pl.read_parquet_schema(path)` which is the more efficient way to read only the metadata of a Parquet file without reading the whole file structure or launching a query engine dataframe.
   - Files to update: `src/utilitarios/aux_leitura_notas.py`, `src/transformacao/tabelas_base/01_item_unidades.py`, `src/transformacao/01_item_unidades.py`.

2. **Complete pre-commit steps to ensure proper testing, verification, review, and reflection are done.**
   - Run linter and tests to make sure no syntax is broken.
   - Run the custom `pre_commit_instructions` tool.
