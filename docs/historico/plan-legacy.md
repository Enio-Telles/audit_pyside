1. Modify `src/utilitarios/codigo_fonte.py` to replace `map_elements` in `expr_normalizar_codigo_fonte` with a native Polars expression using `str.extract_groups` and string methods. This eliminates the GIL bottleneck and provides a massive speedup for data ingestion pipelines.
2. Run `test_perf.py` or unit tests to verify the behavior is identical to the original Python function.
3. Call `pre_commit_instructions` to ensure testing, verification, review, and reflection are done.
4. Submit the pull request with a descriptive title and message according to Bolt's persona.
