## 2025-04-05 - Avoid Full String Parsing in map_elements
**Learning:** Using `map_elements` for full string normalization (uppercase, strip, split/join) creates a significant bottleneck because it completely bypasses Polars' vectorized Rust engine.
**Action:** When a Python function is strictly needed (e.g., `unicodedata` for removing accents), extract ONLY that specific piece into a barebones `map_elements` call, and chain the rest of the string operations using native Polars expressions like `.str.to_uppercase()`, `.str.strip_chars()`, and `.str.replace_all()`.
