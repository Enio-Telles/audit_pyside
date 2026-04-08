
## 2024-05-24 - Number.prototype.toLocaleString vs Intl.NumberFormat performance
**Learning:** Calling `Number.prototype.toLocaleString()` inside a render loop (like a table cell formatter) repeatedly allocates locale data and parses the options object, causing significant performance degradation in JS execution times. In a test with 80k iterations, `toLocaleString()` took ~7.5 seconds, while a cached `Intl.NumberFormat` instance took ~65 milliseconds, making it over 100x faster.
**Action:** Always instantiate and cache `Intl.NumberFormat` objects outside of render loops or frequent operations when formatting large amounts of data in the frontend.
## 2026-04-08 - Vectorized String Normalization and Unicode Mojibake Fix
**Learning:** When refactoring legacy string manipulation or normalization functions, explicitly verify regular expressions for corrupted unicode characters (mojibake like `ÃƒÂÃƒÂ€`), which can cause functional regressions if migrated blindly to native Polars `str.replace_all`. Replacing `.map_elements()` with native Polars expressions drastically improves performance, but the expressions must be character-accurate.
**Action:** Always verify regex matches with live data or unit tests, ensuring characters like accents are properly encoded before relying on `replace_all` in a vectorized environment.
