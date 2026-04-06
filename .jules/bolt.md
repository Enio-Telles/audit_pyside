
## 2024-05-24 - Number.prototype.toLocaleString vs Intl.NumberFormat performance
**Learning:** Calling `Number.prototype.toLocaleString()` inside a render loop (like a table cell formatter) repeatedly allocates locale data and parses the options object, causing significant performance degradation in JS execution times. In a test with 80k iterations, `toLocaleString()` took ~7.5 seconds, while a cached `Intl.NumberFormat` instance took ~65 milliseconds, making it over 100x faster.
**Action:** Always instantiate and cache `Intl.NumberFormat` objects outside of render loops or frequent operations when formatting large amounts of data in the frontend.
