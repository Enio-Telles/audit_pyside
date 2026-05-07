"""I/O helpers for audit_pyside."""

from src.io.categorical_recovery import (
    COLUMN_TO_ENUM_KEY,
    DEFAULT_CODES_PATH,
    DYNAMIC_CATEGORICAL_COLUMNS,
    INVARIANT_BLOCKLIST,
    BOOLEAN_FALSE_VALUES_BY_COLUMN,
    BOOLEAN_TRUE_VALUES_BY_COLUMN,
    INVERTED_BOOLEAN_FIELDS,
    assert_no_invariant_categorized,
    build_categorical_map,
    build_enum_map,
    cast_dataframe_typed,
    get_invariant_dtypes,
    load_fiscal_codes,
    reload_fiscal_codes,
    scan_parquet_typed,
    validate_schema_post_cast,
)
from src.io.categorical_writer import batch_rewrite_parquets, rewrite_parquet_typed

__all__ = [
    "COLUMN_TO_ENUM_KEY",
    "DEFAULT_CODES_PATH",
    "DYNAMIC_CATEGORICAL_COLUMNS",
    "INVARIANT_BLOCKLIST",
    "BOOLEAN_FALSE_VALUES_BY_COLUMN",
    "BOOLEAN_TRUE_VALUES_BY_COLUMN",
    "INVERTED_BOOLEAN_FIELDS",
    "assert_no_invariant_categorized",
    "batch_rewrite_parquets",
    "build_categorical_map",
    "build_enum_map",
    "cast_dataframe_typed",
    "get_invariant_dtypes",
    "load_fiscal_codes",
    "reload_fiscal_codes",
    "rewrite_parquet_typed",
    "scan_parquet_typed",
    "validate_schema_post_cast",
]
