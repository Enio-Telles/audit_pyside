from __future__ import annotations

from typing import Union

import polars as pl

__all__ = ["ensure_id_aliases"]


def ensure_id_aliases(df: Union[pl.DataFrame, pl.LazyFrame]) -> pl.DataFrame:
    """
    Ensure the presence of both `id_agrupado` (canonical) and
    `id_agregado` (presentation alias) on the given DataFrame.

    - If one of the two columns is missing, it is created as a Utf8
      copy of the other.
    - If the frame is a `LazyFrame`, it will be collected.

    This is a small compatibility shim used by parts of the UI and
    persistence layer to accept either column name.
    """
    if isinstance(df, pl.LazyFrame):
        df = df.collect()

    cols = set(df.columns)

    # If id_agrupado exists but id_agregado does not, create alias
    if "id_agrupado" in cols and "id_agregado" not in cols:
        df = df.with_columns(pl.col("id_agrupado").cast(pl.Utf8).alias("id_agrupado"))
        df = df.with_columns(pl.col("id_agrupado").cast(pl.Utf8).alias("id_agregado"))
        return df

    # If id_agregado exists but id_agrupado does not, create canonical id
    if "id_agregado" in cols and "id_agrupado" not in cols:
        df = df.with_columns(pl.col("id_agregado").cast(pl.Utf8).alias("id_agregado"))
        df = df.with_columns(pl.col("id_agregado").cast(pl.Utf8).alias("id_agrupado"))
        return df

    # If both present (or both missing) ensure Utf8 casting when present
    casts = []
    if "id_agrupado" in cols:
        casts.append(pl.col("id_agrupado").cast(pl.Utf8).alias("id_agrupado"))
    if "id_agregado" in cols:
        casts.append(pl.col("id_agregado").cast(pl.Utf8).alias("id_agregado"))
    if casts:
        df = df.with_columns(casts)

    return df
