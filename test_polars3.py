import polars as pl
df = pl.DataFrame({"a": ["x", "y", "x"], "b": [1, 2, 3]})
df_empty = df.filter(pl.lit(False))
d = df.partition_by("a", as_dict=True)
print(d.get(("z",), df_empty))
