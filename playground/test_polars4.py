import polars as pl

df = pl.DataFrame({"a": ["x", "y", "x", None], "b": [1, 2, 3, 4]})
try:
    d = df.partition_by("a", as_dict=True)
    print(d)
except Exception as e:
    print(e)
