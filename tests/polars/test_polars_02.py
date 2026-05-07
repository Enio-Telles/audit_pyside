import polars as pl

df = pl.DataFrame({"a": ["x", "y", "x"], "b": [1, 2, 3]})
try:
    d = df.partition_by("a", as_dict=True)
    print(d[("x",)])
except Exception as e:
    print(e)
