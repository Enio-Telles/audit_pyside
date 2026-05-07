import polars as pl

df = pl.DataFrame({"a": ["x", "y", "x"], "b": [1, 2, 3]})
d = df.partition_by("a", as_dict=True)
print(d.keys())
for k, v in d.items():
    print(f"Key: {k}, Type: {type(k)}")
    print(v)
