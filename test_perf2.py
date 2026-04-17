import polars as pl
import time


def map_approach(df_base, desc_norms):
    for i in range(100):
        _ = df_base.filter(pl.col("descricao").str.to_uppercase().is_in(desc_norms))


def col_approach(df_base, desc_norms):
    df_base = df_base.with_columns(
        pl.col("descricao").str.to_uppercase().alias("descricao_upper")
    )
    for i in range(100):
        _ = df_base.filter(pl.col("descricao_upper").is_in(desc_norms))


# Setup
N = 100000
import random
import string


def get_random_string(length):
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for i in range(length))


descricoes = [get_random_string(10) for _ in range(N)]
df = pl.DataFrame({"descricao": descricoes, "val": range(N)})

# Select 100 norms
norms = [n.upper() for n in random.sample(descricoes, 100)]

t0 = time.time()
map_approach(df, norms)
print(f"Map approach: {time.time() - t0:.4f}s")

t0 = time.time()
col_approach(df, norms)
print(f"Col approach: {time.time() - t0:.4f}s")
