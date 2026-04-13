import polars as pl
import time

def map_approach(df_base, desc_norms):
    for i in range(100):
        _ = df_base.filter(pl.col("descricao").map_elements(lambda x: x.upper(), return_dtype=pl.String).is_in(desc_norms))

def direct_approach(df_base, desc_norms):
    for i in range(100):
        _ = df_base.filter(pl.col("descricao_norm").is_in(desc_norms))

# Setup
N = 100000
import random
import string
def get_random_string(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

descricoes = [get_random_string(10) for _ in range(N)]
df = pl.DataFrame({
    "descricao": descricoes,
    "val": range(N)
})
df = df.with_columns(pl.col("descricao").map_elements(lambda x: x.upper(), return_dtype=pl.String).alias("descricao_norm"))

# Select 100 norms
norms = [n.upper() for n in random.sample(descricoes, 100)]

t0 = time.time()
map_approach(df, norms)
print(f"Map approach: {time.time() - t0:.4f}s")

t0 = time.time()
direct_approach(df, norms)
print(f"Direct approach: {time.time() - t0:.4f}s")
