import polars as pl
import time
import random
import string


def filter_approach(df_base, desc_norms):
    for i in range(100):
        _ = df_base.filter(pl.col("descricao").is_in(desc_norms))


def dict_approach(df_base, desc_norms):
    d = df_base.partition_by("descricao", as_dict=True)
    empty = df_base.filter(pl.lit(False))
    for i in range(100):
        parts = [d.get((n,), empty) for n in desc_norms]
        _ = pl.concat(parts) if parts else empty


# Setup
N = 100000


def get_random_string(length):
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for i in range(length))


descricoes = [get_random_string(10) for _ in range(N)]
df = pl.DataFrame({"descricao": descricoes, "val": range(N)})

# Select 100 norms
norms = random.sample(descricoes, 100)

t0 = time.time()
filter_approach(df, norms)
print(f"Filter approach: {time.time() - t0:.4f}s")

t0 = time.time()
dict_approach(df, norms)
print(f"Dict approach: {time.time() - t0:.4f}s")
