from pathlib import Path
import sys
sys.path.insert(0, str(Path('src').resolve()))
import polars as pl
from transformacao.rastreabilidade_produtos import fatores_conversao as fc

# montar df_final simulado
df_final = pl.DataFrame(
    {
        "id_agrupado": ["P2", "P3"],
        "descricao_normalizada": ["prod_x", "prod_x"],
        "descr_padrao": ["Prod X v2", "Prod X v3"],
        "unid_ref_sugerida": ["UN", "UN"],
    }
)
# garantir coluna descricao_final ausente (como fazemos na funcao)
if "descricao_final" not in df_final.columns:
    df_final = df_final.with_columns(pl.lit(None).alias("descricao_final"))

# criar coluna descr_padrao_calc como o codigo faz
df_final = df_final.with_columns(
    pl.coalesce([pl.col("descr_padrao"), pl.col("descricao_final")]).alias("descr_padrao_calc")
)

# df_map_raw simulado (map aponta para P1 que nao existe em df_final)
df_map_raw = pl.DataFrame({
    "descricao_normalizada": ["prod_x"],
    "id_agrupado": ["P1"],
    "descr_padrao_calc": [None],
})

# chamar construtor para map e final
map_df, resumo_map = fc._construir_vinculo_unico_por_descricao(df_map_raw, "descr_padrao_calc", "map_produto_agrupado")
final_df, resumo_final = fc._construir_vinculo_unico_por_descricao(
    df_final.select(["descricao_normalizada", "id_agrupado", "descr_padrao_calc"]),
    "descr_padrao_calc",
    "produtos_final",
)

print('map_df:')
print(map_df)
print('resumo_map:', resumo_map)
print('\nfinal_df:')
print(final_df)
print('resumo_final:', resumo_final)

# concatenar e escolher o primeiro por ordem
bases = []
if not map_df.is_empty():
    bases.append(map_df.with_columns(pl.lit(0).alias("__ordem_vinculo__")))
if not final_df.is_empty():
    bases.append(final_df.with_columns(pl.lit(1).alias("__ordem_vinculo__")))

if bases:
    df_vinculo = pl.concat(bases, how="vertical_relaxed").sort(["descricao_normalizada", "__ordem_vinculo__"]).unique(subset=["descricao_normalizada"], keep="first").drop("__ordem_vinculo__")
else:
    df_vinculo = fc._df_vazio_vinculo_produto()

print('\nconcat df_vinculo:')
print(df_vinculo)

# simular join com df_unid

df_unid = pl.DataFrame({
    "descricao": ["prod_x"],
    "unid": ["UN"],
})

df_unid = df_unid.with_columns(fc._normalizar_descricao_expr("descricao"))
print('\ndf_unid normalized:')
print(df_unid)

# join

if not df_vinculo.is_empty():
    df_link = df_unid.join(df_vinculo, on="descricao_normalizada", how="left")
    print('\ndf_link:')
    print(df_link)
else:
    print('\ndf_vinculo is empty')
