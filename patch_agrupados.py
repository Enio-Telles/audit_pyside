
with open("src/transformacao/rastreabilidade_produtos/produtos_agrupados.py", "r") as f:
    content = f.read()

# Normalizar descrições uma única vez fora do loop
replacement_1 = """
    df_base = df_base.with_columns(
        pl.col("descricao")
        .map_elements(_normalizar_descricao_para_match, return_dtype=pl.String)
        .alias("__descricao_norm")
    )
    df_base_parts = df_base.partition_by("__descricao_norm", as_dict=True)
    df_base_empty = df_base.filter(pl.lit(False))

    registros_mestra = []
"""
content = content.replace("    registros_mestra = []\n", replacement_1)

# Usar a partição dentro do loop
old_filter_code = """        if desc_norms:
            df_base_filtered = df_base.filter(
                pl.col("descricao").map_elements(_normalizar_descricao_para_match, return_dtype=pl.String).is_in(desc_norms)
            )
        else:
            df_base_filtered = df_base.filter(pl.lit(False))"""

new_filter_code = """        if desc_norms:
            partes = [df_base_parts.get((n,), df_base_empty) for n in desc_norms]
            df_base_filtered = pl.concat(partes, how="vertical_relaxed") if partes else df_base_empty
        else:
            df_base_filtered = df_base_empty"""

content = content.replace(old_filter_code, new_filter_code)

with open("src/transformacao/rastreabilidade_produtos/produtos_agrupados.py", "w") as f:
    f.write(content)
