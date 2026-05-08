with open("src/transformacao/rastreabilidade_produtos/04_produtos_final.py", "r") as f:
    content = f.read()

# Configuração antes do loop
replacement_1 = """
    df_item_unid_parts = df_item_unid_norm.partition_by("__descricao_upper", as_dict=True)
    df_item_unid_empty = df_item_unid_norm.filter(pl.lit(False)).drop("__descricao_upper", strict=False)

    registros_mestra: list[dict] = []
"""
content = content.replace("    registros_mestra: list[dict] = []\n", replacement_1)

# Usando o dict dentro do loop
old_filter_code = """        if desc_norm:
            df_base = (
                df_item_unid_norm
                .filter(pl.col("__descricao_upper") == desc_norm)
                .drop("__descricao_upper")
            )
        else:
            df_base = df_item_unid.filter(pl.lit(False))"""

new_filter_code = """        if desc_norm:
            df_base = df_item_unid_parts.get((desc_norm,), df_item_unid_empty)
            if "__descricao_upper" in df_base.columns:
                df_base = df_base.drop("__descricao_upper")
        else:
            df_base = df_item_unid_empty"""

content = content.replace(old_filter_code, new_filter_code)

with open("src/transformacao/rastreabilidade_produtos/04_produtos_final.py", "w") as f:
    f.write(content)
