import pandas as pd
excel_path = r"c:\funcoes - Copia\dados\referencias\Tabela_estoques.xlsx"
df = pd.read_excel(excel_path)
df.to_json("map_estoque.json", orient="records", indent=4, force_ascii=False)
print("Salvo map_estoque.json")
