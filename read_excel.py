import pandas as pd
excel_path = r"c:\funcoes - Copia\dados\referencias\Tabela_estoques.xlsx"
df = pd.read_excel(excel_path)
print(df.head(20).to_string())
print("\nColunas:", df.columns.tolist())
