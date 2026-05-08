import pandas as pd
from src.utilitarios.project_paths import DATA_ROOT

excel_path = str(DATA_ROOT / "referencias" / "Tabela_estoques.xlsx")
df = pd.read_excel(excel_path)
print(df.head(20).to_string())
print("\nColunas:", df.columns.tolist())
