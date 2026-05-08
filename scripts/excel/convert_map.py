import pandas as pd
from src.utilitarios.project_paths import DATA_ROOT

excel_path = str(DATA_ROOT / "referencias" / "Tabela_estoques.xlsx")
df = pd.read_excel(excel_path)
df.to_json("map_estoque.json", orient="records", indent=4, force_ascii=False)
print("Salvo map_estoque.json")
