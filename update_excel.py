import pandas as pd
from src.utilitarios.project_paths import DATA_ROOT

excel_path = str(DATA_ROOT / "referencias" / "Tabela_estoques.xlsx")
df = pd.read_excel(excel_path)

# Atualiza a linha onde Campo/tabela é 'Ser' para o C170
# Se não existir a linha, podemos criar, mas ela existe (vi no JSON)
mask = df['Campo/tabela'] == 'Ser'
if mask.any():
    df.loc[mask, 'C170'] = 'ser'
    print("Atualizado mapping de 'Ser' para 'ser' no C170.")
else:
    print("Campo 'Ser' nao encontrado no Excel!")

df.to_excel(excel_path, index=False)
print("Excel salvo.")
