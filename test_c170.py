import sys
import os
from pathlib import Path
import polars as pl
from src.utilitarios.project_paths import PROJECT_ROOT

# Setup paths
ROOT_DIR = PROJECT_ROOT
SRC_DIR = ROOT_DIR / "src"
sys.path.insert(0, str(SRC_DIR / "utilitarios"))

from conectar_oracle import conectar
from ler_sql import ler_sql
from salvar_para_parquet import salvar_para_parquet

def test_c170():
    cnpj = "37671507000187"
    sql_path = ROOT_DIR / "sql" / "fiscal" / "efd" / "c170.sql"
    
    print(f"Lendo {sql_path}...")
    sql_text = ler_sql(sql_path)
    
    binds = {
        "CNPJ": cnpj,
        "data_limite_processamento": None
    }
    
    print("Conectando ao Oracle...")
    conn = conectar()
    if not conn:
        print("Erro de conexao")
        return
        
    try:
        with conn.cursor() as cursor:
            print("Executando consulta...")
            cursor.execute(sql_text, binds)
            colunas = [col[0] for col in cursor.description]
            dados = cursor.fetchmany(10)
            
            if dados:
                df = pl.DataFrame(dados, schema=colunas, orient="row")
                print(f"Sucesso! Lidas {len(df)} linhas de teste.")
            else:
                print("Consulta retornou zero linhas (mas executou sem erro).")
                
    except Exception as e:
        print(f"Erro na execucao: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    test_c170()
