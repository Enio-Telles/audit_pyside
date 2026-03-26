import re

with open("src/extracao/extrair_dados_cnpj.py", "r") as f:
    content = f.read()

# Add logging import
content = content.replace("import threading\n", "import threading\nimport logging\n\nlogger = logging.getLogger(__name__)\n")

# Replace get_thread_connection
new_get_thread_connection = """def get_thread_connection():
    if not hasattr(thread_local, "conexao"):
        # Cria uma nova conexão para esta thread
        conn = conectar_oracle()
        if conn is None:
            logger.error(f"[{threading.current_thread().name}] Falha ao criar conexão com banco de dados.")
            return None

        try:
            # Testar a conexão
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
        except Exception as e:
            logger.error(f"[{threading.current_thread().name}] Erro ao testar conexão: {e}")
            return None

        thread_local.conexao = conn
    return thread_local.conexao"""

content = re.sub(r'def get_thread_connection\(\):\n(.*?)\n    return thread_local\.conexao', new_get_thread_connection, content, flags=re.DOTALL)

# Update imports block
new_imports = """try:
    from utilitarios.conectar_oracle import conectar, conectar as conectar_oracle
    from utilitarios.ler_sql import ler_sql
    from utilitarios.salvar_para_parquet import salvar_para_parquet
    from utilitarios.validar_cnpj import validar_cnpj
except ImportError as e:
    rprint(f"[red]Erro ao importar módulos utilitários:[/red] {e}")
    sys.exit(1)"""

content = re.sub(r'try:\n    from utilitarios\.conectar_oracle import conectar\n    from utilitarios\.ler_sql import ler_sql\n    from utilitarios\.salvar_para_parquet import salvar_para_parquet\n    from utilitarios\.validar_cnpj import validar_cnpj\nexcept ImportError as e:\n    rprint\(f"\[red\]Erro ao importar módulos utilitários:\[/red\] \{e\}"\)\n    sys\.exit\(1\)', new_imports, content, flags=re.DOTALL)


with open("src/extracao/extrair_dados_cnpj.py", "w") as f:
    f.write(content)
