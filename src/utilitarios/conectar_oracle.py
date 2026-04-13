"""
Módulo de conexão Oracle.
Usa credenciais do arquivo .env
Implementa Context Manager para evitar vazamentos de recursos.
"""
import os
import logging
import socket
import oracledb
from pathlib import Path
from dotenv import load_dotenv
from contextlib import contextmanager
from rich import print as rprint

# Localização do .env (Project Root)
env_path = Path(__file__).parent.parent.parent / '.env'
load_dotenv(dotenv_path=env_path, encoding='latin-1', override=True)

# Configurações do Banco (Audit: Strict Validation)
def _get_required_env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise EnvironmentError(f"❌ Variável de ambiente OBRIGATÓRIA não encontrada: {key}")
    return val.strip()

try:
    HOST = _get_required_env("ORACLE_HOST")
    PORTA = int(_get_required_env("ORACLE_PORT"))
    SERVICO = _get_required_env("ORACLE_SERVICE")
except Exception as e:
    rprint("[red]Erro na configuração das variáveis de rede Oracle.[/red]")
    HOST, PORTA, SERVICO = None, None, None

def conectar(cpf_usuario=None, senha=None):
    """
    Função legada para compatibilidade. 
    Recomendado usar 'obter_conexao_oracle' como context manager.
    """
    if cpf_usuario is None: cpf_usuario = os.getenv("DB_USER")
    if senha is None: senha = os.getenv("DB_PASSWORD")
    
    if not cpf_usuario or not senha:
        rprint("[red]Erro:[/red] Credenciais (DB_USER/DB_PASSWORD) não encontradas no .env")
        return None
    
    try:
        dsn = oracledb.makedsn(HOST, PORTA, service_name=SERVICO)
        conexao = oracledb.connect(user=cpf_usuario.strip(), password=senha.strip(), dsn=dsn)
        
        # Configuração de NLS para consistência decimal
        with conexao.cursor() as cursor:
            cursor.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'")
            
        return conexao
    except Exception as e:
        rprint("[red]Erro de conexão Oracle:[/red] Falha ao estabelecer conexão com o banco de dados. Verifique suas credenciais e configurações de rede.")
        return None

@contextmanager
def obter_conexao_oracle(user=None, password=None):
    """
    Context Manager conforme Recomendação de Auditoria.
    Garante que a conexão seja fechada ou liberada (pool).
    """
    conn = conectar(user, password)
    if conn is None:
        raise ConnectionError("Não foi possível estabelecer conexão com o Oracle.")
    try:
        yield conn
    finally:
        try:
            conn.close()
            # rprint("[blue]DEBUG: Conexão Oracle encerrada com segurança.[/blue]")
        except Exception as e:
            rprint("[yellow]Aviso:[/yellow] Falha ao encerrar a conexão Oracle com segurança.")

if __name__ == "__main__":    
    try:
        with obter_conexao_oracle() as conn:
            rprint("[green]Conexão via Context Manager estabelecida com sucesso![/green]")
    except Exception as e:
        rprint("[red]Falha no teste de conexão:[/red] Não foi possível estabelecer conexão com o banco de dados.")
