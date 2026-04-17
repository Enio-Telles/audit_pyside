"""
MÃ³dulo de extraÃ§Ã£o de dados cadastrais com cache Parquet.

Utiliza as consultas SQL da pasta sql/ para extrair dados do Oracle DW
e salva em formato Parquet para reutilizaÃ§Ã£o e performance.

Autor: Pipeline Fisconforme
Data: 2026-04-03
"""

import os
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

import polars as pl

# ConfiguraÃ§Ã£o de logging
logger = logging.getLogger(__name__)

# Importa resolvedor de caminhos do pacote integrado
from .path_resolver import get_root_dir, get_env_path
from utilitarios.sql_catalog import resolve_sql_path

# DiretÃ³rio raiz do projeto
ROOT_DIR = get_root_dir()

# DiretÃ³rios
PARQUET_DIR = ROOT_DIR / "dados" / "fisconforme" / "data_parquet"
PARQUET_DIR.mkdir(parents=True, exist_ok=True)

# Arquivos SQL
SQL_DADOS_CADASTRAIS = resolve_sql_path("dados_cadastrais.sql")
SQL_MALHA_CNPJ = resolve_sql_path("Fisconforme_malha_cnpj.sql")

# Arquivos Parquet de cache
PARQUET_CADASTRAIS = PARQUET_DIR / "dados_cadastrais.parquet"
PARQUET_MALHAS = PARQUET_DIR / "malhas_pendencias.parquet"


def ler_sql(caminho_sql: Path) -> Optional[str]:
    """
    LÃª o conteÃºdo de um arquivo SQL.

    Args:
        caminho_sql: Caminho para o arquivo SQL

    Returns:
        ConteÃºdo do SQL ou None se erro
    """
    try:
        if not caminho_sql.exists():
            logger.error(f"Arquivo SQL nÃ£o encontrado: {caminho_sql}")
            return None

        with open(caminho_sql, "r", encoding="utf-8") as f:
            conteudo = f.read().strip().rstrip(";")

        logger.info(f"SQL lido: {caminho_sql.name}")
        return conteudo

    except Exception as e:
        logger.error(f"Erro ao ler SQL {caminho_sql}: {e}")
        return None


def conectar_oracle_simples():
    """
    Cria conexÃ£o simples com Oracle (sem pool).

    Returns:
        ConexÃ£o Oracle ou None
    """
    try:
        import oracledb
        from dotenv import load_dotenv

        env_path = get_env_path()
        if env_path.exists():
            load_dotenv(dotenv_path=env_path, encoding="latin-1", override=True)

        host = os.getenv("ORACLE_HOST", "").strip()
        porta = int(os.getenv("ORACLE_PORT", "1521").strip())
        servico = os.getenv("ORACLE_SERVICE", "").strip()
        usuario = os.getenv("DB_USER", "").strip()
        senha = os.getenv("DB_PASSWORD", "").strip()

        if not all([host, usuario, senha]):
            logger.error("Credenciais Oracle incompletas no .env")
            return None

        dsn = oracledb.makedsn(host, porta, service_name=servico)
        conexao = oracledb.connect(user=usuario, password=senha, dsn=dsn)

        with conexao.cursor() as cursor:
            cursor.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'")

        return conexao

    except Exception as e:
        logger.error(f"Erro ao conectar Oracle: {e}")
        return None


def extrair_dados_cadastrais_oracle(cnpj: str) -> Optional[Dict[str, Any]]:
    """
    Extrai dados cadastrais de um CNPJ usando o SQL da pasta sql/.

    Args:
        cnpj: CNPJ limpo (apenas nÃºmeros)

    Returns:
        DicionÃ¡rio com dados cadastrais ou None
    """
    sql = ler_sql(SQL_DADOS_CADASTRAIS)
    if not sql:
        return None

    conexao = conectar_oracle_simples()
    if not conexao:
        return None

    try:
        with conexao.cursor() as cursor:
            cursor.arraysize = 10
            cursor.execute(sql, {"cnpj": cnpj})

            colunas = [col[0] for col in cursor.description]
            resultado = cursor.fetchone()

            if not resultado:
                logger.warning(f"Nenhum dado encontrado para CNPJ {cnpj}")
                return None

            # Converte para dicionÃ¡rio
            dados = {}
            for i, valor in enumerate(resultado):
                chave = colunas[i].upper().replace(" ", "_")
                if valor is None:
                    dados[chave] = ""
                elif isinstance(valor, str):
                    dados[chave] = valor.strip()
                else:
                    dados[chave] = valor

            logger.info(f"Dados cadastrais extraÃ­dos para CNPJ {cnpj}")
            return dados

    except Exception as e:
        logger.error(f"Erro ao extrair dados cadastrais para {cnpj}: {e}")
        return None

    finally:
        try:
            conexao.close()
        except:
            pass


def salvar_cache_cadastral(cnpj: str, dados: Dict[str, Any]):
    """
    Salva dados cadastrais no cache Parquet (append ou update).

    Args:
        cnpj: CNPJ limpo
        dados: DicionÃ¡rio com dados cadastrais
    """
    try:
        # Cria DataFrame com os dados
        df_novo = pl.DataFrame(
            {
                "CNPJ": [cnpj],
                "RAZAO_SOCIAL": [dados.get("RAZAO_SOCIAL", "")],
                "NOME_FANTASIA": [dados.get("NOME_FANTASIA", "")],
                "IE": [dados.get("IE", "")],
                "ENDERECO": [dados.get("ENDERECO", "")],
                "MUNICIPIO": [dados.get("MUNICIPIO", "")],
                "UF": [dados.get("UF", "")],
                "REGIME_DE_PAGAMENTO": [dados.get("REGIME_DE_PAGAMENTO", "")],
                "SITUACAO_DA_IE": [dados.get("SITUACAO_DA_IE", "")],
                "DATA_INICIO_ATIVIDADE": [
                    str(dados.get("DATA_DE_INICIO_DA_ATIVIDADE", ""))
                ],
                "DATA_ULTIMA_SITUACAO": [str(dados.get("DATA_DA_ULTIMA_SITUACAO", ""))],
                "PERIODO_EM_ATIVIDADE": [dados.get("PERIODO_EM_ATIVIDADE", "")],
                "REDESIM": [dados.get("REDESIM", "")],
                "DATA_EXTRACTION": [datetime.now().isoformat()],
            }
        )

        # Append ou cria novo
        if PARQUET_CADASTRAIS.exists():
            df_existente = pl.read_parquet(PARQUET_CADASTRAIS)
            # Remove entrada antiga do mesmo CNPJ
            df_existente = df_existente.filter(pl.col("CNPJ") != cnpj)
            df_final = pl.concat([df_existente, df_novo], how="diagonal")
        else:
            df_final = df_novo

        df_final.write_parquet(PARQUET_CADASTRAIS)
        logger.info(f"Cache Parquet salvo para CNPJ {cnpj}")

    except Exception as e:
        logger.error(f"Erro ao salvar cache Parquet para {cnpj}: {e}")


def buscar_cache_cadastral(cnpj: str) -> Optional[Dict[str, Any]]:
    """
    Busca dados cadastrais no cache Parquet.

    Args:
        cnpj: CNPJ limpo

    Returns:
        DicionÃ¡rio com dados ou None
    """
    try:
        if not PARQUET_CADASTRAIS.exists():
            return None

        df = (
            pl.scan_parquet(PARQUET_CADASTRAIS).filter(pl.col("CNPJ") == cnpj).collect()
        )

        if df.is_empty():
            return None

        # Converte para dicionÃ¡rio
        row = df.row(0, named=True)
        dados = {}
        for chave, valor in row.items():
            if valor is None:
                dados[chave] = ""
            elif isinstance(valor, str):
                dados[chave] = valor.strip()
            else:
                dados[chave] = valor

        logger.info(f"Cache Parquet encontrado para CNPJ {cnpj}")
        return dados

    except Exception as e:
        logger.warning(f"Erro ao buscar cache Parquet para {cnpj}: {e}")
        return None


def extrair_e_salvar_cadastral(cnpj: str) -> Optional[Dict[str, Any]]:
    """
    Fluxo completo: busca no cache, se nÃ£o encontrar extrai do Oracle e salva.

    Args:
        cnpj: CNPJ limpo

    Returns:
        DicionÃ¡rio com dados cadastrais ou None
    """
    # 1. Busca no cache Parquet
    dados = buscar_cache_cadastral(cnpj)
    if dados:
        dados["_FROM_PARQUET"] = True
        return dados

    # 2. Extrai do Oracle
    dados = extrair_dados_cadastrais_oracle(cnpj)
    if dados:
        # 3. Salva no cache
        salvar_cache_cadastral(cnpj, dados)
        dados["_FROM_PARQUET"] = False
        return dados

    return None


def extrair_multiplos_cnpjs(cnpjs: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Extrai dados cadastrais para mÃºltiplos CNPJs.

    Args:
        cnpjs: Lista de CNPJs limpos

    Returns:
        DicionÃ¡rio {cnpj: dados}
    """
    resultados = {}

    for cnpj in cnpjs:
        try:
            dados = extrair_e_salvar_cadastral(cnpj)
            if dados:
                resultados[cnpj] = dados
        except Exception as e:
            logger.error(f"Falha ao extrair CNPJ {cnpj}: {e}")

    return resultados


def exportar_cache_completo(caminho_saida: Optional[Path] = None) -> Optional[Path]:
    """
    Exporta todo o cache cadastral para um arquivo Parquet.

    Args:
        caminho_saida: Caminho de saÃ­da (opcional)

    Returns:
        Caminho do arquivo exportado ou None
    """
    try:
        if not PARQUET_CADASTRAIS.exists():
            logger.warning("Cache cadastral vazio")
            return None

        if not caminho_saida:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            caminho_saida = PARQUET_DIR / f"dados_cadastrais_export_{timestamp}.parquet"

        df = pl.read_parquet(PARQUET_CADASTRAIS)
        df.write_parquet(caminho_saida)

        logger.info(f"Cache exportado: {caminho_saida} ({len(df)} linhas)")
        return caminho_saida

    except Exception as e:
        logger.error(f"Erro ao exportar cache: {e}")
        return None


def limpar_cache_cadastral():
    """Remove o arquivo de cache cadastral."""
    try:
        if PARQUET_CADASTRAIS.exists():
            PARQUET_CADASTRAIS.unlink()
            logger.info("Cache cadastral limpo")
            return True
        return False
    except Exception as e:
        logger.error(f"Erro ao limpar cache: {e}")
        return False


def obter_estatisticas_cache() -> Dict[str, Any]:
    """
    ObtÃ©m estatÃ­sticas do cache Parquet.

    Returns:
        DicionÃ¡rio com estatÃ­sticas
    """
    try:
        if not PARQUET_CADASTRAIS.exists():
            return {"total": 0, "arquivo": str(PARQUET_CADASTRAIS)}

        df = pl.read_parquet(PARQUET_CADASTRAIS)
        tamanho_mb = PARQUET_CADASTRAIS.stat().st_size / (1024 * 1024)

        return {
            "total": len(df),
            "arquivo": str(PARQUET_CADASTRAIS),
            "tamanho_mb": round(tamanho_mb, 2),
            "colunas": df.columns,
        }

    except Exception as e:
        logger.error(f"Erro ao obter estatÃ­sticas: {e}")
        return {"total": 0, "erro": str(e)}
