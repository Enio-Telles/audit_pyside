"""
MÃ³dulo de extraÃ§Ã£o de dados cadastrais do Oracle.

Este mÃ³dulo Ã© responsÃ¡vel por conectar ao banco Oracle, ler o arquivo SQL
de dados cadastrais e extrair as informaÃ§Ãµes para cada CNPJ fornecido.

Autor: Gerado automaticamente
Data: 2026-04-01
"""

import re
import os
import logging
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Any

# ConfiguraÃ§Ã£o do logging para rastrear execuÃ§Ãµes
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURAÃ‡Ã•ES DE CAMINHOS
# =============================================================================

# Importa resolvedor de caminhos do pacote integrado
from .path_resolver import get_resource_path, get_root_dir, get_env_path
from utilitarios.sql_catalog import resolve_sql_path

# DiretÃ³rio raiz do projeto Fisconforme_nao_atendidos
ROOT_DIR = get_root_dir()

# Arquivos SQL especÃ­ficos do catÃ¡logo canÃ´nico
SQL_DADOS_CADASTRAIS = resolve_sql_path("fisconforme/cadastro/dados_cadastrais.sql")
SQL_MALHA_CNPJ = resolve_sql_path("fisconforme/malhas/Fisconforme_malha_cnpj.sql")


def validar_cnpj(cnpj: str) -> bool:
    """
    Valida se um CNPJ Ã© vÃ¡lido numericamente.
    
    O algoritmo de validaÃ§Ã£o do CNPJ verifica os dois dÃ­gitos verificadores
    usando mÃ³dulo 11 com pesos especÃ­ficos.
    
    Args:
        cnpj: String contendo o CNPJ (pode conter formataÃ§Ã£o como pontos e traÃ§o)
    
    Returns:
        True se o CNPJ for vÃ¡lido, False caso contrÃ¡rio
    
    Exemplo:
        >>> validar_cnpj("12.345.678/0001-90")
        True ou False (dependendo da validade)
    """
    # Remove caracteres nÃ£o numÃ©ricos (pontos, traÃ§o, barra, espaÃ§os)
    cnpj_limpo = re.sub(r'[^0-9]', '', cnpj)
    
    # CNPJ deve ter exatamente 14 dÃ­gitos
    if len(cnpj_limpo) != 14:
        return False
    
    # Verifica se todos os dÃ­gitos sÃ£o iguais (caso especial invÃ¡lido)
    if len(set(cnpj_limpo)) == 1:
        return False
    
    # CÃ¡lculo do primeiro dÃ­gito verificador
    # Pesos para o primeiro dÃ­gito: 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2
    pesos_1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma_1 = sum(int(cnpj_limpo[i]) * pesos_1[i] for i in range(12))
    resto_1 = soma_1 % 11
    dv_1 = 0 if resto_1 < 2 else 11 - resto_1
    
    # CÃ¡lculo do segundo dÃ­gito verificador
    # Pesos para o segundo dÃ­gito: 6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2
    pesos_2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma_2 = sum(int(cnpj_limpo[i]) * pesos_2[i] for i in range(13))
    resto_2 = soma_2 % 11
    dv_2 = 0 if resto_2 < 2 else 11 - resto_2
    
    # Verifica se os dÃ­gitos verificadores calculados conferem com os informados
    return dv_1 == int(cnpj_limpo[12]) and dv_2 == int(cnpj_limpo[13])


def limpar_cnpj(cnpj: str) -> str:
    """
    Remove todos os caracteres nÃ£o numÃ©ricos de um CNPJ.
    
    Args:
        cnpj: String contendo o CNPJ (pode conter formataÃ§Ã£o)
    
    Returns:
        String contendo apenas os dÃ­gitos numÃ©ricos do CNPJ
    
    Exemplo:
        >>> limpar_cnpj("12.345.678/0001-90")
        '12345678000190'
    """
    return re.sub(r'[^0-9]', '', cnpj)


def ler_arquivo_sql(caminho_sql: Path) -> Optional[str]:
    """
    LÃª o conteÃºdo de um arquivo SQL.
    
    Args:
        caminho_sql: Caminho completo para o arquivo SQL
    
    Returns:
        ConteÃºdo do arquivo SQL como string, ou None se houver erro na leitura
    
    Raises:
        FileNotFoundError: Se o arquivo SQL nÃ£o existir
    """
    try:
        # Verifica se o arquivo existe
        if not caminho_sql.exists():
            logger.error(f"Arquivo SQL nÃ£o encontrado: {caminho_sql}")
            raise FileNotFoundError(f"Arquivo SQL nÃ£o encontrado: {caminho_sql}")
        
        # LÃª o arquivo com encoding UTF-8
        with open(caminho_sql, 'r', encoding='utf-8') as arquivo:
            conteudo = arquivo.read()
            
        # Remove espaÃ§os em branco e ponto e vÃ­rgula final (causa erro no driver Oracle)
        conteudo = conteudo.strip().rstrip(';')
        
        logger.info(f"Arquivo SQL lido com sucesso: {caminho_sql}")
        return conteudo
    
    except UnicodeDecodeError:
        # Tenta com encoding latin-1 como fallback
        logger.warning(f"Tentando encoding alternativo para: {caminho_sql}")
        with open(caminho_sql, 'r', encoding='latin-1') as arquivo:
            conteudo = arquivo.read()
            
        # Remove espaÃ§os em branco e ponto e vÃ­rgula final
        conteudo = conteudo.strip().rstrip(';')
        
        logger.info(f"Arquivo SQL lido com encoding latin-1: {caminho_sql}")
        return conteudo
    
    except Exception as e:
        logger.error(f"Erro ao ler arquivo SQL {caminho_sql}: {e}")
        return None


def conectar_oracle() -> Optional[Any]:
    """
    Estabelece conexÃ£o com o banco de dados Oracle.

    Utiliza as credenciais armazenadas no arquivo .env na raiz do projeto.
    Configura a sessÃ£o NLS para consistÃªncia nos formatos numÃ©ricos.

    Returns:
        Objeto de conexÃ£o Oracle em caso de sucesso, None em caso de falha

    Note:
        Requer que o pacote oracledb esteja instalado e configurado.
        As variÃ¡veis de ambiente necessÃ¡rias sÃ£o:
        - ORACLE_HOST: Host do servidor Oracle
        - ORACLE_PORT: Porta de conexÃ£o (padrÃ£o: 1521)
        - ORACLE_SERVICE: Nome do serviÃ§o (padrÃ£o: sefindw)
        - DB_USER: UsuÃ¡rio do banco
        - DB_PASSWORD: Senha do banco
    """
    try:
        import oracledb
        from dotenv import load_dotenv
        
        # Importa resolvedor de caminhos
        from .path_resolver import get_env_path

        # Carrega variÃ¡veis de ambiente do arquivo .env
        # Procura o .env na raiz do projeto Fisconforme_nao_atendidos
        env_path = get_env_path()
        if env_path.exists():
            load_dotenv(dotenv_path=env_path, encoding='latin-1', override=True)
        
        # ObtÃ©m credenciais das variÃ¡veis de ambiente
        host = os.getenv("ORACLE_HOST", 'exa01-scan.sefin.ro.gov.br').strip()
        porta = int(os.getenv("ORACLE_PORT", '1521').strip())
        servico = os.getenv("ORACLE_SERVICE", 'sefindw').strip()
        usuario = os.getenv("DB_USER")
        senha = os.getenv("DB_PASSWORD")
        
        # Valida credenciais obrigatÃ³rias
        if not usuario or not senha:
            logger.error("Credenciais do banco (DB_USER/DB_PASSWORD) nÃ£o encontradas no .env")
            return None
        
        # Cria string de conexÃ£o (DNS)
        dsn = oracledb.makedsn(host, porta, service_name=servico)
        
        # Estabelece conexÃ£o
        conexao = oracledb.connect(
            user=usuario.strip(),
            password=senha.strip(),
            dsn=dsn
        )
        
        # Configura sessÃ£o NLS para formato numÃ©rico brasileiro
        # Usa vÃ­rgula para decimais e ponto para milhares
        with conexao.cursor() as cursor:
            cursor.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'")
        
        logger.info("ConexÃ£o com Oracle estabelecida com sucesso")
        return conexao
    
    except ImportError:
        logger.error("Pacote oracledb nÃ£o instalado. Execute: pip install oracledb")
        return None
    
    except Exception as e:
        logger.error(f"Erro ao conectar ao Oracle: {e}")
        return None


def normalizar_texto_para_chave(texto: str) -> str:
    """
    Normaliza uma string para ser usada como chave de dicionÃ¡rio/placeholder.
    
    1. Remove acentos (ex: "Ã§" -> "c", "Ã£" -> "a")
    2. Substitui espaÃ§os e hÃ­fens por underscore ("_")
    3. Converte para maiÃºsculas (UPPERCASE)
    
    Args:
        texto: String a ser normalizada
        
    Returns:
        String normalizada
    """
    if not texto:
        return ""
    
    # Normaliza para forma NFD (decomposiÃ§Ã£o de caracteres acentuados)
    texto_norm = unicodedata.normalize('NFD', str(texto))
    # Filtra apenas caracteres que nÃ£o sejam marcas de acentuaÃ§Ã£o
    texto_sem_acento = "".join([c for c in texto_norm if unicodedata.category(c) != 'Mn'])
    
    # Substitui espaÃ§os, hÃ­fens e pontos por underscore
    texto_limpo = re.sub(r'[\s\-.]+', '_', texto_sem_acento)
    
    # Converte para maiÃºsculas e remove caracteres nÃ£o alfanumÃ©ricos (exceto _)
    chave = re.sub(r'[^A-Z0-9_]', '', texto_limpo.upper())
    
    # Remove underscores duplicados e nas extremidades
    chave = re.sub(r'_+', '_', chave).strip('_')
    
    return chave


def extrair_dados_cadastrais(cnpj: str) -> Optional[Dict[str, Any]]:
    """
    Extrai dados cadastrais de um CNPJ especÃ­fico do banco Oracle.
    
    Executa a query SQL definida em dados_cadastrais.sql, substituindo
    o bind variable :CO_CNPJ_CPF pelo CNPJ fornecido.
    
    Args:
        cnpj: CNPJ da empresa (pode estar formatado ou nÃ£o)
    
    Returns:
        DicionÃ¡rio com os dados cadastrais se encontrados, None caso contrÃ¡rio.
        As chaves sÃ£o normalizadas para MAIÃšSCULAS e SEM ACENTOS.
    """
    # Valida e limpa o CNPJ
    if not validar_cnpj(cnpj):
        raise ValueError(f"CNPJ invÃ¡lido: {cnpj}")
    
    cnpj_limpo = limpar_cnpj(cnpj)
    logger.info(f"Iniciando extraÃ§Ã£o de dados para CNPJ: {cnpj_limpo}")
    
    # LÃª o arquivo SQL
    sql_conteudo = ler_arquivo_sql(SQL_DADOS_CADASTRAIS)
    if not sql_conteudo:
        raise ConnectionError(f"NÃ£o foi possÃ­vel ler o arquivo SQL: {SQL_DADOS_CADASTRAIS}")
    
    # Conecta ao banco
    conexao = conectar_oracle()
    if not conexao:
        raise ConnectionError("Falha ao conectar com o banco de dados Oracle")
    
    try:
        # Cria cursor para execuÃ§Ã£o da query
        with conexao.cursor() as cursor:
            cursor.arraysize = 100
            cursor.prepare(sql_conteudo)
            cursor.execute(None, {"cnpj": cnpj_limpo})
            
            # ObtÃ©m nomes das colunas originais
            colunas_raw = [col[0] for col in cursor.description]
            
            # Busca resultados
            dados = cursor.fetchall()
            
            if not dados:
                logger.warning(f"Nenhum dado encontrado para o CNPJ: {cnpj_limpo}")
                return None
            
            # Pega a primeira linha
            linha = dados[0]
            
            # Monta o dicionÃ¡rio com chaves normalizadas
            dados_normalizados = {}
            for i, valor in enumerate(linha):
                chave_orig = colunas_raw[i]
                chave_norm = normalizar_texto_para_chave(chave_orig)
                
                # Trata valor e remove espaÃ§os extras de strings
                if valor is None:
                    dados_normalizados[chave_norm] = ""
                elif isinstance(valor, str):
                    dados_normalizados[chave_norm] = valor.strip()
                else:
                    dados_normalizados[chave_norm] = valor
            
            logger.info(f"Dados cadastrais normalizados para {cnpj_limpo}: {list(dados_normalizados.keys())}")
            return dados_normalizados
    
    except Exception as e:
        logger.error(f"Erro ao extrair dados do CNPJ {cnpj_limpo}: {e}")
        return None
    
    finally:
        # Sempre fecha a conexÃ£o, mesmo em caso de erro
        try:
            conexao.close()
            logger.info("ConexÃ£o com Oracle fechada")
        except Exception as e:
            logger.warning(f"Erro ao fechar conexÃ£o: {e}")


def extrair_dados_multiplos_cnpjs(cnpjs: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Extrai dados cadastrais para mÃºltiplos CNPJs.
    
    Args:
        cnpjs: Lista de CNPJs para extraÃ§Ã£o
    
    Returns:
        DicionÃ¡rio onde as chaves sÃ£o os CNPJs limpos e os valores sÃ£o
        os dicionÃ¡rios de dados cadastrais retornados por extrair_dados_cadastrais
    
    Exemplo:
        >>> extrair_dados_multiplos_cnpjs(["12.345.678/0001-90", "98.765.432/0001-10"])
        {
            "12345678000190": {"CNPJ": "...", "RAZAO_SOCIAL": "...", ...},
            "98765432000110": {"CNPJ": "...", "RAZAO_SOCIAL": "...", ...}
        }
    """
    resultados = {}
    
    for cnpj in cnpjs:
        try:
            cnpj_limpo = limpar_cnpj(cnpj)
            dados = extrair_dados_cadastrais(cnpj)
            
            if dados:
                resultados[cnpj_limpo] = dados
                logger.info(f"ExtraÃ§Ã£o concluÃ­da para CNPJ: {cnpj_limpo}")
            else:
                logger.warning(f"Nenhum dado retornado para CNPJ: {cnpj_limpo}")
        
        except Exception as e:
            logger.error(f"Falha na extraÃ§Ã£o do CNPJ {cnpj}: {e}")
            # Continua para o prÃ³ximo CNPJ mesmo em caso de erro
    
    return resultados

def extrair_dados_malha(cnpj: str, data_inicio: str = None, data_fim: str = None) -> List[Dict[str, Any]]:
    """
    Extrai dados de pendÃªncias de malha para um CNPJ especÃ­fico do banco Oracle.
    
    Executa a query SQL definida em Fisconforme_malha_cnpj.sql.
    
    Args:
        cnpj: CNPJ da empresa (formato numÃ©rico ou com mÃ¡scara)
        data_inicio: InÃ­cio do perÃ­odo (MM/AAAA)
        data_fim: Fim do perÃ­odo (MM/AAAA)
    
    Returns:
        Lista de dicionÃ¡rios, cada um representando uma pendÃªncia.
    """
    if not validar_cnpj(cnpj):
        logger.warning(f"CNPJ invÃ¡lido para extraÃ§Ã£o de malha: {cnpj}")
        return []
    
    cnpj_limpo = limpar_cnpj(cnpj)
    logger.info(f"Extraindo dados de malha para CNPJ: {cnpj_limpo} (PerÃ­odo: {data_inicio} a {data_fim})")
    
    # Normaliza perÃ­odos MM/YYYY -> YYYYMM para o Oracle
    d_ini = "190001" # Valor padrÃ£o remoto
    if data_inicio and "/" in data_inicio:
        try:
            m, y = data_inicio.split("/")
            d_ini = f"{y.strip()}{m.strip().zfill(2)}"
        except: pass
        
    d_fim = "209912" # Valor padrÃ£o remoto
    if data_fim and "/" in data_fim:
        try:
            m, y = data_fim.split("/")
            d_fim = f"{y.strip()}{m.strip().zfill(2)}"
        except: pass

    # LÃª o arquivo SQL
    sql_conteudo = ler_arquivo_sql(SQL_MALHA_CNPJ)
    if not sql_conteudo:
        logger.error(f"NÃ£o foi possÃ­vel ler o arquivo SQL: {SQL_MALHA_CNPJ}")
        return []
    
    # Conecta ao banco
    conexao = conectar_oracle()
    if not conexao:
        logger.error("Falha ao conectar com o banco de dados Oracle para extraÃ§Ã£o de malha")
        return []
    
    try:
        with conexao.cursor() as cursor:
            cursor.arraysize = 50
            # Prepara parÃ¢metros com bind variables
            params = {
                "cnpj": cnpj_limpo,
                "data_inicio": d_ini,
                "data_fim": d_fim
            }
            cursor.execute(sql_conteudo, params)
            
            # ObtÃ©m nomes das colunas (mantendo o que vem do SQL)
            colunas = [col[0].lower() for col in cursor.description]
            
            resultados = []
            for linha in cursor.fetchall():
                # Cria dicionÃ¡rio para a linha
                item = dict(zip(colunas, linha))
                
                # Normaliza valores nulos para string vazia
                for k, v in item.items():
                    if v is None:
                        item[k] = ""
                    elif isinstance(v, str):
                        item[k] = v.strip()
                
                resultados.append(item)
            
            logger.info(f"ExtraÃ­dos {len(resultados)} registros de malha para {cnpj_limpo}")
            return resultados
            
    except Exception as e:
        logger.error(f"Erro ao extrair dados de malha para CNPJ {cnpj_limpo}: {e}")
        return []
    
    finally:
        try:
            conexao.close()
        except:
            pass



# =============================================================================
# PONTO DE ENTRADA PRINCIPAL (para testes)
# =============================================================================

if __name__ == "__main__":
    # Exemplo de uso do mÃ³dulo
    print("=" * 60)
    print("MÃ³dulo de ExtraÃ§Ã£o de Dados Cadastrais - Oracle")
    print("=" * 60)
    
    # CNPJ de exemplo para teste
    cnpj_teste = input("\nInforme o CNPJ para extraÃ§Ã£o (ou Enter para sair): ").strip()
    
    if cnpj_teste:
        print(f"\nExtraindo dados para: {cnpj_teste}")
        print("-" * 60)
        
        try:
            dados = extrair_dados_cadastrais(cnpj_teste)
            
            if dados:
                print("\nDados extraÃ­dos com sucesso!")
                print("\nCampos encontrados:")
                for chave, valor in dados.items():
                    print(f"  {chave}: {valor}")
            else:
                print("\nNenhum dado encontrado para este CNPJ.")
        
        except Exception as e:
            print(f"\nErro durante a extraÃ§Ã£o: {e}")
    
    print("\n" + "=" * 60)
    print("Processamento concluÃ­do.")

