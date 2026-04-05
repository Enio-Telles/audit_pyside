"""
Módulo de extração de dados cadastrais do Oracle.

Este módulo é responsável por conectar ao banco Oracle, ler o arquivo SQL
de dados cadastrais e extrair as informações para cada CNPJ fornecido.

Autor: Gerado automaticamente
Data: 2026-04-01
"""

import re
import os
import logging
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Any

# Configuração do logging para rastrear execuções
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURAÇÕES DE CAMINHOS
# =============================================================================

# Importa resolvedor de caminhos do pacote integrado
from .path_resolver import get_resource_path, get_root_dir, get_env_path

# Diretório raiz do projeto Fisconforme_nao_atendidos
ROOT_DIR = get_root_dir()

# Diretório onde estão os arquivos SQL
SQL_DIR = ROOT_DIR / "sql"

# Arquivo SQL específico para dados cadastrais
SQL_DADOS_CADASTRAIS = SQL_DIR / "dados_cadastrais.sql"

# Arquivo SQL específico para pendências de malha por CNPJ
SQL_MALHA_CNPJ = SQL_DIR / "Fisconforme_malha_cnpj.sql"


def validar_cnpj(cnpj: str) -> bool:
    """
    Valida se um CNPJ é válido numericamente.
    
    O algoritmo de validação do CNPJ verifica os dois dígitos verificadores
    usando módulo 11 com pesos específicos.
    
    Args:
        cnpj: String contendo o CNPJ (pode conter formatação como pontos e traço)
    
    Returns:
        True se o CNPJ for válido, False caso contrário
    
    Exemplo:
        >>> validar_cnpj("12.345.678/0001-90")
        True ou False (dependendo da validade)
    """
    # Remove caracteres não numéricos (pontos, traço, barra, espaços)
    cnpj_limpo = re.sub(r'[^0-9]', '', cnpj)
    
    # CNPJ deve ter exatamente 14 dígitos
    if len(cnpj_limpo) != 14:
        return False
    
    # Verifica se todos os dígitos são iguais (caso especial inválido)
    if len(set(cnpj_limpo)) == 1:
        return False
    
    # Cálculo do primeiro dígito verificador
    # Pesos para o primeiro dígito: 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2
    pesos_1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma_1 = sum(int(cnpj_limpo[i]) * pesos_1[i] for i in range(12))
    resto_1 = soma_1 % 11
    dv_1 = 0 if resto_1 < 2 else 11 - resto_1
    
    # Cálculo do segundo dígito verificador
    # Pesos para o segundo dígito: 6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2
    pesos_2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma_2 = sum(int(cnpj_limpo[i]) * pesos_2[i] for i in range(13))
    resto_2 = soma_2 % 11
    dv_2 = 0 if resto_2 < 2 else 11 - resto_2
    
    # Verifica se os dígitos verificadores calculados conferem com os informados
    return dv_1 == int(cnpj_limpo[12]) and dv_2 == int(cnpj_limpo[13])


def limpar_cnpj(cnpj: str) -> str:
    """
    Remove todos os caracteres não numéricos de um CNPJ.
    
    Args:
        cnpj: String contendo o CNPJ (pode conter formatação)
    
    Returns:
        String contendo apenas os dígitos numéricos do CNPJ
    
    Exemplo:
        >>> limpar_cnpj("12.345.678/0001-90")
        '12345678000190'
    """
    return re.sub(r'[^0-9]', '', cnpj)


def ler_arquivo_sql(caminho_sql: Path) -> Optional[str]:
    """
    Lê o conteúdo de um arquivo SQL.
    
    Args:
        caminho_sql: Caminho completo para o arquivo SQL
    
    Returns:
        Conteúdo do arquivo SQL como string, ou None se houver erro na leitura
    
    Raises:
        FileNotFoundError: Se o arquivo SQL não existir
    """
    try:
        # Verifica se o arquivo existe
        if not caminho_sql.exists():
            logger.error(f"Arquivo SQL não encontrado: {caminho_sql}")
            raise FileNotFoundError(f"Arquivo SQL não encontrado: {caminho_sql}")
        
        # Lê o arquivo com encoding UTF-8
        with open(caminho_sql, 'r', encoding='utf-8') as arquivo:
            conteudo = arquivo.read()
            
        # Remove espaços em branco e ponto e vírgula final (causa erro no driver Oracle)
        conteudo = conteudo.strip().rstrip(';')
        
        logger.info(f"Arquivo SQL lido com sucesso: {caminho_sql}")
        return conteudo
    
    except UnicodeDecodeError:
        # Tenta com encoding latin-1 como fallback
        logger.warning(f"Tentando encoding alternativo para: {caminho_sql}")
        with open(caminho_sql, 'r', encoding='latin-1') as arquivo:
            conteudo = arquivo.read()
            
        # Remove espaços em branco e ponto e vírgula final
        conteudo = conteudo.strip().rstrip(';')
        
        logger.info(f"Arquivo SQL lido com encoding latin-1: {caminho_sql}")
        return conteudo
    
    except Exception as e:
        logger.error(f"Erro ao ler arquivo SQL {caminho_sql}: {e}")
        return None


def conectar_oracle() -> Optional[Any]:
    """
    Estabelece conexão com o banco de dados Oracle.

    Utiliza as credenciais armazenadas no arquivo .env na raiz do projeto.
    Configura a sessão NLS para consistência nos formatos numéricos.

    Returns:
        Objeto de conexão Oracle em caso de sucesso, None em caso de falha

    Note:
        Requer que o pacote oracledb esteja instalado e configurado.
        As variáveis de ambiente necessárias são:
        - ORACLE_HOST: Host do servidor Oracle
        - ORACLE_PORT: Porta de conexão (padrão: 1521)
        - ORACLE_SERVICE: Nome do serviço (padrão: sefindw)
        - DB_USER: Usuário do banco
        - DB_PASSWORD: Senha do banco
    """
    try:
        import oracledb
        from dotenv import load_dotenv
        
        # Importa resolvedor de caminhos
        from .path_resolver import get_env_path

        # Carrega variáveis de ambiente do arquivo .env
        # Procura o .env na raiz do projeto Fisconforme_nao_atendidos
        env_path = get_env_path()
        if env_path.exists():
            load_dotenv(dotenv_path=env_path, encoding='latin-1', override=True)
        
        # Obtém credenciais das variáveis de ambiente
        host = os.getenv("ORACLE_HOST", 'exa01-scan.sefin.ro.gov.br').strip()
        porta = int(os.getenv("ORACLE_PORT", '1521').strip())
        servico = os.getenv("ORACLE_SERVICE", 'sefindw').strip()
        usuario = os.getenv("DB_USER")
        senha = os.getenv("DB_PASSWORD")
        
        # Valida credenciais obrigatórias
        if not usuario or not senha:
            logger.error("Credenciais do banco (DB_USER/DB_PASSWORD) não encontradas no .env")
            return None
        
        # Cria string de conexão (DNS)
        dsn = oracledb.makedsn(host, porta, service_name=servico)
        
        # Estabelece conexão
        conexao = oracledb.connect(
            user=usuario.strip(),
            password=senha.strip(),
            dsn=dsn
        )
        
        # Configura sessão NLS para formato numérico brasileiro
        # Usa vírgula para decimais e ponto para milhares
        with conexao.cursor() as cursor:
            cursor.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'")
        
        logger.info("Conexão com Oracle estabelecida com sucesso")
        return conexao
    
    except ImportError:
        logger.error("Pacote oracledb não instalado. Execute: pip install oracledb")
        return None
    
    except Exception as e:
        logger.error(f"Erro ao conectar ao Oracle: {e}")
        return None


def normalizar_texto_para_chave(texto: str) -> str:
    """
    Normaliza uma string para ser usada como chave de dicionário/placeholder.
    
    1. Remove acentos (ex: "ç" -> "c", "ã" -> "a")
    2. Substitui espaços e hífens por underscore ("_")
    3. Converte para maiúsculas (UPPERCASE)
    
    Args:
        texto: String a ser normalizada
        
    Returns:
        String normalizada
    """
    if not texto:
        return ""
    
    # Normaliza para forma NFD (decomposição de caracteres acentuados)
    texto_norm = unicodedata.normalize('NFD', str(texto))
    # Filtra apenas caracteres que não sejam marcas de acentuação
    texto_sem_acento = "".join([c for c in texto_norm if unicodedata.category(c) != 'Mn'])
    
    # Substitui espaços, hífens e pontos por underscore
    texto_limpo = re.sub(r'[\s\-.]+', '_', texto_sem_acento)
    
    # Converte para maiúsculas e remove caracteres não alfanuméricos (exceto _)
    chave = re.sub(r'[^A-Z0-9_]', '', texto_limpo.upper())
    
    # Remove underscores duplicados e nas extremidades
    chave = re.sub(r'_+', '_', chave).strip('_')
    
    return chave


def extrair_dados_cadastrais(cnpj: str) -> Optional[Dict[str, Any]]:
    """
    Extrai dados cadastrais de um CNPJ específico do banco Oracle.
    
    Executa a query SQL definida em dados_cadastrais.sql, substituindo
    o bind variable :CO_CNPJ_CPF pelo CNPJ fornecido.
    
    Args:
        cnpj: CNPJ da empresa (pode estar formatado ou não)
    
    Returns:
        Dicionário com os dados cadastrais se encontrados, None caso contrário.
        As chaves são normalizadas para MAIÚSCULAS e SEM ACENTOS.
    """
    # Valida e limpa o CNPJ
    if not validar_cnpj(cnpj):
        raise ValueError(f"CNPJ inválido: {cnpj}")
    
    cnpj_limpo = limpar_cnpj(cnpj)
    logger.info(f"Iniciando extração de dados para CNPJ: {cnpj_limpo}")
    
    # Lê o arquivo SQL
    sql_conteudo = ler_arquivo_sql(SQL_DADOS_CADASTRAIS)
    if not sql_conteudo:
        raise ConnectionError(f"Não foi possível ler o arquivo SQL: {SQL_DADOS_CADASTRAIS}")
    
    # Conecta ao banco
    conexao = conectar_oracle()
    if not conexao:
        raise ConnectionError("Falha ao conectar com o banco de dados Oracle")
    
    try:
        # Cria cursor para execução da query
        with conexao.cursor() as cursor:
            cursor.arraysize = 100
            cursor.prepare(sql_conteudo)
            cursor.execute(None, {"cnpj": cnpj_limpo})
            
            # Obtém nomes das colunas originais
            colunas_raw = [col[0] for col in cursor.description]
            
            # Busca resultados
            dados = cursor.fetchall()
            
            if not dados:
                logger.warning(f"Nenhum dado encontrado para o CNPJ: {cnpj_limpo}")
                return None
            
            # Pega a primeira linha
            linha = dados[0]
            
            # Monta o dicionário com chaves normalizadas
            dados_normalizados = {}
            for i, valor in enumerate(linha):
                chave_orig = colunas_raw[i]
                chave_norm = normalizar_texto_para_chave(chave_orig)
                
                # Trata valor e remove espaços extras de strings
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
        # Sempre fecha a conexão, mesmo em caso de erro
        try:
            conexao.close()
            logger.info("Conexão com Oracle fechada")
        except Exception as e:
            logger.warning(f"Erro ao fechar conexão: {e}")


def extrair_dados_multiplos_cnpjs(cnpjs: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Extrai dados cadastrais para múltiplos CNPJs.
    
    Args:
        cnpjs: Lista de CNPJs para extração
    
    Returns:
        Dicionário onde as chaves são os CNPJs limpos e os valores são
        os dicionários de dados cadastrais retornados por extrair_dados_cadastrais
    
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
                logger.info(f"Extração concluída para CNPJ: {cnpj_limpo}")
            else:
                logger.warning(f"Nenhum dado retornado para CNPJ: {cnpj_limpo}")
        
        except Exception as e:
            logger.error(f"Falha na extração do CNPJ {cnpj}: {e}")
            # Continua para o próximo CNPJ mesmo em caso de erro
    
    return resultados

def extrair_dados_malha(cnpj: str, data_inicio: str = None, data_fim: str = None) -> List[Dict[str, Any]]:
    """
    Extrai dados de pendências de malha para um CNPJ específico do banco Oracle.
    
    Executa a query SQL definida em Fisconforme_malha_cnpj.sql.
    
    Args:
        cnpj: CNPJ da empresa (formato numérico ou com máscara)
        data_inicio: Início do período (MM/AAAA)
        data_fim: Fim do período (MM/AAAA)
    
    Returns:
        Lista de dicionários, cada um representando uma pendência.
    """
    if not validar_cnpj(cnpj):
        logger.warning(f"CNPJ inválido para extração de malha: {cnpj}")
        return []
    
    cnpj_limpo = limpar_cnpj(cnpj)
    logger.info(f"Extraindo dados de malha para CNPJ: {cnpj_limpo} (Período: {data_inicio} a {data_fim})")
    
    # Normaliza períodos MM/YYYY -> YYYYMM para o Oracle
    d_ini = "190001" # Valor padrão remoto
    if data_inicio and "/" in data_inicio:
        try:
            m, y = data_inicio.split("/")
            d_ini = f"{y.strip()}{m.strip().zfill(2)}"
        except: pass
        
    d_fim = "209912" # Valor padrão remoto
    if data_fim and "/" in data_fim:
        try:
            m, y = data_fim.split("/")
            d_fim = f"{y.strip()}{m.strip().zfill(2)}"
        except: pass

    # Lê o arquivo SQL
    sql_conteudo = ler_arquivo_sql(SQL_MALHA_CNPJ)
    if not sql_conteudo:
        logger.error(f"Não foi possível ler o arquivo SQL: {SQL_MALHA_CNPJ}")
        return []
    
    # Conecta ao banco
    conexao = conectar_oracle()
    if not conexao:
        logger.error("Falha ao conectar com o banco de dados Oracle para extração de malha")
        return []
    
    try:
        with conexao.cursor() as cursor:
            cursor.arraysize = 50
            # Prepara parâmetros com bind variables
            params = {
                "cnpj": cnpj_limpo,
                "data_inicio": d_ini,
                "data_fim": d_fim
            }
            cursor.execute(sql_conteudo, params)
            
            # Obtém nomes das colunas (mantendo o que vem do SQL)
            colunas = [col[0].lower() for col in cursor.description]
            
            resultados = []
            for linha in cursor.fetchall():
                # Cria dicionário para a linha
                item = dict(zip(colunas, linha))
                
                # Normaliza valores nulos para string vazia
                for k, v in item.items():
                    if v is None:
                        item[k] = ""
                    elif isinstance(v, str):
                        item[k] = v.strip()
                
                resultados.append(item)
            
            logger.info(f"Extraídos {len(resultados)} registros de malha para {cnpj_limpo}")
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
    # Exemplo de uso do módulo
    print("=" * 60)
    print("Módulo de Extração de Dados Cadastrais - Oracle")
    print("=" * 60)
    
    # CNPJ de exemplo para teste
    cnpj_teste = input("\nInforme o CNPJ para extração (ou Enter para sair): ").strip()
    
    if cnpj_teste:
        print(f"\nExtraindo dados para: {cnpj_teste}")
        print("-" * 60)
        
        try:
            dados = extrair_dados_cadastrais(cnpj_teste)
            
            if dados:
                print("\nDados extraídos com sucesso!")
                print("\nCampos encontrados:")
                for chave, valor in dados.items():
                    print(f"  {chave}: {valor}")
            else:
                print("\nNenhum dado encontrado para este CNPJ.")
        
        except Exception as e:
            print(f"\nErro durante a extração: {e}")
    
    print("\n" + "=" * 60)
    print("Processamento concluído.")
