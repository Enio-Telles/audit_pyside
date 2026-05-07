"""
Módulo de extração de dados do Oracle para o pipeline Fisconforme.

Extrai dados de tabelas individuais do Oracle DW e salva em formato Parquet
para processamento posterior com Polars. Suporta:
- Connection pooling para eficiência
- Extração paralela via ThreadPoolExecutor
- Retry com backoff exponencial para resiliência
- SQL customizado para tabelas que exigem subqueries de filtro
- Validação pós-extração

Autor: Pipeline Fisconforme
Data: 2026-04-02
"""

import os
import re
import time
import logging
import oracledb
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
import sys

# Configuração de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

PADRAO_IDENTIFICADOR_ORACLE = re.compile(r"^[a-zA-Z0-9_$#]+$")

# Configuração e Carregamento de Ambiente
from .path_resolver import get_root_dir, get_env_path

ROOT_DIR = get_root_dir()
load_dotenv(dotenv_path=get_env_path(), encoding="latin-1", override=True)

# Tornar possível importar utilitários do pacote `src` quando executado diretamente
SRC_DIR = Path(ROOT_DIR) / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Importa função eficiente de escrita em Parquet por lotes
try:
    from extracao.extracao_oracle_eficiente import _gravar_cursor_em_parquet
except Exception:
    _gravar_cursor_em_parquet = None


class ExtratorOracle:
    """
    Classe responsável pela extração eficiente de dados do Oracle DW.

    Utiliza connection pooling para reutilizar conexões entre extrações
    paralelas, reduzindo overhead de rede e autenticação.
    """

    MAX_TENTATIVAS = 3  # Máximo de retries por extração

    def __init__(self):
        self.usuario = os.getenv("DB_USER")
        self.senha = os.getenv("DB_PASSWORD")
        self.host = os.getenv("ORACLE_HOST")
        porta_str = os.getenv("ORACLE_PORT")
        self.porta = int(porta_str) if porta_str else 0
        self.servico = os.getenv("ORACLE_SERVICE")

        if not all([self.host, porta_str, self.servico, self.usuario, self.senha]):
            raise RuntimeError(
                "Configuracao Oracle incompleta. Verifique as variaveis ORACLE_HOST, ORACLE_PORT, ORACLE_SERVICE, DB_USER e DB_PASSWORD no .env"
            )

        self.dsn = oracledb.makedsn(self.host, self.porta, service_name=self.servico)
        self.pasta_dados = ROOT_DIR / "dados" / "fisconforme" / "data_parquet"
        self.pasta_dados.mkdir(parents=True, exist_ok=True)
        self._pool = None

    # =========================================================================
    # GESTÃO DE CONEXÕES
    # =========================================================================

    def _criar_pool(self):
        """Cria connection pool Oracle se ainda não existir."""
        if self._pool is None:
            self._pool = oracledb.create_pool(
                user=self.usuario,
                password=self.senha,
                dsn=self.dsn,
                min=2,
                max=4,
                increment=1,
            )
            logger.info("Connection pool Oracle criado (min=2, max=4)")
        return self._pool

    def obter_conexao(self):
        """Obtém conexão do pool e configura sessão NLS."""
        pool = self._criar_pool()
        conn = pool.acquire()
        # Configura formato numérico brasileiro (ponto decimal, vírgula milhar)
        with conn.cursor() as cursor:
            cursor.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'")
        return conn

    def fechar(self):
        """Fecha o connection pool e libera recursos."""
        if self._pool:
            self._pool.close()
            self._pool = None
            logger.info("Connection pool Oracle fechado")

    # =========================================================================
    # RESILIÊNCIA — RETRY COM BACKOFF EXPONENCIAL
    # =========================================================================

    def _executar_com_retry(self, func, descricao="operação"):
        """
        Executa uma função com retry e backoff exponencial.

        Args:
            func: Callable sem argumentos a ser executado
            descricao: Nome da operação para logging

        Returns:
            Resultado de func()

        Raises:
            Exception: Se todas as tentativas falharem
        """
        for tentativa in range(1, self.MAX_TENTATIVAS + 1):
            try:
                return func()
            except Exception as e:
                if tentativa == self.MAX_TENTATIVAS:
                    logger.error(
                        f"[ERRO] {descricao}: todas as {self.MAX_TENTATIVAS} "
                        f"tentativas falharam. Último erro: {e}"
                    )
                    raise
                espera = 2**tentativa
                logger.warning(
                    f"[RETRY] {descricao}: tentativa {tentativa}/{self.MAX_TENTATIVAS} "
                    f"falhou ({e}). Aguardando {espera}s..."
                )
                time.sleep(espera)

    # =========================================================================
    # VALIDAÇÃO PÓS-EXTRAÇÃO
    # =========================================================================

    def _validar_parquet(self, arquivo, nome_tabela):
        """
        Valida que o Parquet foi criado com linhas > 0.

        Args:
            arquivo: Path do arquivo Parquet
            nome_tabela: Nome legível da tabela para logging

        Returns:
            True se válido, False caso contrário
        """
        if not arquivo.exists():
            logger.error(f"[VALIDAÇÃO] Arquivo não criado: {arquivo}")
            return False
        try:
            import polars as pl

            n_linhas = pl.scan_parquet(arquivo).select(pl.len()).collect().item()
            if n_linhas == 0:
                logger.warning(f"[VALIDAÇÃO] {nome_tabela}: 0 linhas extraídas")
                return False
            logger.info(f"[VALIDAÇÃO] {nome_tabela}: {n_linhas} linhas OK")
            return True
        except Exception as e:
            logger.error(f"[VALIDAÇÃO] Erro ao validar {nome_tabela}: {e}")
            return False

    # =========================================================================
    # MÉTODOS DE EXTRAÇÃO
    # =========================================================================

    def extrair_tabela(self, schema, tabela, filtro_coluna=None, valor_filtro=None):
        """
        Extrai dados de uma tabela com filtro simples (coluna = :valor).

        Para tabelas-fato: passar filtro_coluna e valor_filtro.
        Para tabelas-dimensão: omitir filtros (SELECT * integral).

        Args:
            schema: Schema Oracle (ex: 'APP_PENDENCIA', 'BI')
            tabela: Nome da tabela
            filtro_coluna: Coluna de filtro (ex: 'CPF_CNPJ')
            valor_filtro: Valor do filtro (ex: '12345678000190')

        Returns:
            True se a extração foi bem-sucedida, False caso contrário
        """
        # Validação de segurança contra SQL Injection (CWE-89) em identificadores
        if not PADRAO_IDENTIFICADOR_ORACLE.match(schema):
            raise ValueError(f"Nome de schema inválido: '{schema}'")
        if not PADRAO_IDENTIFICADOR_ORACLE.match(tabela):
            raise ValueError(f"Nome de tabela inválido: '{tabela}'")
        if filtro_coluna and not PADRAO_IDENTIFICADOR_ORACLE.match(filtro_coluna):
            raise ValueError(f"Nome de coluna de filtro inválido: '{filtro_coluna}'")
        nome_completo = f"{schema}.{tabela}"
        arquivo_saida = self.pasta_dados / f"{schema}_{tabela}.parquet"

        # Monta SQL com bind variable (nunca f-string com valor de usuário)
        sql = f"SELECT * FROM {nome_completo}"
        params = {}
        if filtro_coluna and valor_filtro:
            sql += f" WHERE {filtro_coluna} = :valor"
            params = {"valor": valor_filtro}

        logger.info(f"[*] Extraindo {nome_completo}...")

        def _extrair():
            # Uso de escrita por lotes via cursor para reduzir uso de memória
            with self.obter_conexao() as conn:
                if _gravar_cursor_em_parquet is None:
                    # Fallback para pandas caso utilitário não esteja disponível
                    df = pd.read_sql(sql, conn, params=params)
                    df.to_parquet(arquivo_saida, index=False)
                else:
                    with conn.cursor() as cursor:
                        # Configura para fetch por lotes
                        cursor.arraysize = 50_000
                        if params:
                            cursor.execute(sql, params)
                        else:
                            cursor.execute(sql)
                        _gravar_cursor_em_parquet(
                            cursor,
                            arquivo_saida,
                            tamanho_lote=50_000,
                            rotulo_consulta=nome_completo,
                        )

        try:
            self._executar_com_retry(_extrair, descricao=nome_completo)
            valido = self._validar_parquet(arquivo_saida, nome_completo)
            status = "OK" if valido else "AVISO"
            logger.info(f"[{status}] {nome_completo} → {arquivo_saida.name}")
            return valido
        except Exception as e:
            logger.error(f"[ERRO] Falha ao extrair {nome_completo}: {e}")
            return False

    def extrair_com_sql(self, sql, params, nome_saida):
        """
        Extrai dados usando SQL customizado e salva em Parquet.

        Usado para tabelas que precisam de subqueries de filtro,
        como DM_NFE_CHAVE_E_FISCONFORME e DM_CONSOLIDADO_PENDENCIAS.

        Args:
            sql: Query SQL completa com bind variables (:nome)
            params: Dicionário de parâmetros (ex: {"valor": "12345678000190"})
            nome_saida: Nome do arquivo Parquet de saída

        Returns:
            True se a extração foi bem-sucedida, False caso contrário
        """
        arquivo_saida = self.pasta_dados / nome_saida

        logger.info(f"[*] Extraindo via SQL customizado → {nome_saida}...")

        def _extrair():
            # Uso de escrita por lotes via cursor para reduzir uso de memória
            with self.obter_conexao() as conn:
                if _gravar_cursor_em_parquet is None:
                    df = pd.read_sql(sql, conn, params=params)
                    df.to_parquet(arquivo_saida, index=False)
                else:
                    with conn.cursor() as cursor:
                        cursor.arraysize = 50_000
                        # prepared SQL: usar prepare + execute para binds se houver
                        try:
                            cursor.prepare(sql)
                            cursor.execute(None, params or {})
                        except Exception:
                            # fallback simples
                            cursor.execute(sql, params or {})
                        _gravar_cursor_em_parquet(
                            cursor,
                            arquivo_saida,
                            tamanho_lote=50_000,
                            rotulo_consulta=nome_saida,
                        )

        try:
            self._executar_com_retry(_extrair, descricao=nome_saida)
            valido = self._validar_parquet(arquivo_saida, nome_saida)
            status = "OK" if valido else "AVISO"
            logger.info(f"[{status}] SQL customizado → {nome_saida}")
            return valido
        except Exception as e:
            logger.error(f"[ERRO] Falha SQL customizado ({nome_saida}): {e}")
            return False

    # =========================================================================
    # ORQUESTRAÇÃO — EXTRAÇÃO COMPLETA POR CNPJ
    # =========================================================================

    def extrair_conjunto_dados_contribuinte(self, cnpj):
        """
        Executa a extração em paralelo de todas as tabelas necessárias
        para processar um CNPJ no pipeline Fisconforme.

        Extrai 10 tabelas em 3 categorias:
        1. Tabelas-fato filtradas por CNPJ (4 tabelas)
        2. Tabelas-dimensão integrais (4 tabelas pequenas)
        3. Tabelas com SQL customizado via subquery (2 tabelas grandes)

        Args:
            cnpj: CNPJ do contribuinte (apenas números)

        Returns:
            True se todas as extrações foram bem-sucedidas, False caso contrário
        """
        logger.info(f"{'=' * 60}")
        logger.info(f"Extração completa para CNPJ: {cnpj}")
        logger.info(f"{'=' * 60}")

        # -----------------------------------------------------------------
        # GRUPO 1: Tabelas-fato com filtro simples por CNPJ
        # -----------------------------------------------------------------
        tabelas_filtradas = [
            ("APP_PENDENCIA", "PENDENCIAS", "CPF_CNPJ"),
            ("BI", "FATO_DET_NOTIFICACAO", "CPF_CNPJ"),
            ("APP_PENDENCIA", "VW_FISCONFORME_CHAVE_NOTA", "CPF_CNPJ"),
            ("BI", "DM_PESSOA", "CO_CNPJ_CPF"),
        ]

        # -----------------------------------------------------------------
        # GRUPO 2: Tabelas-dimensão (extração integral, < 100K linhas)
        # -----------------------------------------------------------------
        tabelas_full = [
            ("APP_PENDENCIA", "MALHAS"),
            ("BI", "DM_LOCALIDADE"),
            ("BI", "DM_REGIME_PAGTO_DESCRICAO"),
            ("BI", "DM_SITUACAO_CONTRIBUINTE"),
        ]

        # -----------------------------------------------------------------
        # GRUPO 3: Tabelas grandes com SQL customizado (subquery por CNPJ)
        # Extraem apenas os registros vinculados ao CNPJ via subquery,
        # evitando full scan em tabelas de alto volume.
        # -----------------------------------------------------------------
        tabelas_sql_customizado = [
            {
                "sql": """
                    SELECT nfe.*
                    FROM bi.dm_nfe_chave_e_fisconforme nfe
                    WHERE nfe.chave_acesso IN (
                        SELECT vfcn.chave_acesso
                        FROM app_pendencia.vw_fisconforme_chave_nota vfcn
                        WHERE vfcn.cpf_cnpj = :valor
                          AND vfcn.chave_acesso IS NOT NULL
                    )
                """,
                "nome": "BI_DM_NFE_CHAVE_E_FISCONFORME.parquet",
            },
            {
                "sql": """
                    SELECT c.*
                    FROM bi.dm_consolidado_pendencias c
                    WHERE c.pendencia_id IN (
                        SELECT p.id
                        FROM app_pendencia.pendencias p
                        WHERE p.cpf_cnpj = :valor
                    )
                """,
                "nome": "BI_DM_CONSOLIDADO_PENDENCIAS.parquet",
            },
        ]

        # -----------------------------------------------------------------
        # EXECUÇÃO PARALELA (max_workers=4 conforme capacidade do DW)
        # -----------------------------------------------------------------
        tarefas = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            # Grupo 1: filtro simples
            for schema, tabela, coluna_filtro in tabelas_filtradas:
                tarefas.append(
                    executor.submit(self.extrair_tabela, schema, tabela, coluna_filtro, cnpj)
                )

            # Grupo 2: dimensões integrais
            for schema, tabela in tabelas_full:
                tarefas.append(executor.submit(self.extrair_tabela, schema, tabela))

            # Grupo 3: SQL customizado
            for config in tabelas_sql_customizado:
                tarefas.append(
                    executor.submit(
                        self.extrair_com_sql,
                        config["sql"],
                        {"valor": cnpj},
                        config["nome"],
                    )
                )

        # -----------------------------------------------------------------
        # COLETA DE RESULTADOS
        # -----------------------------------------------------------------
        resultados = [t.result() for t in tarefas]
        total = len(resultados)
        ok = sum(1 for r in resultados if r)
        falhas = total - ok

        logger.info(f"{'=' * 60}")
        logger.info(f"Extração concluída: {ok}/{total} tabelas OK, {falhas} falha(s)")
        logger.info(f"{'=' * 60}")

        return ok == total


if __name__ == "__main__":
    # Teste de extração
    extrator = ExtratorOracle()
    try:
        exemplo_cnpj = "13906970000100"
        extrator.extrair_conjunto_dados_contribuinte(exemplo_cnpj)
    finally:
        # Sempre fecha o pool ao terminar
        extrator.fechar()
