"""
Motor de processamento analítico utilizando Polars LazyFrame.

Recebe os arquivos Parquet gerados pelo ExtratorOracle e aplica as
transformações equivalentes às queries SQL originais, com vantagens de:
- Predicate pushdown (filtros antes dos joins)
- Processamento lazy (apenas materializa no .collect() final)
- Controle de memória via streaming

Autor: Pipeline Fisconforme
Data: 2026-04-02
"""

import polars as pl
import logging
from pathlib import Path
from .path_resolver import get_root_dir

# Configuração de logging
logger = logging.getLogger(__name__)

# Diretório dos arquivos Parquet intermediários
PASTA_DADOS = get_root_dir() / "dados" / "fisconforme" / "data_parquet"


class ProcessadorPolars:
    """Motor de processamento analítico utilizando Polars para alto desempenho."""

    def __init__(self):
        # Mapeamento de nomes lógicos → arquivos Parquet
        self.tabelas = {
            "pendencias": PASTA_DADOS / "APP_PENDENCIA_PENDENCIAS.parquet",
            "malhas": PASTA_DADOS / "APP_PENDENCIA_MALHAS.parquet",
            "pessoa": PASTA_DADOS / "BI_DM_PESSOA.parquet",
            "localidade": PASTA_DADOS / "BI_DM_LOCALIDADE.parquet",
            "regime_pagto": PASTA_DADOS / "BI_DM_REGIME_PAGTO_DESCRICAO.parquet",
            "situacao": PASTA_DADOS / "BI_DM_SITUACAO_CONTRIBUINTE.parquet",
            "notificacao": PASTA_DADOS / "BI_FATO_DET_NOTIFICACAO.parquet",
            "chaves_nota": PASTA_DADOS / "APP_PENDENCIA_VW_FISCONFORME_CHAVE_NOTA.parquet",
            "nfe_detalhe": PASTA_DADOS / "BI_DM_NFE_CHAVE_E_FISCONFORME.parquet",
            "consolidado": PASTA_DADOS / "BI_DM_CONSOLIDADO_PENDENCIAS.parquet",
        }

    def carregar_tabela(self, nome):
        """Carrega um arquivo Parquet em modo LazyFrame."""
        caminho = self.tabelas.get(nome)
        if caminho is None:
            raise ValueError(f"Tabela '{nome}' não mapeada no processador")
        if not caminho.exists():
            raise FileNotFoundError(f"Tabela '{nome}' não encontrada: {caminho}")
        return pl.scan_parquet(caminho)

    # =========================================================================
    # EXPRESSÃO AUXILIAR: CASE WHEN STATUS → DESCRIÇÃO
    # =========================================================================

    @staticmethod
    def _expr_status_pendencia():
        """
        Gera a expressão Polars equivalente ao CASE WHEN p.status do SQL.

        Retorna:
            pl.Expr com alias 'status_pendencia'
        """
        return (
            pl.when(pl.col("STATUS") == 0).then(pl.lit("0 - pendente"))
            .when(pl.col("STATUS") == 1).then(pl.lit("1 - contestado"))
            .when(pl.col("STATUS") == 2).then(pl.lit("2 - resolvido"))
            .when(pl.col("STATUS") == 3).then(pl.lit("3 - acao fiscal"))
            .when(pl.col("STATUS") == 4).then(pl.lit("4 - pendente indeferido"))
            .when(pl.col("STATUS") == 5).then(pl.lit("5 - deferido"))
            .when(pl.col("STATUS") == 6).then(pl.lit("6 - notificado"))
            .when(pl.col("STATUS") == 7).then(pl.lit("7 - deferido automaticamente"))
            .when(pl.col("STATUS") == 8).then(pl.lit("8 - aguardando autorizacao"))
            .when(pl.col("STATUS") == 9).then(pl.lit("9 - cancelado"))
            .when(pl.col("STATUS") == 10).then(pl.lit("10 - fiscalizado"))
            .when(pl.col("STATUS") == 11).then(pl.lit("11 - inapta - 5 anos"))
            .when(pl.col("STATUS") == 12).then(pl.lit("12 - pre-fiscalizacao"))
            .otherwise(pl.col("STATUS").cast(pl.Utf8))
            .alias("status_pendencia")
        )


    # =========================================================================
    # RELATÓRIO: DADOS CADASTRAIS
    # Equivale a: sql/dados_cadastrais.sql
    # =========================================================================

    def relatorio_dados_cadastrais(self):
        """Implementa a lógica da consulta sql/dados_cadastrais.sql."""
        pessoa = self.carregar_tabela("pessoa")
        localidade = self.carregar_tabela("localidade")
        regime = self.carregar_tabela("regime_pagto")
        situacao = self.carregar_tabela("situacao")

        resultado = (
            pessoa
            .join(localidade, on="CO_MUNICIPIO", how="left")
            .join(regime, left_on="CO_REGIME_PAGTO", right_on="CO_REGIME_PAGAMENTO", how="left")
            .join(situacao, left_on="IN_SITUACAO", right_on="CO_SITUACAO_CONTRIBUINTE", how="left")
            .select([
                pl.col("CO_CNPJ_CPF").alias("CNPJ"),
                pl.col("CO_CAD_ICMS").alias("IE"),
                pl.col("NO_RAZAO_SOCIAL").alias("RAZAO_SOCIAL"),
                pl.col("NO_FANTASIA").alias("Nome Fantasia"),
                (pl.col("DESC_ENDERECO") + " " + pl.col("BAIRRO")).alias("Endereço"),
                "NO_MUNICIPIO",
                "CO_UF",
                (
                    pl.col("CO_REGIME_PAGTO").cast(pl.Utf8)
                    + " - "
                    + pl.col("NO_REGIME_PAGAMENTO")
                ).alias("Regime de Pagamento"),
                (
                    pl.col("IN_SITUACAO") + " - " + pl.col("NO_SITUACAO_CONTRIBUINTE")
                ).alias("Situação da IE"),
            ])
        )

        return resultado.collect()

    # =========================================================================
    # RELATÓRIO: PENDÊNCIAS RANKEADAS
    # Equivale a: sql/Fisconforme_malha_cnpj.sql
    # =========================================================================

    def relatorio_pendencias_rankeadas(self):
        """
        Implementa a lógica da consulta sql/Fisconforme_malha_cnpj.sql.

        Ranking: ROW_NUMBER() OVER (PARTITION BY id_pendencia
                 ORDER BY data_ciencia_consolidada DESC)
        Retém apenas a linha mais recente (rn = 1) por pendência.
        """
        pendencias = self.carregar_tabela("pendencias")
        malhas = self.carregar_tabela("malhas")
        notificacoes = self.carregar_tabela("notificacao")

        # Sort global e ranking via cum_sum dentro do grupo
        resultado = (
            pendencias
            .join(malhas, left_on="MALHAS_ID", right_on="ID", how="left")
            .join(notificacoes, left_on="ID", right_on="ID_FISCONFORME", how="left")
            .with_columns(
                pl.coalesce(["DT_CIENCIA", "DATA_CIENCIA"]).alias("data_consolidada")
            )
            .sort(
                by=["ID", "data_consolidada"],
                descending=[False, True],
                nulls_last=[False, True],
            )
            .with_columns(
                pl.lit(1).cum_sum().over("ID").alias("rn")
            )
            .filter(pl.col("rn") == 1)
            .select([
                pl.col("CO_CNPJ_CPF").alias("cnpj"),
                pl.col("ID").alias("id_pendencia"),
                "ID_NOTIFICACAO",
                "MALHAS_ID",
                pl.col("TITULO").alias("titulo_malha"),
                "PERIODO",
                "STATUS",
                pl.col("TP_STATUS").alias("status_notificacao"),
                "data_consolidada",
            ])
        )

        return resultado.collect()


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    try:
        processador = ProcessadorPolars()

        print(f"\n{'='*60}")
        print("[*] Processando Dados Cadastrais...")
        df_cad = processador.relatorio_dados_cadastrais()
        print(f"[OK] {len(df_cad)} linhas processadas.")
        print(df_cad.head(5))

    except FileNotFoundError as e:
        print(f"[ERRO] Parquet não encontrado: {e}")
        print("Execute o ExtratorOracle primeiro para gerar os arquivos.")
    except Exception as e:
        print(f"[ERRO] Falha no processamento: {e}")
        raise
