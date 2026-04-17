from __future__ import annotations

import re
import threading
import logging
import sys
from pathlib import Path
from typing import Sequence

import polars as pl
from rich import print as rprint

logger = logging.getLogger(__name__)
thread_local = threading.local()


def close_thread_connection():
    if hasattr(thread_local, "conexao"):
        if thread_local.conexao:
            try:
                thread_local.conexao.close()
            except Exception as e:
                logger.warning(f"Erro ao fechar conexao de thread: {e}")
        thread_local.conexao = None


def get_thread_connection():
    if not hasattr(thread_local, "conexao"):
        # Cria uma nova conexão para esta thread
        # Usa o nome `conectar` importado de utilitarios.conectar_oracle para permitir
        # que os testes possam mockar `extracao.extrair_dados_cnpj.conectar`.
        conn = conectar()
        if conn is None:
            logger.error(
                f"[{threading.current_thread().name}] Falha ao criar conexão com banco de dados."
            )
            return None

        try:
            # Testar a conexão
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
        except Exception as e:
            logger.error(
                f"[{threading.current_thread().name}] Erro ao testar conexão: {e}"
            )
            return None

        thread_local.conexao = conn
    return thread_local.conexao


ROOT_DIR = Path(r"c:\funcoes - Copia")
SRC_DIR = ROOT_DIR / "src"
SQL_DIR = ROOT_DIR / "sql"
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"


try:
    from utilitarios.conectar_oracle import conectar, conectar as conectar_oracle
    from utilitarios.ler_sql import ler_sql
    from utilitarios.salvar_para_parquet import salvar_para_parquet
    from utilitarios.validar_cnpj import validar_cnpj
except ImportError as e:
    rprint(f"[red]Erro ao importar módulos utilitários:[/red] {e}")
    sys.exit(1)

# Import helpers from the efficient extractor module (same package)
from .extracao_oracle_eficiente import (
    descobrir_consultas_sql,
    executar_extracao_oracle,
    imprimir_resumo_extracao,
)


def processar_arquivo(
    arq_sql, cnpj_limpo, data_limite_input, consultas_dir, pasta_saida
):
    # Garantir que arq_sql e consultas_dir sejam Path (podem vir como str do catalogo)
    arq_sql = Path(arq_sql) if not isinstance(arq_sql, Path) else arq_sql
    consultas_dir = (
        Path(consultas_dir) if not isinstance(consultas_dir, Path) else consultas_dir
    )
    try:
        conexao = get_thread_connection()
        if not conexao:
            rprint(f"[red]Falha na conexão para o arquivo {arq_sql.name}[/red]")
            return False

        with conexao.cursor() as cursor:
            cursor.arraysize = 1000

            rprint(
                f"\n[bold cyan]Processando: {arq_sql.relative_to(consultas_dir)}[/bold cyan]"
            )

            sql_txt = ler_sql(arq_sql)
            if not sql_txt:
                rprint(
                    f"[yellow]Arquivo {arq_sql.name} vazio ou com erro de leitura.[/yellow]"
                )
                return True

            cursor.prepare(sql_txt)
            nomes_binds = cursor.bindnames()

            binds = {}
            tem_bind_cnpj = False
            for b in nomes_binds:
                b_upper = b.upper()
                if b_upper == "CNPJ":
                    binds[b] = cnpj_limpo
                    tem_bind_cnpj = True
                elif b_upper == "DATA_LIMITE_PROCESSAMENTO":
                    binds[b] = data_limite_input if data_limite_input else None

            # Executa consulta
            if not tem_bind_cnpj:
                if nomes_binds:
                    rprint(
                        f"[yellow]⚠️ Consulta possui os binds ({', '.join(nomes_binds)}) mas não o :CNPJ. Pulando para evitar extração imensa.[/yellow]"
                    )
                else:
                    rprint(
                        "[yellow]⚠️ Consulta não possui nenhuma variável de bind. Pulando para evitar extração imensa da base.[/yellow]"
                    )
                return True

            cursor.execute(None, binds)

            colunas = [col[0].lower() for col in cursor.description]
            dados = cursor.fetchall()

            if not dados:
                rprint(
                    f"[yellow]  Zero linhas retornadas para {arq_sql.name}. Pulando gravação.[/yellow]"
                )
                return True

            df = pl.DataFrame(dados, schema=colunas, orient="row")
            rprint(
                f"[green]  {len(df)} linhas lidas com sucesso para {arq_sql.name}.[/green]"
            )

            # Nome do arquivo no formato nomedaconsulta_<cnpj>.parquet (mantendo subpastas se houver)
            caminho_relativo = arq_sql.relative_to(consultas_dir)
            nome_arquivo = f"{arq_sql.stem}_{cnpj_limpo}.parquet"
            arquivo_saida = pasta_saida / caminho_relativo.parent / nome_arquivo

            salvar_para_parquet(df, arquivo_saida)
            return True

    except Exception as e_proc:
        rprint(f"[red]  [PROC] Erro processando {arq_sql.name}: {e_proc}[/red]")
        return False
    # finally block removed because connection is thread-local and will be closed later


def extrair_dados(
    cnpj_input: str,
    data_limite_input: str | None = None,
    consultas_selecionadas: Sequence[Path | str] | None = None,
) -> bool:
    if not validar_cnpj(cnpj_input):
        rprint(f"[red]Erro:[/red] CNPJ '{cnpj_input}' invalido!")
        return False

    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj_input)
    msg_inicio = (
        f"[bold green]Iniciando extracao para o CNPJ: {cnpj_limpo}[/bold green]"
    )
    if data_limite_input:
        msg_inicio += f" [cyan](Data Limite: {data_limite_input})[/cyan]"
    rprint(msg_inicio)

    pasta_saida = CNPJ_ROOT / cnpj_limpo / "arquivos_parquet"
    pasta_saida.mkdir(parents=True, exist_ok=True)

    consultas = descobrir_consultas_sql(consultas_selecionadas=consultas_selecionadas)
    if not consultas:
        rprint("[yellow]Nenhum arquivo .sql encontrado para extracao.[/yellow]")
        return False

    rprint(f"[cyan]Encontradas {len(consultas)} consultas SQL para execucao.[/cyan]")
    resultados = executar_extracao_oracle(
        cnpj_input=cnpj_limpo,
        data_limite_input=data_limite_input,
        consultas_selecionadas=[consulta.caminho for consulta in consultas],
        pasta_saida_base=pasta_saida,
        max_workers=5,
        progresso=lambda texto: rprint(texto),
    )

    rprint("\n[bold green]Processamento concluido.[/bold green]")
    return imprimir_resumo_extracao(resultados)


def main() -> None:
    data_limite_arg = None
    if len(sys.argv) > 1:
        cnpj_arg = sys.argv[1]
        if len(sys.argv) > 2:
            data_limite_arg = sys.argv[2]
    else:
        try:
            cnpj_arg = input("Informe o CNPJ para extracao: ").strip()
            if cnpj_arg:
                data_limite_arg = input(
                    "Data Limite Processamento (DD/MM/YYYY) [opcional, Enter para pular]: "
                ).strip()
                if not data_limite_arg:
                    data_limite_arg = None
        except KeyboardInterrupt:
            rprint("\n[yellow]Operacao cancelada pelo usuario.[/yellow]")
            sys.exit(0)
        except EOFError:
            sys.exit(0)

    if not cnpj_arg:
        rprint("[red]Erro: CNPJ nao fornecido.[/red]")
        sys.exit(1)

    sucesso = extrair_dados(cnpj_arg, data_limite_arg)
    sys.exit(0 if sucesso else 1)


if __name__ == "__main__":
    main()
