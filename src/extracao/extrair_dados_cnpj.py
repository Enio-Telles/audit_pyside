from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Sequence

from rich import print as rprint

from extracao.extracao_oracle_eficiente import (
    CNPJ_ROOT,
    descobrir_consultas_sql,
    executar_extracao_oracle,
    imprimir_resumo_extracao,
)
from utilitarios.validar_cnpj import validar_cnpj


def extrair_dados(
    cnpj_input: str,
    data_limite_input: str | None = None,
    consultas_selecionadas: Sequence[Path | str] | None = None,
) -> bool:
    """Extrai as consultas SQL em parquet usando escrita incremental por lotes."""

    if not validar_cnpj(cnpj_input):
        rprint(f"[red]Erro:[/red] CNPJ '{cnpj_input}' invalido!")
        return False

    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj_input)
    msg_inicio = f"[bold green]Iniciando extracao para o CNPJ: {cnpj_limpo}[/bold green]"
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
                data_limite_arg = input("Data Limite Processamento (DD/MM/YYYY) [opcional, Enter para pular]: ").strip()
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
