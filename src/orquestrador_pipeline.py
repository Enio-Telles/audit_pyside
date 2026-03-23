"""
Orquestrador principal do ETL.
Consolida dados brutos do Oracle em tabelas analiticas.
"""

from __future__ import annotations

import re
import sys
import traceback
from pathlib import Path

from rich import print as rprint

ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / "src"

for _dir in ["extracao", "transformacao", "utilitarios"]:
    path = SRC_DIR / _dir
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

try:
    from extrair_dados_cnpj import extrair_dados
    from tabela_documentos import gerar_tabela_documentos
    from item_unidades import gerar_item_unidades
    from itens import gerar_itens
    from descricao_produtos import gerar_descricao_produtos
    from produtos_final_v2 import gerar_produtos_final
    from fontes_produtos import gerar_fontes_produtos
    from fatores_conversao import calcular_fatores_conversao
    from c170_xml import gerar_c170_xml
    from c176_xml import gerar_c176_xml
    from movimentacao_estoque import gerar_movimentacao_estoque
except ImportError as e:
    rprint(f"[red]Erro de importacao no orquestrador:[/red] {e}")
    sys.exit(1)


def executar_pipeline_completo(
    cnpj: str,
    consultas_selecionadas: list[Path] | None = None,
    tabelas_selecionadas: list[str] | None = None,
    data_limite: str | None = None,
) -> bool:
    cnpj = re.sub(r"\D", "", cnpj)
    if len(cnpj) != 14:
        rprint(f"[red]Erro:[/red] CNPJ invalido: {cnpj}")
        return False

    rprint(f"\n[bold green]Iniciando pipeline para CNPJ: {cnpj}[/bold green]")
    sucesso_global = True

    if consultas_selecionadas:
        rprint(f"[bold blue]Fase 1: extraindo {len(consultas_selecionadas)} tabelas brutas...[/bold blue]")
        try:
            extrair_dados(cnpj=cnpj, data_limite_input=data_limite)
            rprint("[green]Extracao concluida.[/green]")
        except Exception as e:
            rprint(f"[red]Falha critica na extracao para {cnpj}:[/red] {e}")
            return False

    if tabelas_selecionadas:
        rprint(f"[bold blue]Fase 2: gerando {len(tabelas_selecionadas)} tabelas de negocio...[/bold blue]")
        ordem = [
            ("tb_documentos", gerar_tabela_documentos),
            ("item_unidades", gerar_item_unidades),
            ("itens", gerar_itens),
            ("descricao_produtos", gerar_descricao_produtos),
            ("produtos_final", gerar_produtos_final),
            ("fontes_produtos", gerar_fontes_produtos),
            ("fatores_conversao", calcular_fatores_conversao),
            ("c170_xml", gerar_c170_xml),
            ("c176_xml", gerar_c176_xml),
            ("movimentacao_estoque", gerar_movimentacao_estoque),
        ]

        for tab_id, func in ordem:
            if tab_id not in tabelas_selecionadas:
                continue

            rprint(f"[yellow]Processando etapa:[/yellow] [bold]{tab_id}[/bold]...")

            try:
                ok = func(cnpj)
                if not ok:
                    rprint(f"[red]Etapa {tab_id} retornou falha (False).[/red]")
                    sucesso_global = False
                    if tab_id in ["tb_documentos", "item_unidades"]:
                        rprint(f"[red]Parando pipeline: {tab_id} e dependencia critica.[/red]")
                        break
                else:
                    rprint(f"[green]{tab_id} finalizada.[/green]")
            except Exception as e:
                rprint(f"[red]Erro inesperado na etapa {tab_id}:[/red] {e}")
                rprint(f"[dim]{traceback.format_exc()}[/dim]")
                sucesso_global = False
                if tab_id in ["tb_documentos", "item_unidades"]:
                    break

    if sucesso_global:
        rprint(f"\n[bold green]Pipeline finalizado com sucesso para {cnpj}![/bold green]\n")
    else:
        rprint(f"\n[bold yellow]Pipeline finalizado com avisos/falhas parciais para {cnpj}.[/bold yellow]\n")

    return sucesso_global


if __name__ == "__main__":
    if len(sys.argv) > 1:
        cnpj_alvo = sys.argv[1]
        executar_pipeline_completo(cnpj_alvo, tabelas_selecionadas=["tb_documentos", "item_unidades", "itens"])
    else:
        rprint("[yellow]Uso: python orquestrador_pipeline.py <CNPJ>[/yellow]")
