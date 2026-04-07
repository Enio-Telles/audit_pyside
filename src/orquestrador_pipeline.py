"""
Orquestrador principal do ETL.
Consolida dados brutos do Oracle em tabelas analiticas.

Usa o padrao Registry para mapear IDs de tabelas a funcoes de geracao,
com dependencias explicitas para execucao inteligente.
"""

from __future__ import annotations

import re
import sys
import traceback
from pathlib import Path
from typing import Callable

from rich import print as rprint

ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / "src"


# ---------------------------------------------------------------------------
# Registry: cada entrada mapeia um ID a sua funcao + dependencias criticas
# ---------------------------------------------------------------------------
class _TabelaRegistro:
    """Entrada do registo de tabelas."""

    __slots__ = ("id", "funcao_path", "deps", "_func")

    def __init__(self, id: str, funcao_path: str, deps: list[str] | None = None):
        self.id = id
        self.funcao_path = funcao_path  # "modulo:funcao"
        self.deps = deps or []
        self._func: Callable | None = None

    def resolver(self) -> Callable:
        if self._func is None:
            modulo_path, nome_func = self.funcao_path.rsplit(":", 1)
            import importlib
            mod = importlib.import_module(modulo_path)
            self._func = getattr(mod, nome_func)
        return self._func


REGISTO_TABELAS: dict[str, _TabelaRegistro] = {}


def _registar(id: str, funcao_path: str, deps: list[str] | None = None) -> None:
    REGISTO_TABELAS[id] = _TabelaRegistro(id, funcao_path, deps)


# Ordem lógica: dependencias criticas explícitas
_registar("tb_documentos",       "transformacao.tabela_documentos:gerar_tabela_documentos")
_registar("item_unidades",       "transformacao.item_unidades:gerar_item_unidades",       deps=["tb_documentos"])
_registar("itens",               "transformacao.itens:gerar_itens",                       deps=["item_unidades"])
_registar("descricao_produtos",  "transformacao.descricao_produtos:gerar_descricao_produtos", deps=["itens"])
_registar("produtos_final",      "transformacao.produtos_final_v2:gerar_produtos_final",  deps=["descricao_produtos"])
_registar("fontes_produtos",     "transformacao.fontes_produtos:gerar_fontes_produtos",   deps=["produtos_final"])
_registar("fatores_conversao",   "transformacao.fatores_conversao:calcular_fatores_conversao", deps=["fontes_produtos"])
_registar("c170_xml",            "transformacao.c170_xml:gerar_c170_xml",                 deps=["fatores_conversao"])
_registar("c176_xml",            "transformacao.c176_xml:gerar_c176_xml",                 deps=["fatores_conversao"])
_registar("movimentacao_estoque","transformacao.movimentacao_estoque:gerar_movimentacao_estoque", deps=["c170_xml", "c176_xml"])
_registar("calculos_mensais",    "transformacao.calculos_mensais:gerar_calculos_mensais", deps=["movimentacao_estoque"])
_registar("calculos_anuais",     "transformacao.calculos_anuais:gerar_calculos_anuais",   deps=["movimentacao_estoque"])


def _ordem_topologica(selecionadas: list[str]) -> list[str]:
    """Resolve a ordem de execucao respeitando dependencias."""
    visitados: set[str] = set()
    ordem: list[str] = []

    def _visitar(tab_id: str) -> None:
        if tab_id in visitados:
            return
        visitados.add(tab_id)
        reg = REGISTO_TABELAS.get(tab_id)
        if reg is None:
            return
        for dep in reg.deps:
            if dep in selecionadas or dep in REGISTO_TABELAS:
                _visitar(dep)
        ordem.append(tab_id)

    for tab_id in selecionadas:
        _visitar(tab_id)

    return ordem


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
            from extracao.extrair_dados_cnpj import extrair_dados
            extrair_dados(
                cnpj_input=cnpj,
                data_limite_input=data_limite,
                consultas_selecionadas=consultas_selecionadas,
            )
            rprint("[green]Extracao concluida.[/green]")
        except Exception as e:
            rprint(f"[red]Falha critica na extracao para {cnpj}:[/red] {e}")
            return False

    if tabelas_selecionadas:
        ordem = _ordem_topologica(tabelas_selecionadas)
        rprint(f"[bold blue]Fase 2: gerando {len(ordem)} tabelas de negocio...[/bold blue]")

        etapas_executadas: set[str] = set()

        for tab_id in ordem:
            reg = REGISTO_TABELAS.get(tab_id)
            if reg is None:
                rprint(f"[yellow]Tabela desconhecida: {tab_id}[/yellow]")
                continue

            # Verificar dependencias criticas
            deps_falhadas = [d for d in reg.deps if d not in etapas_executadas and d in tabelas_selecionadas]
            if deps_falhadas:
                rprint(f"[red]Pulando {tab_id}: dependencias falharam ({', '.join(deps_falhadas)})[/red]")
                sucesso_global = False
                continue

            rprint(f"[yellow]Processando etapa:[/yellow] [bold]{tab_id}[/bold]...")

            try:
                funcao = reg.resolver()
                ok = funcao(cnpj)
                if not ok:
                    rprint(f"[red]Etapa {tab_id} retornou falha (False).[/red]")
                    sucesso_global = False
                else:
                    etapas_executadas.add(tab_id)
                    rprint(f"[green]{tab_id} finalizada.[/green]")
            except Exception as e:
                rprint(f"[red]Erro inesperado na etapa {tab_id}:[/red] {e}")
                rprint(f"[dim]{traceback.format_exc()}[/dim]")
                sucesso_global = False

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
