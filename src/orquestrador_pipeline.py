"""
Orquestrador principal do ETL.
Consolida dados brutos do Oracle em tabelas analiticas.

Usa o padrao Registry para mapear IDs de tabelas a funcoes de geracao,
com dependencias explicitas para execucao inteligente.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Callable

import structlog
from rich import print as rprint

log = structlog.get_logger(__name__)

ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / "src"


# ---------------------------------------------------------------------------
# Registry: cada entrada mapeia um ID a sua funcao + dependencias criticas
# ---------------------------------------------------------------------------
class _TabelaRegistro:
    """Entrada do registo de tabelas."""

    __slots__ = ("_func", "deps", "funcao_path", "id")

    def __init__(self, id: str, funcao_path: str, deps: list[str] | None = None):
        """Inicializa entrada do registro com ID, caminho de função e dependências.

        Args:
            id: Identificador único da tabela no registro.
            funcao_path: Caminho ``modulo:funcao`` resolvido via importlib.
            deps: IDs de tabelas das quais esta depende para execução correta.
        """
        self.id = id
        self.funcao_path = funcao_path  # "modulo:funcao"
        self.deps = deps or []
        self._func: Callable | None = None

    def resolver(self) -> Callable:
        """Resolve e cacheia a função de geração associada a esta entrada.

        Returns:
            Callable que recebe ``cnpj: str`` e retorna ``bool``.
        """
        if self._func is None:
            modulo_path, nome_func = self.funcao_path.rsplit(":", 1)
            import importlib

            mod = importlib.import_module(modulo_path)
            self._func = getattr(mod, nome_func)
        return self._func


REGISTO_TABELAS: dict[str, _TabelaRegistro] = {}


def _registar(id: str, funcao_path: str, deps: list[str] | None = None) -> None:
    """Registra uma tabela em REGISTO_TABELAS com seu caminho de função e dependências."""
    REGISTO_TABELAS[id] = _TabelaRegistro(id, funcao_path, deps)


# Ordem lógica: dependencias criticas explícitas
_registar("tb_documentos", "transformacao.tabela_documentos:gerar_tabela_documentos")
_registar("efd_atomizacao", "transformacao.efd_atomizacao:gerar_efd_atomizacao")
_registar(
    "item_unidades",
    "transformacao.item_unidades:gerar_item_unidades",
    deps=["tb_documentos"],
)
_registar("itens", "transformacao.itens:gerar_itens", deps=["item_unidades"])
_registar(
    "descricao_produtos",
    "transformacao.descricao_produtos:gerar_descricao_produtos",
    deps=["itens"],
)
_registar(
    "produtos_final",
    "transformacao.produtos_final_v2:gerar_produtos_final",
    deps=["descricao_produtos"],
)
_registar(
    "fontes_produtos",
    "transformacao.fontes_produtos:gerar_fontes_produtos",
    deps=["produtos_final"],
)
_registar(
    "fatores_conversao",
    "transformacao.fatores_conversao:calcular_fatores_conversao",
    deps=["fontes_produtos"],
)
_registar(
    "c170_xml", "transformacao.c170_xml:gerar_c170_xml", deps=["fatores_conversao"]
)
_registar(
    "c176_xml", "transformacao.c176_xml:gerar_c176_xml", deps=["fatores_conversao"]
)
_registar(
    "movimentacao_estoque",
    "transformacao.movimentacao_estoque:gerar_movimentacao_estoque",
    deps=["c170_xml", "c176_xml"],
)
_registar(
    "calculos_mensais",
    "transformacao.calculos_mensais:gerar_calculos_mensais",
    deps=["movimentacao_estoque"],
)
_registar(
    "calculos_anuais",
    "transformacao.calculos_anuais:gerar_calculos_anuais",
    deps=["movimentacao_estoque"],
)
_registar(
    "calculos_periodos",
    "transformacao.calculos_periodo_pkg:gerar_calculos_periodos",
    deps=["movimentacao_estoque"],
)
_registar(
    "ressarcimento_st",
    "transformacao.ressarcimento_st_pkg:executar_pipeline_ressarcimento_st",
    deps=["movimentacao_estoque"],
)
_registar(
    "aba_resumo_global",
    "transformacao.resumo_global:gerar_aba_resumo_global",
    deps=["calculos_mensais", "calculos_anuais"],
)
_registar(
    "aba_produtos_selecionados",
    "transformacao.produtos_selecionados:gerar_aba_produtos_selecionados",
    deps=["calculos_mensais", "calculos_anuais"],
)


def _ordem_topologica(selecionadas: list[str]) -> list[str]:
    """Resolve a ordem de execucao respeitando dependencias."""
    visitados: set[str] = set()
    ordem: list[str] = []

    def _visitar(tab_id: str) -> None:
        """Visita recursivamente uma tabela e suas dependências adicionando-as à ordem."""
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
    """Executa o pipeline ETL completo para um CNPJ.

    Fase 1 (opcional): extrai consultas SQL selecionadas do Oracle.
    Fase 2 (opcional): gera tabelas de negócio em ordem topológica, respeitando dependências.

    Args:
        cnpj: CNPJ do contribuinte (com ou sem formatação; apenas dígitos são usados).
        consultas_selecionadas: Caminhos de arquivos SQL a extrair do Oracle. Omitir pula a Fase 1.
        tabelas_selecionadas: IDs de tabelas registradas em REGISTO_TABELAS a gerar. Omitir pula a Fase 2.
        data_limite: Data limite de processamento no formato ``DD/MM/YYYY`` (bind Oracle).

    Returns:
        True se todas as etapas concluíram sem falhas; False em caso de falha parcial ou total.
    """
    cnpj = re.sub(r"\D", "", cnpj)
    if len(cnpj) != 14:
        rprint(f"[red]Erro:[/red] CNPJ invalido: {cnpj}")
        return False

    log.info("orquestrador.pipeline.iniciado", cnpj=cnpj)
    rprint(f"\n[bold green]Iniciando pipeline para CNPJ: {cnpj}[/bold green]")
    sucesso_global = True

    if consultas_selecionadas:
        rprint(
            f"[bold blue]Fase 1: extraindo {len(consultas_selecionadas)} tabelas brutas...[/bold blue]"
        )
        try:
            from extracao.extrair_dados_cnpj import extrair_dados

            extrair_dados(
                cnpj_input=cnpj,
                data_limite_input=data_limite,
                consultas_selecionadas=consultas_selecionadas,
            )
            rprint("[green]Extracao concluida.[/green]")
        except Exception as e:
            from transformacao.auxiliares.logs import setup_logging

            setup_logging().error(f"Falha critica na extracao para {cnpj}", exc_info=e)
            rprint(
                f"[red]Falha critica na extracao para {cnpj}. Consulte os logs para mais detalhes.[/red]"
            )
            return False

    if tabelas_selecionadas:
        ordem = _ordem_topologica(tabelas_selecionadas)
        rprint(
            f"[bold blue]Fase 2: gerando {len(ordem)} tabelas de negocio...[/bold blue]"
        )

        etapas_executadas: set[str] = set()

        for tab_id in ordem:
            reg = REGISTO_TABELAS.get(tab_id)
            if reg is None:
                rprint(f"[yellow]Tabela desconhecida: {tab_id}[/yellow]")
                continue

            # Verificar dependencias criticas
            deps_falhadas = [
                d
                for d in reg.deps
                if d not in etapas_executadas and d in tabelas_selecionadas
            ]
            if deps_falhadas:
                rprint(
                    f"[red]Pulando {tab_id}: dependencias falharam ({', '.join(deps_falhadas)})[/red]"
                )
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
                from transformacao.auxiliares.logs import setup_logging

                setup_logging().error(f"Erro inesperado na etapa {tab_id}", exc_info=e)
                rprint(
                    f"[red]Erro inesperado na etapa {tab_id}. Consulte os logs para mais detalhes.[/red]"
                )
                sucesso_global = False

    if sucesso_global:
        log.info("orquestrador.pipeline.sucesso", cnpj=cnpj)
        rprint(
            f"\n[bold green]Pipeline finalizado com sucesso para {cnpj}![/bold green]\n"
        )
    else:
        log.info("orquestrador.pipeline.falha_parcial", cnpj=cnpj)
        rprint(
            f"\n[bold yellow]Pipeline finalizado com avisos/falhas parciais para {cnpj}.[/bold yellow]\n"
        )

    return sucesso_global


if __name__ == "__main__":
    if len(sys.argv) > 1:
        cnpj_alvo = sys.argv[1]
        log.info("orquestrador.start", target_cnpj=cnpj_alvo)
        executar_pipeline_completo(
            cnpj_alvo, tabelas_selecionadas=["tb_documentos", "item_unidades", "itens"]
        )
    else:
        rprint("[yellow]Uso: python orquestrador_pipeline.py <CNPJ>[/yellow]")
