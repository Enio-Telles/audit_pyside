from __future__ import annotations

from pathlib import Path

from rich import print as rprint

from transformacao.atomizacao_pkg.pipeline_efd_atomizado import salvar_c100_tipado


def gerar_efd_atomizacao(cnpj: str, _pasta_cnpj: Path | None = None) -> bool:
    """
    Gera a primeira camada analitica da abordagem atomizada.

    A etapa atual materializa o C100 tipado a partir dos parquets extraidos em
    `arquivos_parquet/atomizadas`. Mantem a funcao desacoplada do restante do
    pipeline para permitir evolucao incremental da atomizacao.
    """

    try:
        caminho = salvar_c100_tipado(cnpj)
        rprint(f"[green]Atomizacao EFD gerada com sucesso:[/green] {caminho.name}")
        return True
    except FileNotFoundError as exc:
        rprint(f"[yellow]Atomizacao EFD nao executada:[/yellow] {exc}")
        return False
    except Exception as exc:
        rprint(f"[red]Falha ao gerar atomizacao EFD:[/red] {exc}")
        return False
