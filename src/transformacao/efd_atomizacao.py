from __future__ import annotations

from pathlib import Path

from rich import print as rprint

from transformacao.atomizacao_pkg.pipeline_efd_atomizado import _base_atomizada
from transformacao.atomizacao_pkg.pipeline_efd_atomizado import materializar_camadas_atomizadas


def gerar_efd_atomizacao(cnpj: str, _pasta_cnpj: Path | None = None) -> bool:
    """
    Gera a camada analitica principal da abordagem atomizada.

    A materializacao parte dos parquets em `arquivos_parquet/atomizadas` e gera
    visoes tipadas de `0200`, `C100`, `C170`, `C176`, `H005`, `H010`, `H020`
    e uma consolidacao do `Bloco H`.
    """

    caminhos_esperados = [
        _base_atomizada(cnpj) / "dimensions" / f"50_reg0200_raw_{cnpj}.parquet",
        _base_atomizada(cnpj) / "c100" / f"10_c100_raw_{cnpj}.parquet",
        _base_atomizada(cnpj) / "c170" / f"20_c170_raw_{cnpj}.parquet",
        _base_atomizada(cnpj) / "c176" / f"30_c176_raw_{cnpj}.parquet",
        _base_atomizada(cnpj) / "bloco_h" / f"40_h005_raw_{cnpj}.parquet",
        _base_atomizada(cnpj) / "bloco_h" / f"41_h010_raw_{cnpj}.parquet",
        _base_atomizada(cnpj) / "bloco_h" / f"42_h020_raw_{cnpj}.parquet",
    ]
    faltantes = [str(caminho.name) for caminho in caminhos_esperados if not caminho.exists()]
    if faltantes:
        raise RuntimeError(
            "Parquets atomizados ausentes para a EFD atomizada. "
            "Execute a extracao das consultas atomizadas antes do processamento. "
            f"Arquivos faltantes: {', '.join(faltantes)}"
        )

    caminhos = materializar_camadas_atomizadas(cnpj)
    rprint(
        "[green]Atomizacao EFD gerada com sucesso:[/green] "
        + ", ".join(caminho.name for caminho in caminhos)
    )
    return True
