"""Executa o pipeline de transformacao em um commit especifico via git worktree.

Em modo dry-run devolve DataFrames de smoke sem rodar o pipeline real.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

import polars as pl

_WORKTREE_BASE = Path(__file__).resolve().parents[2] / ".audit_pyside" / "worktrees"
_CACHE: dict[tuple[str, str], dict[str, pl.DataFrame]] = {}


def rodar_pipeline_em_commit(
    commit_sha: str,
    cnpj: str,
    pasta_dados: Path,
    pasta_saida: Path,
    dry_run: bool = False,
) -> dict[str, pl.DataFrame]:
    """Executa pipeline em commit_sha e devolve dict {etapa: DataFrame}.

    Em dry_run=True retorna fixtures minimas sem acessar git ou disco.
    Idempotente: segunda chamada com mesmo (commit_sha, cnpj) retorna do cache.
    """
    cache_key = (commit_sha, cnpj)
    if cache_key in _CACHE:
        return _CACHE[cache_key]

    if dry_run:
        resultado = _smoke_fixtures(cnpj)
        _CACHE[cache_key] = resultado
        return resultado

    worktree_path = _WORKTREE_BASE / commit_sha
    _garantir_worktree(commit_sha, worktree_path)

    cnpj_dados = pasta_dados / cnpj
    saida_cnpj = pasta_saida / commit_sha / cnpj
    saida_cnpj.mkdir(parents=True, exist_ok=True)

    subprocess.run(
        [
            "uv", "run", "--project", str(worktree_path),
            "python", "-m", "transformacao.rastreabilidade_produtos.fontes_produtos", cnpj,
        ],
        check=True,
        cwd=worktree_path,
        env={
            "PYTHONPATH": str(worktree_path / "src"),
            "CNPJ_ROOT_OVERRIDE": str(pasta_dados),
        },
    )

    resultado = _carregar_artefatos(cnpj_dados / "arquivos_parquet", cnpj)
    _CACHE[cache_key] = resultado
    return resultado


def _garantir_worktree(commit_sha: str, caminho: Path) -> None:
    if caminho.exists():
        return
    caminho.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["git", "worktree", "add", "--detach", str(caminho), commit_sha],
        check=True,
    )


def _carregar_artefatos(pasta_parquet: Path, cnpj: str) -> dict[str, pl.DataFrame]:
    resultado: dict[str, pl.DataFrame] = {}
    mapeamento = {
        "nfe_agr": f"nfe_agr_{cnpj}.parquet",
        "nfce_agr": f"nfce_agr_{cnpj}.parquet",
        "c170_agr": f"c170_agr_{cnpj}.parquet",
        "bloco_h_agr": f"bloco_h_agr_{cnpj}.parquet",
    }
    for etapa, nome in mapeamento.items():
        p = pasta_parquet / nome
        if p.exists():
            resultado[etapa] = pl.read_parquet(p)
    return resultado


def _smoke_fixtures(cnpj: str) -> dict[str, pl.DataFrame]:
    schema = {"chave_acesso": pl.Utf8, "prod_nitem": pl.Int64, "id_agrupado": pl.Utf8}
    rows = [{"chave_acesso": f"SMOKE_{i:03d}", "prod_nitem": 1, "id_agrupado": f"AGR_{i % 3}"}
            for i in range(5)]
    df = pl.DataFrame(rows, schema=schema)
    return {etapa: df for etapa in ["nfe_agr", "nfce_agr", "c170_agr", "bloco_h_agr"]}
