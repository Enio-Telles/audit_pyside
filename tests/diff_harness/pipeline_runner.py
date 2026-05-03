"""Executa o pipeline de transformacao em um commit especifico via git worktree."""

import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from tests.diff_harness.invariantes import FONTES_AUDITADAS
from tests.diff_harness.nivel_3_colapso_tripwire import assert_artefato_nao_stale

_WORKTREE_BASE = Path(__file__).resolve().parents[2] / ".audit_pyside" / "worktrees"


@dataclass(frozen=True)
class FonteArtefatos:
    fonte: str
    principal: pl.DataFrame
    sem_id: pl.DataFrame
    fora_escopo: pl.DataFrame
    principal_path: Path
    sem_id_path: Path
    fora_escopo_path: Path

    @property
    def total(self) -> int:
        return self.principal.height + self.sem_id.height + self.fora_escopo.height


@dataclass(frozen=True)
class PipelineArtefatos:
    fontes: dict[str, FonteArtefatos]
    mov_estoque: pl.DataFrame
    mov_estoque_path: Path


class ArtefatoStaleError(AssertionError):
    """Arquivo de saida existe, mas nao foi gerado no ciclo atual."""


def rodar_pipeline_em_commit(
    commit_sha: str,
    cnpj: str,
    pasta_dados: Path,
    pasta_saida: Path,
    dry_run: bool = False,
) -> PipelineArtefatos:
    """Executa o pipeline em um commit e devolve os artefatos carregados."""

    if dry_run:
        return _smoke_fixtures(cnpj)

    worktree_path = _WORKTREE_BASE / commit_sha
    _garantir_worktree(commit_sha, worktree_path)

    stamp_inicio = time.time()

    env = os.environ.copy()
    env["PYTHONPATH"] = str(worktree_path / "src")
    env["CNPJ_ROOT_OVERRIDE"] = str(pasta_dados)

    subprocess.run(
        [
            sys.executable,
            "-m",
            "transformacao.rastreabilidade_produtos.fontes_produtos",
            cnpj,
        ],
        check=True,
        cwd=worktree_path,
        env=env,
    )

    pasta_cnpj = pasta_dados / cnpj
    pasta_analises = pasta_cnpj / "analises" / "produtos"
    pasta_brutos = pasta_cnpj / "arquivos_parquet"
    fontes = _carregar_fontes(cnpj, pasta_analises, pasta_brutos)
    for fonte in fontes.values():
        assert_artefato_nao_stale(fonte.principal_path, stamp_inicio, f"{fonte.fonte} principal")
        assert_artefato_nao_stale(fonte.sem_id_path, stamp_inicio, f"{fonte.fonte} sem_id_agrupado")
        assert_artefato_nao_stale(
            fonte.fora_escopo_path,
            stamp_inicio,
            f"{fonte.fonte} fora_escopo_canonico",
        )
    mov_estoque_path = pasta_analises / f"mov_estoque_{cnpj}.parquet"
    mov_estoque = _ler_parquet_ou_vazio(mov_estoque_path, descricao="mov_estoque", exigir=True)

    pasta_saida.mkdir(parents=True, exist_ok=True)
    return PipelineArtefatos(
        fontes=fontes, mov_estoque=mov_estoque, mov_estoque_path=mov_estoque_path
    )


def carregar_artefatos_fonte(
    pasta_brutos: Path,
    pasta_analises: Path,
    cnpj: str,
    fonte: str,
) -> FonteArtefatos:
    """Carrega os tres parquets canonicos de uma fonte.

    Auxiliares ausentes retornam DataFrame vazio porque a ausencia pode ser legitima.
    """

    principal_path = pasta_brutos / f"{fonte}_agr_{cnpj}.parquet"
    sem_id_path = pasta_analises / f"{fonte}_agr_sem_id_agrupado_{cnpj}.parquet"
    fora_escopo_path = pasta_analises / f"{fonte}_agr_fora_escopo_canonico_{cnpj}.parquet"

    principal = _ler_parquet_ou_vazio(principal_path, descricao=f"{fonte} principal", exigir=True)
    sem_id = _ler_parquet_ou_vazio(sem_id_path, descricao=f"{fonte} sem_id_agrupado", exigir=False)
    fora_escopo = _ler_parquet_ou_vazio(
        fora_escopo_path,
        descricao=f"{fonte} fora_escopo_canonico",
        exigir=False,
    )

    return FonteArtefatos(
        fonte=fonte,
        principal=principal,
        sem_id=sem_id,
        fora_escopo=fora_escopo,
        principal_path=principal_path,
        sem_id_path=sem_id_path,
        fora_escopo_path=fora_escopo_path,
    )


def _garantir_worktree(commit_sha: str, caminho: Path) -> None:
    if caminho.exists():
        return
    caminho.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "git",
            "worktree",
            "add",
            "--detach",
            str(caminho),
            commit_sha,
        ],
        check=True,
    )


def _carregar_fontes(
    cnpj: str,
    pasta_analises: Path,
    pasta_brutos: Path,
) -> dict[str, FonteArtefatos]:
    resultado: dict[str, FonteArtefatos] = {}
    for fonte in FONTES_AUDITADAS:
        resultado[fonte] = carregar_artefatos_fonte(pasta_brutos, pasta_analises, cnpj, fonte)
    return resultado


def _ler_parquet_ou_vazio(
    caminho: Path,
    descricao: str,
    exigir: bool,
) -> pl.DataFrame:
    if caminho.exists():
        return pl.read_parquet(caminho)

    if exigir:
        raise FileNotFoundError(f"{descricao} nao encontrado: {caminho}")
    return pl.DataFrame()


def _smoke_fixtures(cnpj: str) -> PipelineArtefatos:
    schema = {
        "chave_acesso": pl.Utf8,
        "prod_nitem": pl.Int64,
        "id_agrupado": pl.Utf8,
    }
    rows = [
        {
            "chave_acesso": f"SMOKE_{i:03d}",
            "prod_nitem": 1,
            "id_agrupado": f"AGR_{i % 3}",
        }
        for i in range(5)
    ]
    df = pl.DataFrame(rows, schema=schema)
    fontes = {
        fonte: FonteArtefatos(
            fonte=fonte,
            principal=df,
            sem_id=pl.DataFrame(),
            fora_escopo=pl.DataFrame(),
            principal_path=Path(f"{fonte}_agr_{cnpj}.parquet"),
            sem_id_path=Path(f"{fonte}_agr_sem_id_agrupado_{cnpj}.parquet"),
            fora_escopo_path=Path(f"{fonte}_agr_fora_escopo_canonico_{cnpj}.parquet"),
        )
        for fonte in FONTES_AUDITADAS
    }
    mov_estoque = df.with_columns(pl.lit(1).alias("mov_estoque"))
    return PipelineArtefatos(
        fontes=fontes,
        mov_estoque=mov_estoque,
        mov_estoque_path=Path(f"mov_estoque_{cnpj}.parquet"),
    )
