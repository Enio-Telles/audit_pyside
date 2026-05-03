"""CLI do differential harness.

Uso:
    uv run python tests/diff_harness/run_harness_cli.py \\
        --baseline-commit <sha> \\
        --novo-commit HEAD \\
        --cnpj 04240370002877 \\
        --cnpj 84654326000394 \\
        --pasta-dados dados/CNPJ \\
        --out reports/diff/ \\
        [--dry-run]

Exit code 0 somente se todos os CNPJs aprovarem nos 3 niveis.
Exit code 1 se qualquer um reprovar.
"""
from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from tests.diff_harness.differential_report import (
    DifferentialReport,
    DownstreamResultado,
    FonteResultado,
)
from tests.diff_harness.invariantes import CHAVES_POR_ETAPA, FONTES_AUDITADAS, INVARIANTES_FISCAIS
from tests.diff_harness.nivel_1_divergencias import assert_zero_divergencias
from tests.diff_harness.nivel_2_conservacao import assert_conservacao_de_massa
from tests.diff_harness.nivel_3_colapso_tripwire import (
    assert_nao_colapsou,
    assert_tripwire_mov_estoque,
)
from tests.diff_harness.pipeline_runner import rodar_pipeline_em_commit


def _vazio(schema: dict[str, type] | None = None) -> pl.DataFrame:
    if schema:
        return pl.DataFrame(schema={k: v for k, v in schema.items()})
    return pl.DataFrame({"chave_acesso": pl.Series([], dtype=pl.Utf8)})


def _executar_cnpj(
    cnpj: str,
    baseline_commit: str,
    novo_commit: str,
    pasta_dados: Path,
    pasta_saida: Path,
    dry_run: bool,
) -> DifferentialReport:
    artefatos_base = rodar_pipeline_em_commit(
        baseline_commit, cnpj, pasta_dados, pasta_saida / "baseline", dry_run=dry_run
    )
    artefatos_novo = rodar_pipeline_em_commit(
        novo_commit, cnpj, pasta_dados, pasta_saida / "novo", dry_run=dry_run
    )

    fontes: list[FonteResultado] = []
    divergencias_globais: dict[str, int] = {inv: 0 for inv in INVARIANTES_FISCAIS}
    resultado_final = "APROVADO"

    for fonte in FONTES_AUDITADAS:
        etapa = f"{fonte}_agr"
        chave = list(CHAVES_POR_ETAPA.get(etapa, ("chave_acesso", "prod_nitem")))

        b_principal = artefatos_base.get(etapa, _vazio())
        n_principal = artefatos_novo.get(etapa, _vazio())
        b_sem_id = _vazio()
        n_sem_id = _vazio()
        n_fora = _vazio()

        motivo = None
        status_fonte = "APROVADO"

        try:
            assert_zero_divergencias(b_principal, n_principal, chave=chave, etapa=etapa)
        except AssertionError as exc:
            status_fonte = "REPROVADO"
            motivo = str(exc)[:200]
            resultado_final = "REPROVADO"

        try:
            assert_conservacao_de_massa(b_principal, b_sem_id, n_principal, n_sem_id, n_fora, fonte)
        except AssertionError as exc:
            status_fonte = "REPROVADO"
            motivo = (motivo or "") + " | " + str(exc)[:200]
            resultado_final = "REPROVADO"

        try:
            assert_nao_colapsou(b_principal, n_principal, fonte=fonte)
        except AssertionError as exc:
            status_fonte = "REPROVADO"
            motivo = (motivo or "") + " | " + str(exc)[:200]
            resultado_final = "REPROVADO"

        inv_presentes = [c for c in INVARIANTES_FISCAIS if c in b_principal.columns and c in n_principal.columns]
        chave_valida = [c for c in chave if c in b_principal.columns and c in n_principal.columns]
        divs: dict[str, int] = {}
        if inv_presentes and chave_valida:
            joined = b_principal.select(chave_valida + inv_presentes).join(
                n_principal.select(chave_valida + inv_presentes), on=chave_valida, how="inner", suffix="_novo"
            )
            for inv in inv_presentes:
                col_b = joined[inv]
                col_n = joined[f"{inv}_novo"]
                mask = (col_b != col_n) | (col_b.is_null() != col_n.is_null())
                n_div = int(mask.sum())
                divs[inv] = n_div
                divergencias_globais[inv] = divergencias_globais.get(inv, 0) + n_div

        fontes.append(FonteResultado(
            fonte=fonte,
            baseline_principal=b_principal.height,
            baseline_sem_id=b_sem_id.height,
            novo_principal=n_principal.height,
            novo_sem_id=n_sem_id.height,
            novo_fora_escopo=n_fora.height,
            conservacao_ok=(status_fonte == "APROVADO" or "massa" not in (motivo or "")),
            colapso_ok=n_principal.height > 0 or b_principal.height == 0,
            divergencias_por_invariante=divs,
            status=status_fonte,
            motivo_reprovacao=motivo,
        ))

    downstream: list[DownstreamResultado] = []
    mov_base = artefatos_base.get("movimentacao_estoque", _vazio())
    mov_novo = artefatos_novo.get("movimentacao_estoque", _vazio())
    if mov_base.height > 0:
        delta = (mov_novo.height - mov_base.height) / mov_base.height
        tripwire_ok = abs(delta) <= 0.01
        if not tripwire_ok:
            resultado_final = "REPROVADO"
        downstream.append(DownstreamResultado(
            nome="mov_estoque",
            baseline=mov_base.height,
            novo=mov_novo.height,
            delta_pct=delta,
            tripwire_ok=tripwire_ok,
            status="APROVADO" if tripwire_ok else "REPROVADO",
        ))

    return DifferentialReport(
        pr_id="CLI",
        cnpj=cnpj,
        baseline_commit=baseline_commit,
        novo_commit=novo_commit,
        gerado_em=datetime.now(tz=UTC).replace(tzinfo=None),
        harness_version="1.0.0",
        fontes=fontes,
        downstream=downstream,
        divergencias_globais=divergencias_globais,
        resultado_final=resultado_final,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Differential harness CLI")
    parser.add_argument("--baseline-commit", default="HEAD~1")
    parser.add_argument("--novo-commit", default="HEAD")
    parser.add_argument("--cnpj", action="append", dest="cnpjs", default=[])
    parser.add_argument("--pasta-dados", type=Path, default=Path("dados/CNPJ"))
    parser.add_argument("--out", type=Path, default=Path("reports/diff"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    if not args.cnpjs:
        args.cnpjs = ["99999999999999"]

    args.out.mkdir(parents=True, exist_ok=True)

    reprovados: list[str] = []
    for cnpj in args.cnpjs:
        relatorio = _executar_cnpj(
            cnpj=cnpj,
            baseline_commit=args.baseline_commit,
            novo_commit=args.novo_commit,
            pasta_dados=args.pasta_dados,
            pasta_saida=args.out / "pipeline",
            dry_run=args.dry_run,
        )
        relatorio.pr_id = f"CLI-{cnpj}"
        txt = relatorio.render()
        destino = args.out / f"diff-{cnpj}.txt"
        destino.write_text(txt, encoding="utf-8")
        print(txt)
        if relatorio.resultado_final == "REPROVADO":
            reprovados.append(cnpj)

    if reprovados:
        print(f"\nREPROVADO em: {reprovados}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
