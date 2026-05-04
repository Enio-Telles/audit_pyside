"""CLI do differential harness."""

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tests.diff_harness.differential_report import (
    DifferentialReport,
    DownstreamResultado,
    FonteResultado,
    StatusFonteAnalise,
    StatusFonte,
    __version__ as HARNESS_VERSION,
)
from tests.diff_harness.invariantes import CHAVES_POR_ETAPA, FONTES_AUDITADAS, INVARIANTES_FISCAIS
from tests.diff_harness.nivel_1_divergencias import resumir_divergencias_por_chave
from tests.diff_harness.nivel_2_conservacao import assert_conservacao_de_massa
from tests.diff_harness.nivel_3_colapso_tripwire import (
    assert_nao_colapsou,
    assert_tripwire_mov_estoque,
)
from tests.diff_harness.pipeline_runner import PipelineArtefatos, rodar_pipeline_em_commit


def _executar_cnpj(
    cnpj: str,
    baseline_commit: str,
    novo_commit: str,
    pasta_dados: Path,
    pasta_saida: Path,
    dry_run: bool,
) -> DifferentialReport:
    artefatos_base = rodar_pipeline_em_commit(
        baseline_commit,
        cnpj,
        pasta_dados,
        pasta_saida / "baseline",
        dry_run=dry_run,
    )
    artefatos_novo = rodar_pipeline_em_commit(
        novo_commit,
        cnpj,
        pasta_dados,
        pasta_saida / "novo",
        dry_run=dry_run,
    )

    return _montar_relatorio(
        cnpj=cnpj,
        baseline_commit=baseline_commit,
        novo_commit=novo_commit,
        artefatos_base=artefatos_base,
        artefatos_novo=artefatos_novo,
    )


def _montar_relatorio(
    cnpj: str,
    baseline_commit: str,
    novo_commit: str,
    artefatos_base: PipelineArtefatos,
    artefatos_novo: PipelineArtefatos,
) -> DifferentialReport:
    fontes: list[FonteResultado] = []
    statuses_por_fonte: list[StatusFonteAnalise] = []
    divergencias_globais: dict[str, int] = {inv: 0 for inv in INVARIANTES_FISCAIS}
    tripwire_mov_estoque = True

    for fonte in FONTES_AUDITADAS:
        etapa = f"{fonte}_agr"
        chave = list(CHAVES_POR_ETAPA.get(etapa, ("chave_acesso", "prod_nitem")))

        fonte_base = artefatos_base.fontes[fonte]
        fonte_novo = artefatos_novo.fontes[fonte]

        divergencias, amostras = resumir_divergencias_por_chave(
            fonte_base.principal,
            fonte_novo.principal,
            chave=chave,
            colunas_invariantes=list(INVARIANTES_FISCAIS),
            n_amostras=10,
        )
        for inv, valor in divergencias.items():
            divergencias_globais[inv] = divergencias_globais.get(inv, 0) + valor

        status_fonte = StatusFonte.APROVADO
        motivo: list[str] = []

        try:
            assert_conservacao_de_massa(
                fonte_base.principal,
                fonte_base.sem_id,
                fonte_novo.principal,
                fonte_novo.sem_id,
                fonte_novo.fora_escopo,
                fonte,
            )
            conservacao_ok = True
        except AssertionError as exc:
            conservacao_ok = False
            status_fonte = StatusFonte.REPROVADO
            motivo.append(str(exc))

        try:
            assert_nao_colapsou(fonte_base.principal, fonte_novo.principal, fonte=fonte)
            colapso_ok = True
        except AssertionError as exc:
            colapso_ok = False
            status_fonte = StatusFonte.REPROVADO
            motivo.append(str(exc))

        if any(valor > 0 for valor in divergencias.values()):
            status_fonte = StatusFonte.REPROVADO
            motivo.append(
                "; ".join(f"{inv}={valor}" for inv, valor in divergencias.items() if valor > 0)
            )

        statuses_por_fonte.append(
            StatusFonteAnalise(
                fonte=fonte,
                nivel_1=not any(valor > 0 for valor in divergencias.values()),
                nivel_2=conservacao_ok,
                nivel_3=colapso_ok,
            )
        )

        fontes.append(
            FonteResultado(
                fonte=fonte,
                baseline_principal=fonte_base.principal.height,
                baseline_sem_id=fonte_base.sem_id.height,
                novo_principal=fonte_novo.principal.height,
                novo_sem_id=fonte_novo.sem_id.height,
                novo_fora_escopo=fonte_novo.fora_escopo.height,
                conservacao_ok=conservacao_ok,
                colapso_ok=colapso_ok,
                divergencias_por_invariante=divergencias,
                status=status_fonte,
                motivo_reprovacao=" | ".join(motivo) if motivo else None,
            )
        )

    downstream: list[DownstreamResultado] = []
    mov_base = artefatos_base.mov_estoque
    mov_novo = artefatos_novo.mov_estoque
    if mov_base.height > 0:
        delta = (mov_novo.height - mov_base.height) / mov_base.height
        tripwire_ok = abs(delta) <= 0.01
        tripwire_mov_estoque = tripwire_ok
        downstream.append(
            DownstreamResultado(
                nome="mov_estoque",
                baseline=mov_base.height,
                novo=mov_novo.height,
                delta_pct=delta,
                tripwire_ok=tripwire_ok,
                status=StatusFonte.APROVADO if tripwire_ok else StatusFonte.REPROVADO,
            )
        )
        if not tripwire_ok:
            resultado_final = StatusFonte.REPROVADO

        try:
            assert_tripwire_mov_estoque(mov_base, mov_novo)
        except AssertionError:
            tripwire_mov_estoque = False

    aprovado_global = all(status.aprovado for status in statuses_por_fonte) and tripwire_mov_estoque
    resultado_final = StatusFonte.APROVADO if aprovado_global else StatusFonte.REPROVADO

    return DifferentialReport(
        pr_id="CLI",
        cnpj=cnpj,
        baseline_commit=baseline_commit,
        novo_commit=novo_commit,
        gerado_em=datetime.now(tz=UTC),
        harness_version=HARNESS_VERSION,
        statuses_por_fonte=statuses_por_fonte,
        tripwire_mov_estoque=tripwire_mov_estoque,
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
        args.cnpjs = ["04240370002877", "84654326000394"]

    args.out.mkdir(parents=True, exist_ok=True)

    reprovados: list[str] = []
    for cnpj in args.cnpjs:
        try:
            relatorio = _executar_cnpj(
                cnpj=cnpj,
                baseline_commit=args.baseline_commit,
                novo_commit=args.novo_commit,
                pasta_dados=args.pasta_dados,
                pasta_saida=args.out / "pipeline",
                dry_run=args.dry_run,
            )
        except Exception as exc:  # pragma: no cover - caminho operacional
            print(f"Erro ao executar harness para {cnpj}: {exc}", file=sys.stderr)
            reprovados.append(cnpj)
            continue

        txt = relatorio.render()
        destino = args.out / f"diff-{cnpj}.txt"
        destino.write_text(txt, encoding="utf-8")
        print(txt)
        if not relatorio.aprovado_global:
            reprovados.append(cnpj)

    if reprovados:
        print(f"\nREPROVADO em: {reprovados}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
