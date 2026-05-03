"""Testes do DifferentialReport: estrutura, render e estabilidade de snapshot."""
from datetime import datetime

from tests.diff_harness.differential_report import (
    DifferentialReport,
    DownstreamResultado,
    FonteResultado,
)

_GERADO_EM = datetime(2026, 5, 3, 13, 45, 0)


def _report_aprovado() -> DifferentialReport:
    return DifferentialReport(
        pr_id="#196",
        cnpj="84654326000394",
        baseline_commit="40daa7f",
        novo_commit="4bd4bf2",
        gerado_em=_GERADO_EM,
        harness_version="1.0.0",
        fontes=[
            FonteResultado(
                fonte="nfe",
                baseline_principal=23111,
                baseline_sem_id=1008,
                novo_principal=19171,
                novo_sem_id=0,
                novo_fora_escopo=4948,
                conservacao_ok=True,
                colapso_ok=True,
                divergencias_por_invariante={"id_agrupado": 0},
                status="APROVADO",
            ),
            FonteResultado(
                fonte="nfce",
                baseline_principal=42,
                baseline_sem_id=20,
                novo_principal=42,
                novo_sem_id=20,
                novo_fora_escopo=0,
                conservacao_ok=True,
                colapso_ok=True,
                divergencias_por_invariante={"id_agrupado": 0},
                status="APROVADO",
            ),
        ],
        downstream=[],
        divergencias_globais={
            "id_agrupado": 0,
            "id_agregado": 0,
            "__qtd_decl_final_audit__": 0,
            "q_conv": 0,
            "q_conv_fisica": 0,
        },
        resultado_final="APROVADO",
    )


def test_render_contem_campos_essenciais() -> None:
    txt = _report_aprovado().render()
    assert "#196" in txt
    assert "84654326000394" in txt
    assert "40daa7f" in txt
    assert "4bd4bf2" in txt
    assert "APROVADO" in txt
    assert "nfe" in txt
    assert "nfce" in txt


def test_render_contem_invariantes() -> None:
    txt = _report_aprovado().render()
    assert "id_agrupado" in txt
    assert "q_conv" in txt


def test_render_estavel_byte_a_byte() -> None:
    r = _report_aprovado()
    assert r.render() == r.render()


def test_render_reprovado_indica_reprovado() -> None:
    r = _report_aprovado()
    r.resultado_final = "REPROVADO"
    assert "REPROVADO" in r.render()


def test_downstream_aparece_no_render() -> None:
    r = _report_aprovado()
    r.downstream.append(
        DownstreamResultado(
            nome="mov_estoque",
            baseline=29967,
            novo=29395,
            delta_pct=-0.0191,
            tripwire_ok=False,
            status="REPROVADO",
        )
    )
    txt = r.render()
    assert "mov_estoque" in txt
    assert "REPROVADO" in txt
    assert "excede tripwire" in txt
