"""DifferentialReport: dataclass + renderizador no formato canonico do gate."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

__version__ = "1.0.0"


@dataclass(frozen=True)
class FonteResultado:
    fonte: str
    baseline_principal: int
    baseline_sem_id: int
    novo_principal: int
    novo_sem_id: int
    novo_fora_escopo: int
    conservacao_ok: bool
    colapso_ok: bool
    divergencias_por_invariante: dict[str, int]
    status: Literal["APROVADO", "REPROVADO"]
    motivo_reprovacao: str | None = None


@dataclass(frozen=True)
class DownstreamResultado:
    nome: str
    baseline: int
    novo: int
    delta_pct: float
    tripwire_ok: bool
    status: Literal["APROVADO", "REPROVADO"]


@dataclass
class DifferentialReport:
    pr_id: str
    cnpj: str
    baseline_commit: str
    novo_commit: str
    gerado_em: datetime
    harness_version: str
    fontes: list[FonteResultado] = field(default_factory=list)
    downstream: list[DownstreamResultado] = field(default_factory=list)
    divergencias_globais: dict[str, int] = field(default_factory=dict)
    resultado_final: Literal["APROVADO", "REPROVADO"] = "APROVADO"

    def render(self) -> str:
        linhas: list[str] = []
        linhas.append(
            f"DifferentialReport — {self.pr_id} · {self.cnpj} "
            f"· {self.baseline_commit} vs {self.novo_commit}"
        )
        linhas.append(f"Gerado em: {self.gerado_em.strftime('%Y-%m-%dT%H:%M:%SZ')}")
        linhas.append(f"Harness: tests/diff_harness/run_harness.py v{self.harness_version}")

        for fr in self.fontes:
            linhas.append(f"FONTE: {fr.fonte} " + "—" * 45)
            linhas.append(f"Baseline principal:        {fr.baseline_principal:>7} linhas")
            linhas.append(f"Baseline sem_id_agrupado:  {fr.baseline_sem_id:>7} linhas")
            linhas.append(f"Novo principal:            {fr.novo_principal:>7} linhas")
            linhas.append(f"Novo sem_id_agrupado:      {fr.novo_sem_id:>7} linhas")
            linhas.append(f"Novo fora_escopo_canonico: {fr.novo_fora_escopo:>7} linhas")
            soma_base = fr.baseline_principal + fr.baseline_sem_id
            soma_novo = fr.novo_principal + fr.novo_sem_id + fr.novo_fora_escopo
            linhas.append(
                f"Conservacao de massa:      {'OK' if fr.conservacao_ok else 'FALHOU'}"
                f" ({soma_base} == {soma_novo})"
            )
            linhas.append(f"Colapso:                   {'OK' if fr.colapso_ok else 'FALHOU'}"
                          f" (novo > 0)" if fr.novo_principal > 0 else
                          f"Colapso:                   {'OK' if fr.colapso_ok else 'FALHOU'}")
            for inv, n_div in fr.divergencias_por_invariante.items():
                intersecao = fr.novo_principal  # linhas comparadas
                linhas.append(
                    f"Divergencias {inv[:20]:<20}: {n_div} / {intersecao} intersecao"
                )
            linhas.append(f"STATUS: {fr.status}"
                          + (f": {fr.motivo_reprovacao}" if fr.motivo_reprovacao else ""))

        for dr in self.downstream:
            linhas.append(f"DOWNSTREAM: {dr.nome} " + "—" * 40)
            linhas.append(f"Baseline:                  {dr.baseline:>7} linhas")
            linhas.append(f"Novo:                      {dr.novo:>7} linhas")
            linhas.append(f"Delta:                     {dr.delta_pct:+.2%}"
                          + (" (excede tripwire 1%)" if not dr.tripwire_ok else ""))
            linhas.append(f"STATUS: {dr.status}")

        linhas.append("DIVERGENCIAS POR CHAVE (5 invariantes) " + "—" * 38)
        inv_ordem = [
            "id_agrupado", "id_agregado", "__qtd_decl_final_audit__",
            "q_conv", "q_conv_fisica",
        ]
        for inv in inv_ordem:
            v = self.divergencias_globais.get(inv, 0)
            linhas.append(f"{inv:<30}: {v}")

        linhas.append(f"RESULTADO FINAL: {self.resultado_final}")
        return "\n".join(linhas)
