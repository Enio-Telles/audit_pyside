"""
run_harness.py

Compara duas implementacoes byte-a-byte nas 5 colunas invariantes.
Tolerancia: zero — qualquer diferenca e reportada.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import polars as pl

from tests.diff_harness.golden_dataset import INVARIANTS, load_golden

ImplFn = Callable[[pl.DataFrame], pl.DataFrame]


@dataclass
class DifferentialReport:
    total_rows: int
    divergentes: dict[str, int] = field(default_factory=dict)
    amostras: dict[str, list[dict]] = field(default_factory=dict)

    @property
    def tem_divergencia(self) -> bool:
        return any(v > 0 for v in self.divergentes.values())

    def resumo(self) -> str:
        if not self.tem_divergencia:
            return f"OK: {self.total_rows} linhas, 0 divergencias em todas as {len(self.divergentes)} chaves."
        partes = [f"DIVERGENCIAS em {self.total_rows} linhas:"]
        for chave, n in self.divergentes.items():
            if n > 0:
                partes.append(f"  {chave}: {n} linhas divergentes")
        return "\n".join(partes)


def run_harness(
    impl_old: ImplFn,
    impl_new: ImplFn,
    dataset: pl.DataFrame | None = None,
    n_amostras: int = 10,
) -> DifferentialReport:
    """
    Executa impl_old e impl_new sobre dataset e compara as 5 chaves invariantes.

    Argumentos:
        impl_old: implementacao de referencia (geralmente a versao antes da mudanca)
        impl_new: implementacao nova (a ser validada)
        dataset: DataFrame de entrada; se None, usa load_golden()
        n_amostras: numero maximo de linhas divergentes por chave a incluir no relatorio

    Retorna:
        DifferentialReport com contagem e amostras por chave
    """
    if dataset is None:
        dataset = load_golden()

    df_old = impl_old(dataset)
    df_new = impl_new(dataset)

    if len(df_old) != len(df_new):
        raise ValueError(
            f"Implementacoes retornam DataFrames com tamanhos diferentes: "
            f"old={len(df_old)}, new={len(df_new)}"
        )

    n = len(df_old)
    report = DifferentialReport(total_rows=n)

    for chave in INVARIANTS:
        old_has = chave in df_old.columns
        new_has = chave in df_new.columns

        if not old_has and not new_has:
            report.divergentes[chave] = 0
            report.amostras[chave] = []
            continue

        if old_has != new_has:
            report.divergentes[chave] = n
            report.amostras[chave] = [
                {
                    "erro": f"coluna presente em {'old' if old_has else 'new'} mas ausente em {'new' if old_has else 'old'}"
                }
            ]
            continue

        col_old = df_old[chave]
        col_new = df_new[chave]

        if col_old.equals(col_new, null_equal=True):
            report.divergentes[chave] = 0
            report.amostras[chave] = []
            continue

        mask_diff = (col_old != col_new) | (col_old.is_null() != col_new.is_null())
        n_diff = int(mask_diff.sum())
        report.divergentes[chave] = n_diff

        idx_diff = mask_diff.arg_true().head(n_amostras).to_list()
        amostras: list[dict] = []
        for idx in idx_diff:
            row_input: dict = {}
            for c in dataset.columns:
                v = dataset[c][idx]
                row_input[c] = v if not isinstance(v, pl.Series) else v.to_list()
            amostras.append(
                {
                    "linha": idx,
                    "input": row_input,
                    "old": _serialize(col_old[idx]),
                    "new": _serialize(col_new[idx]),
                }
            )
        report.amostras[chave] = amostras

    return report


def _serialize(value: object) -> object:
    if isinstance(value, pl.Series):
        return value.to_list()
    return value
