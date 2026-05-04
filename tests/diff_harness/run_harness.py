"""
run_harness.py

Compara duas implementacoes byte-a-byte nas 5 colunas invariantes.
Tolerancia: zero — qualquer diferenca e reportada.
"""
from dataclasses import dataclass, field
from typing import Callable

import polars as pl

from tests.diff_harness.golden_dataset import load_golden
from tests.diff_harness.invariantes import INVARIANTES_FISCAIS
from tests.diff_harness.nivel_1_divergencias import _eq_nan_safe

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

    for chave in INVARIANTES_FISCAIS:
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

        col_old = df_old[chave].to_list()
        col_new = df_new[chave].to_list()

        diff_indices: list[int] = []
        amostras: list[dict] = []
        for idx, (valor_old, valor_new) in enumerate(zip(col_old, col_new)):
            if _eq_nan_safe(valor_old, valor_new):
                continue
            diff_indices.append(idx)
            if len(amostras) < n_amostras:
                row_input: dict = {}
                for c in dataset.columns:
                    v = dataset[c][idx]
                    row_input[c] = _serialize(v)
                amostras.append(
                    {
                        "linha": idx,
                        "input": row_input,
                        "old": _serialize(valor_old),
                        "new": _serialize(valor_new),
                    }
                )

        report.divergentes[chave] = len(diff_indices)
        report.amostras[chave] = amostras

    return report


def _serialize(value: object) -> object:
    if isinstance(value, pl.Series):
        return value.to_list()
    return value
