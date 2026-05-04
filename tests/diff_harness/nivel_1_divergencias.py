"""Nivel 1 do gate: divergencias por chave nas 5 invariantes fiscais."""

import math

import polars as pl

from tests.diff_harness.invariantes import INVARIANTES_FISCAIS


def _eq_nan_safe(left: object, right: object) -> bool:
    """Compara valores escalares tratando NaN como igual a NaN."""

    if left is None and right is None:
        return True
    if isinstance(left, float) and isinstance(right, float):
        if math.isnan(left) and math.isnan(right):
            return True
    return left == right


def resumir_divergencias_por_chave(
    baseline: pl.DataFrame,
    novo: pl.DataFrame,
    chave: list[str],
    colunas_invariantes: list[str] | None = None,
    n_amostras: int = 10,
) -> tuple[dict[str, int], dict[str, list[dict[str, object]]]]:
    """Retorna contagem de divergencias e amostras por invariante."""

    if colunas_invariantes is None:
        colunas_invariantes = list(INVARIANTES_FISCAIS)

    presentes = [c for c in colunas_invariantes if c in baseline.columns and c in novo.columns]
    chave_filtrada = [c for c in chave if c in baseline.columns and c in novo.columns]

    divergentes: dict[str, int] = {col: 0 for col in colunas_invariantes}
    amostras: dict[str, list[dict[str, object]]] = {col: [] for col in colunas_invariantes}

    if not chave_filtrada:
        return divergentes, amostras

    if not presentes:
        return divergentes, amostras

    joined = baseline.select(chave_filtrada + presentes).join(
        novo.select(chave_filtrada + presentes),
        on=chave_filtrada,
        how="inner",
        suffix="_novo",
    )

    for col in presentes:
        valores_baseline = joined[col].to_list()
        valores_novo = joined[f"{col}_novo"].to_list()
        diferencas: list[int] = []
        exemplos: list[dict[str, object]] = []

        for idx, (valor_baseline, valor_novo) in enumerate(zip(valores_baseline, valores_novo)):
            if _eq_nan_safe(valor_baseline, valor_novo):
                continue
            diferencas.append(idx)
            if len(exemplos) < n_amostras:
                exemplo = {chave_nome: joined[chave_nome][idx] for chave_nome in chave_filtrada}
                exemplo[col] = valor_baseline
                exemplo[f"{col}_novo"] = valor_novo
                exemplos.append(exemplo)

        divergentes[col] = len(diferencas)
        amostras[col] = exemplos

    for col in colunas_invariantes:
        if col not in presentes and col in baseline.columns and col not in novo.columns:
            divergentes[col] = max(len(baseline), len(novo))
            amostras[col] = [
                {
                    "erro": f"coluna presente em old mas ausente em new: {col}",
                }
            ]
        elif col not in presentes and col in novo.columns and col not in baseline.columns:
            divergentes[col] = max(len(baseline), len(novo))
            amostras[col] = [
                {
                    "erro": f"coluna presente em new mas ausente em old: {col}",
                }
            ]

    return divergentes, amostras


def assert_zero_divergencias(
    baseline: pl.DataFrame,
    novo: pl.DataFrame,
    chave: list[str],
    colunas_invariantes: list[str] | None = None,
    etapa: str = "",
) -> None:
    """Falha se qualquer linha da intersecao tem invariante divergente."""

    divergentes, amostras = resumir_divergencias_por_chave(
        baseline,
        novo,
        chave=chave,
        colunas_invariantes=colunas_invariantes,
    )

    problemas = {col: n for col, n in divergentes.items() if n > 0}
    if not problemas:
        return

    mensagens: list[str] = []
    for col, n in problemas.items():
        exemplos = amostras.get(col, [])[:3]
        mensagens.append(f"[{etapa}] {n} divergencias em '{col}'. Exemplos: {exemplos}")
    raise AssertionError("\n".join(mensagens))
