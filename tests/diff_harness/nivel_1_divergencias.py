"""Nivel 1 do gate: divergencias por chave nas 5 invariantes fiscais."""
import polars as pl

from tests.diff_harness.invariantes import INVARIANTES_FISCAIS


def assert_zero_divergencias(
    baseline: pl.DataFrame,
    novo: pl.DataFrame,
    chave: list[str],
    colunas_invariantes: list[str] | None = None,
    etapa: str = "",
) -> None:
    """Falha se qualquer linha da intersecao tem invariante divergente.

    Compara somente as colunas presentes em ambos os DataFrames. Se a
    intersecao for vazia (zero linhas comuns), a guarda passa — o nivel 3
    (colapso) cobre esse caso.
    """
    if colunas_invariantes is None:
        colunas_invariantes = list(INVARIANTES_FISCAIS)

    presentes = [
        c for c in colunas_invariantes if c in baseline.columns and c in novo.columns
    ]

    chave_filtrada = [c for c in chave if c in baseline.columns and c in novo.columns]
    if not chave_filtrada:
        return

    joined = baseline.select(chave_filtrada + presentes).join(
        novo.select(chave_filtrada + presentes),
        on=chave_filtrada,
        how="inner",
        suffix="_novo",
    )

    for col in presentes:
        col_b = joined[col]
        col_n = joined[f"{col}_novo"]
        divergentes = joined.filter(
            (col_b != col_n) | (col_b.is_null() != col_n.is_null())
        )
        assert divergentes.height == 0, (
            f"[{etapa}] {divergentes.height} divergencias em '{col}'. "
            f"Exemplos:\n"
            + divergentes.head(3)
            .select([*chave_filtrada, col, f"{col}_novo"])
            .to_pandas()
            .to_string(index=False)
        )
