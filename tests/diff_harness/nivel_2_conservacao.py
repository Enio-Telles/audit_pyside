"""Nivel 2 do gate: conservacao de massa por fonte."""
import polars as pl


def assert_conservacao_de_massa(
    baseline_principal: pl.DataFrame,
    baseline_sem_id: pl.DataFrame,
    novo_principal: pl.DataFrame,
    novo_sem_id: pl.DataFrame,
    novo_fora_escopo: pl.DataFrame | None,
    fonte: str,
) -> None:
    """Falha se a soma de linhas do baseline nao bater com a soma do novo.

    Equacao esperada:
        baseline_principal + baseline_sem_id
        == novo_principal + novo_sem_id + (novo_fora_escopo ou 0)
    """
    base_total = baseline_principal.height + baseline_sem_id.height
    novo_total = (
        novo_principal.height
        + novo_sem_id.height
        + (novo_fora_escopo.height if novo_fora_escopo is not None else 0)
    )
    assert novo_total == base_total, (
        f"[{fonte}] colapso de massa: baseline={base_total} novo={novo_total}. "
        f"Detalhe: principal={novo_principal.height} sem_id={novo_sem_id.height} "
        f"fora_escopo={novo_fora_escopo.height if novo_fora_escopo is not None else 0}"
    )
