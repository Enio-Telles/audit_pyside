"""Nivel 3 do gate: detector de colapso e tripwire downstream em mov_estoque."""
import polars as pl

MOV_ESTOQUE_TRIPWIRE_PADRAO: float = 0.01


def assert_nao_colapsou(
    baseline_principal: pl.DataFrame,
    novo_principal: pl.DataFrame,
    fonte: str,
) -> None:
    """Falha se baseline tinha linhas e novo tem zero (colapso silencioso)."""
    if baseline_principal.height > 0:
        assert novo_principal.height > 0, (
            f"[{fonte}] colapso: baseline={baseline_principal.height}, novo=0. "
            f"Output principal foi suprimido inteiro."
        )


def assert_tripwire_mov_estoque(
    baseline_mov: pl.DataFrame,
    novo_mov: pl.DataFrame,
    tolerancia: float = MOV_ESTOQUE_TRIPWIRE_PADRAO,
) -> None:
    """Tripwire de 1% no contador de mov_estoque.

    Uma reducao superior ao limite indica efeito downstream significativo
    e exige aprovacao humana explicita + ADR antes do merge.
    """
    if baseline_mov.height == 0:
        return
    delta = abs(novo_mov.height - baseline_mov.height) / baseline_mov.height
    assert delta <= tolerancia, (
        f"mov_estoque tripwire: delta={delta:.2%} > {tolerancia:.0%}. "
        f"baseline={baseline_mov.height} novo={novo_mov.height}"
    )
