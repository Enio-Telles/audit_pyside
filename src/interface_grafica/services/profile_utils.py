from __future__ import annotations


def ordenar_colunas_perfil(
    available_columns: list[str],
    visible_columns: list[str] | None,
    column_order: list[str] | None = None,
) -> list[str]:
    disponiveis = [str(col) for col in available_columns]
    disponiveis_set = set(disponiveis)

    visiveis = [str(col) for col in (visible_columns or []) if str(col) in disponiveis_set]
    if not visiveis:
        return []

    visiveis_set = set(visiveis)
    ordem = [str(col) for col in (column_order or []) if str(col) in visiveis_set]
    faltantes = [col for col in visiveis if col not in ordem]
    return ordem + faltantes


def ordenar_colunas_visiveis(
    available_columns: list[str],
    visible_columns: list[str] | None,
    visual_order: list[str] | None = None,
) -> list[str]:
    return ordenar_colunas_perfil(
        available_columns=available_columns,
        visible_columns=visible_columns,
        column_order=visual_order,
    )


__all__ = ["ordenar_colunas_perfil", "ordenar_colunas_visiveis"]
