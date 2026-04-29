from __future__ import annotations

import pytest

from utilitarios.codigo_fonte import gerar_codigo_fonte, normalizar_codigo_fonte


@pytest.mark.parametrize(
    ("cnpj", "codigo", "expected"),
    [("11.222.333/0001-81", "NFe", "11222333000181|NFe"), (None, "EFD", "EFD"), ("11222333000181", "", None), (None, None, None)],
)
def test_gerar_codigo_fonte(cnpj: str | None, codigo: str | None, expected: str | None) -> None:
    assert gerar_codigo_fonte(cnpj, codigo) == expected


@pytest.mark.parametrize(
    ("codigo", "expected"),
    [("11222333000181|NFe", "11222333000181|NFe"), ("EFD", "EFD"), ("", None), (None, None), ("|NFe", "NFe")],
)
def test_normalizar_codigo_fonte(codigo: str | None, expected: str | None) -> None:
    assert normalizar_codigo_fonte(codigo) == expected
