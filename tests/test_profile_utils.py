from pathlib import Path
import sys

sys.path.insert(0, str(Path("src").resolve()))

from interface_grafica.services.profile_utils import ordenar_colunas_perfil, ordenar_colunas_visiveis


def test_ordenar_colunas_perfil_prioriza_ordem_salva():
    resultado = ordenar_colunas_perfil(
        ["col_a", "col_b", "col_c"],
        ["col_a", "col_b", "col_c"],
        ["col_c", "col_a", "col_b"],
    )

    assert resultado == ["col_c", "col_a", "col_b"]


def test_ordenar_colunas_perfil_preserva_restantes_visiveis():
    resultado = ordenar_colunas_perfil(
        ["col_a", "col_b", "col_c", "col_d"],
        ["col_a", "col_b", "col_d"],
        ["col_d", "col_b"],
    )

    assert resultado == ["col_d", "col_b", "col_a"]


def test_ordenar_colunas_visiveis_respeita_ordem_visual_atual():
    resultado = ordenar_colunas_visiveis(
        ["col_a", "col_b", "col_c", "col_d"],
        ["col_a", "col_c", "col_d"],
        ["col_d", "col_a", "col_c"],
    )

    assert resultado == ["col_d", "col_a", "col_c"]
