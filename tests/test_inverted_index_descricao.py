import polars as pl
import pytest

from interface_grafica.services.inverted_index_descricao import (
    agrupar_por_inverted_index,
    ordenar_blocos_apenas_por_descricao,
)
from interface_grafica.services.particionamento_fiscal import (
    _construir_linhas, _resolver_coluna, COLUNAS_DESCRICAO,
)


def _construir_linhas_de(descricoes: list[str]):
    df = pl.DataFrame({
        "descr_padrao": descricoes,
    })
    col = _resolver_coluna(df, COLUNAS_DESCRICAO)
    return _construir_linhas(df, col, None, None, None, None)


def test_agrupa_descricoes_com_tokens_em_comum():
    linhas = _construir_linhas_de([
        "CAFE TORRADO MOIDO 250G",
        "CAFE TORRADO MOIDO PACOTE 250G",
        "REFRIGERANTE COCA-COLA 2L",
    ])
    componentes = agrupar_por_inverted_index(linhas, threshold=0.4)
    n_componentes_de_2_ou_mais = sum(1 for c in componentes if len(c) >= 2)
    assert n_componentes_de_2_ou_mais == 1


def test_pares_com_apenas_um_token_em_comum_nao_agrupam():
    linhas = _construir_linhas_de([
        "CAFE EXPRESSO ITALIANO",
        "MELANCIA CAFE NAO COMBINA",
    ])
    componentes = agrupar_por_inverted_index(linhas, threshold=0.5)
    assert all(len(c) == 1 for c in componentes)


def test_threshold_alto_separa_descricoes_diferentes():
    linhas = _construir_linhas_de([
        "CAFE TORRADO MOIDO 250G",
        "CAFE SOLUVEL PO 250G",
    ])
    componentes_alto = agrupar_por_inverted_index(linhas, threshold=0.95)
    n_de_2 = sum(1 for c in componentes_alto if len(c) >= 2)
    assert n_de_2 == 0


def test_funcao_publica_standalone():
    df = pl.DataFrame({
        "id_agrupado": ["1", "2", "3"],
        "descr_padrao": [
            "CAFE TORRADO MOIDO 250G",
            "CAFE TORRADO MOIDO PACOTE 250G",
            "REFRIGERANTE COCA-COLA 2L",
        ],
    })
    out = ordenar_blocos_apenas_por_descricao(df, threshold=0.4)
    assert out.height == 3
    assert "sim_bloco" in out.columns
    assert out["sim_motivo"].unique().to_list() == ["DESC_TOKENS"]


def test_dataframe_vazio():
    df = pl.DataFrame({"descr_padrao": []}, schema={"descr_padrao": pl.Utf8})
    out = ordenar_blocos_apenas_por_descricao(df)
    assert out.height == 0
    assert "sim_bloco" in out.columns
    assert "sim_motivo" in out.columns
    assert "sim_camada" in out.columns
    assert "sim_desc_norm" in out.columns


def test_lista_de_uma_unica_linha():
    linhas = _construir_linhas_de(["PRODUTO UNICO TESTE"])
    componentes = agrupar_por_inverted_index(linhas)
    assert len(componentes) == 1
    assert len(componentes[0]) == 1


def test_corpus_grande_com_tokens_genericos_podados():
    descricoes = [f"CERVEJA MARCA{i} LATA 350ML" for i in range(100)]
    descricoes += ["VINHO ESPECIAL GARRAFA 750ML"] * 2
    linhas = _construir_linhas_de(descricoes)
    componentes = agrupar_por_inverted_index(linhas, threshold=0.6)
    componentes_grandes = [c for c in componentes if len(c) >= 2]
    assert all(len(c) <= 50 for c in componentes_grandes)
