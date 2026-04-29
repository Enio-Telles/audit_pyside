import polars as pl
import pytest

from interface_grafica.services.particionamento_fiscal import (
    ordenar_blocos_por_particionamento_fiscal,
)


def _df_basico() -> pl.DataFrame:
    return pl.DataFrame({
        "id_agrupado": ["1", "2", "3", "4", "5"],
        "descr_padrao": [
            "CERVEJA HEINEKEN LATA 350ML",
            "ARROZ TIPO 1 5KG",
            "CERVEJA HEINEKEN 350 ML LATA",
            "CERVEJA HEINEKEN LONG NECK 330ML",
            "BISCOITO RECHEADO MORANGO 100G",
        ],
        "ncm_padrao": ["22030000", "10063021", "22030000", "22030000", "19053100"],
        "cest_padrao": ["0302100", "", "0302100", "0302100", ""],
        "gtin_padrao": ["7891001", "", "7891001", "7891999", ""],
        "unid_padrao": ["UN", "KG", "UN", "UN", "PCT"],
    })


def test_camada_0_gtin_igual_forma_bloco_automatico():
    df = _df_basico()
    out = ordenar_blocos_por_particionamento_fiscal(df)
    bloco_gtin = out.filter(
        pl.col("id_agrupado").is_in(["1", "3"])
    )["sim_bloco"]
    assert bloco_gtin.n_unique() == 1
    assert (
        out.filter(pl.col("id_agrupado").is_in(["1", "3"]))["sim_motivo"]
        .unique().to_list()[0]
    ) == "GTIN_IGUAL"


def test_camada_1_ncm_cest_unidade_iguais():
    df = _df_basico()
    out = ordenar_blocos_por_particionamento_fiscal(df)
    item_4 = out.filter(pl.col("id_agrupado") == "4")
    assert item_4["sim_motivo"].to_list()[0] != "GTIN_IGUAL"


def test_camada_4_residual_para_itens_isolados():
    df = pl.DataFrame({
        "id_agrupado": ["1"],
        "descr_padrao": ["PRODUTO UNICO"],
        "ncm_padrao": ["12345678"],
        "cest_padrao": [""],
        "gtin_padrao": [""],
        "unid_padrao": ["UN"],
    })
    out = ordenar_blocos_por_particionamento_fiscal(df)
    assert out["sim_motivo"].to_list() == ["ISOLADO"]
    assert out["sim_camada"].to_list() == [4]


def test_preserva_todas_as_linhas():
    df = _df_basico()
    out = ordenar_blocos_por_particionamento_fiscal(df)
    assert out.height == df.height
    assert set(out["id_agrupado"].to_list()) == set(df["id_agrupado"].to_list())


def test_colunas_indicadoras_adicionadas():
    df = _df_basico()
    out = ordenar_blocos_por_particionamento_fiscal(df)
    for col in ["sim_bloco", "sim_motivo", "sim_camada", "sim_score",
                "sim_desc_norm", "sim_chave_fiscal"]:
        assert col in out.columns


def test_camada_5_desativada_por_default():
    df = pl.DataFrame({
        "id_agrupado": ["1", "2"],
        "descr_padrao": ["CAFE EM PO 250G", "CAFE TORRADO MOIDO 250G"],
        "ncm_padrao": ["", ""],
        "cest_padrao": ["", ""],
        "gtin_padrao": ["", ""],
        "unid_padrao": ["", ""],
    })
    out = ordenar_blocos_por_particionamento_fiscal(df)
    assert set(out["sim_motivo"].unique().to_list()) == {"ISOLADO"}


def test_camada_5_ativa_agrupa_por_descricao():
    df = pl.DataFrame({
        "id_agrupado": ["1", "2", "3"],
        "descr_padrao": [
            "CAFE TORRADO MOIDO 250G",
            "CAFE TORRADO MOIDO PACOTE 250G",
            "REFRIGERANTE COCA-COLA 2L",
        ],
        "ncm_padrao": ["", "", ""],
        "cest_padrao": ["", "", ""],
        "gtin_padrao": ["", "", ""],
        "unid_padrao": ["", "", ""],
    })
    out = ordenar_blocos_por_particionamento_fiscal(
        df, incluir_camada_so_descricao=True,
    )
    bloco_cafe = out.filter(
        pl.col("id_agrupado").is_in(["1", "2"])
    )["sim_bloco"].n_unique()
    assert bloco_cafe == 1


def test_thresholds_customizaveis():
    df = _df_basico()
    out = ordenar_blocos_por_particionamento_fiscal(
        df, thresholds={"camada_1": 99},
    )
    assert out.height == df.height
