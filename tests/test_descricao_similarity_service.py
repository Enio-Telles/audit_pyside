import polars as pl

from interface_grafica.services.descricao_similarity_service import (
    ordenar_blocos_similaridade_descricao,
)


def _df_base() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id_agrupado": ["1", "2", "3", "4"],
            "descr_padrao": [
                "CERVEJA HEINEKEN LATA 350ML",
                "ARROZ TIPO 1 5KG",
                "CERVEJA HEINEKEN 350 ML LATA",
                "CERVEJA HEINEKEN LONG NECK 330ML",
            ],
            "ncm_padrao": ["2203", "1006", "2203", "2203"],
            "cest_padrao": ["0302100", "", "0302100", "0302100"],
            "gtin_padrao": ["789000000001", "", "789000000001", "789000000099"],
        }
    )


def test_ordenacao_adiciona_indicadores_e_preserva_linhas():
    df = _df_base()
    out = ordenar_blocos_similaridade_descricao(df)

    assert out.height == df.height
    assert set(out["id_agrupado"].to_list()) == set(df["id_agrupado"].to_list())
    for col in [
        "sim_bloco",
        "sim_score",
        "sim_score_desc",
        "sim_score_tokens",
        "sim_score_numeros",
        "sim_score_ncm",
        "sim_score_cest",
        "sim_score_gtin",
        "sim_nivel",
        "sim_motivos",
        "sim_desc_norm",
        "sim_chave_ordem",
        "sim_desc_referencia",
    ]:
        assert col in out.columns


def test_descricoes_parecidas_com_mesmo_ncm_cest_gtin_recebem_score_alto():
    out = ordenar_blocos_similaridade_descricao(_df_base())
    cervejas_iguais = out.filter(pl.col("id_agrupado").is_in(["1", "3"]))

    assert cervejas_iguais["sim_score"].min() >= 90
    assert cervejas_iguais["sim_score_ncm"].drop_nulls().max() == 100
    assert cervejas_iguais["sim_score_cest"].drop_nulls().max() == 100
    assert cervejas_iguais["sim_score_gtin"].drop_nulls().max() == 100
    assert cervejas_iguais["sim_motivos"].str.contains("GTIN_IGUAL").any()


def test_ncm_com_mesmos_4_primeiros_digitos_recebe_score_parcial():
    df = pl.DataFrame(
        {
            "id_agrupado": ["1", "2"],
            "descr_padrao": [
                "BISCOITO RECHEADO CHOCOLATE 100G",
                "BISCOITO RECHEADO MORANGO 100G",
            ],
            "ncm_padrao": ["19053100", "19059090"],
            "cest_padrao": ["", ""],
            "gtin_padrao": ["", ""],
        }
    )

    out = ordenar_blocos_similaridade_descricao(df)

    assert out["sim_score_ncm"].drop_nulls().to_list() == [70, 70]
    assert out["sim_motivos"].str.contains("NCM4_IGUAL").any()


def test_blocos_por_grafo_aproximam_descricoes_com_ordem_diferente():
    df = pl.DataFrame(
        {
            "id_agrupado": ["1", "2", "3", "4"],
            "descr_padrao": [
                "CERVEJA HEINEKEN LATA 350ML",
                "ARROZ TIPO 1 5KG",
                "HEINEKEN CERVEJA 350 ML LT",
                "CERV HEINEKEN 350ML LATA",
            ],
            "ncm_padrao": ["22030000", "10063021", "22030090", "22030000"],
            "cest_padrao": ["0302100", "", "0302100", "0302100"],
            "gtin_padrao": ["7891", "", "7892", "7893"],
        }
    )

    out = ordenar_blocos_similaridade_descricao(df)
    ids = out["id_agrupado"].to_list()
    pos_cervejas = [ids.index(id_) for id_ in ["1", "3", "4"]]

    assert max(pos_cervejas) - min(pos_cervejas) == 2
    assert out.filter(pl.col("id_agrupado").is_in(["1", "3", "4"]))["sim_bloco"].n_unique() == 1


def test_pode_ignorar_priorizacao_ncm_cest():
    out = ordenar_blocos_similaridade_descricao(_df_base(), usar_ncm_cest=False)

    assert out.height == 4
    assert "sim_bloco" in out.columns


def test_base_grande_usa_particionamento_fiscal_com_colunas_compat(monkeypatch):
    import interface_grafica.services.particionamento_fiscal as particionamento
    from interface_grafica.services.descricao_similarity_service import SIM_CONFIG

    chamadas = {"n": 0}

    def fake_particionamento(df):
        chamadas["n"] += 1
        return df.with_columns(
            [
                pl.lit(1).alias("sim_bloco"),
                pl.lit("ISOLADO").alias("sim_motivo"),
                pl.lit(4).alias("sim_camada"),
                pl.lit(0).alias("sim_score"),
                pl.lit("PRODUTO").alias("sim_desc_norm"),
                pl.lit("").alias("sim_chave_fiscal"),
            ]
        )

    monkeypatch.setattr(
        particionamento,
        "ordenar_blocos_por_particionamento_fiscal",
        fake_particionamento,
    )
    original = SIM_CONFIG["fast_path_min_rows"]
    try:
        SIM_CONFIG["fast_path_min_rows"] = 3
        df = pl.DataFrame(
            {
                "id_agrupado": ["1", "2", "3"],
                "descr_padrao": ["Produto A", "Produto B", "Produto C"],
                "ncm_padrao": ["22030000", "22030000", "22030000"],
                "cest_padrao": ["0302100", "0302100", "0302100"],
                "gtin_padrao": ["7891", "7892", "7893"],
            }
        )

        out = ordenar_blocos_similaridade_descricao(df)

        assert chamadas["n"] == 1
        for col in [
            "sim_bloco",
            "sim_score",
            "sim_score_desc",
            "sim_score_tokens",
            "sim_score_numeros",
            "sim_score_ncm",
            "sim_score_cest",
            "sim_score_gtin",
            "sim_nivel",
            "sim_motivos",
            "sim_desc_norm",
            "sim_chave_ordem",
            "sim_desc_referencia",
        ]:
            assert col in out.columns
    finally:
        SIM_CONFIG["fast_path_min_rows"] = original


def test_ncm_com_mesmos_6_primeiros_digitos_recebe_score_85():
    df = pl.DataFrame(
        {
            "id_agrupado": ["1", "2"],
            "descr_padrao": [
                "BISCOITO RECHEADO CHOCOLATE 100G",
                "BISCOITO RECHEADO CHOCOLATE 200G",
            ],
            "ncm_padrao": ["19053100", "19053190"],
            "cest_padrao": ["", ""],
            "gtin_padrao": ["", ""],
        }
    )

    out = ordenar_blocos_similaridade_descricao(df)

    assert out["sim_score_ncm"].drop_nulls().to_list() == [85, 85]
    assert out["sim_motivos"].str.contains("NCM6_IGUAL").any()


def test_ncm_com_mesmos_2_primeiros_digitos_recebe_score_30():
    df = pl.DataFrame(
        {
            "id_agrupado": ["1", "2"],
            "descr_padrao": [
                "PRODUTO QUIMICO ALFA",
                "PRODUTO QUIMICO BETA",
            ],
            "ncm_padrao": ["28011000", "28499000"],
            "cest_padrao": ["", ""],
            "gtin_padrao": ["", ""],
        }
    )

    out = ordenar_blocos_similaridade_descricao(df)

    valores = out["sim_score_ncm"].drop_nulls().to_list()
    assert valores == [30, 30]
    assert out["sim_motivos"].str.contains("NCM2_IGUAL").any()


def test_ncm_sem_coincidencia_hierarquica_recebe_zero():
    df = pl.DataFrame(
        {
            "id_agrupado": ["1", "2"],
            "descr_padrao": [
                "BISCOITO 100G",
                "PARAFUSO METAL",
            ],
            "ncm_padrao": ["19053100", "73181500"],
            "cest_padrao": ["", ""],
            "gtin_padrao": ["", ""],
        }
    )

    out = ordenar_blocos_similaridade_descricao(df)

    assert out["sim_score_ncm"].drop_nulls().to_list() == [0, 0]


def test_max_bloco_size_fragmenta_blocos_muito_grandes():
    from interface_grafica.services.descricao_similarity_service import SIM_CONFIG

    descricoes = [f"CERVEJA HEINEKEN LATA 350ML PACK {i}" for i in range(8)]
    df = pl.DataFrame(
        {
            "id_agrupado": [str(i) for i in range(8)],
            "descr_padrao": descricoes,
            "ncm_padrao": ["22030000"] * 8,
            "cest_padrao": ["0302100"] * 8,
            "gtin_padrao": ["7891"] * 8,
        }
    )
    original = SIM_CONFIG["max_bloco_size"]
    try:
        SIM_CONFIG["max_bloco_size"] = 3
        out = ordenar_blocos_similaridade_descricao(df)
        n_blocos = out["sim_bloco"].n_unique()
        assert n_blocos >= 3
        max_size = (
            out.group_by("sim_bloco").len().select(pl.col("len").max()).item()
        )
        assert max_size <= 3
    finally:
        SIM_CONFIG["max_bloco_size"] = original


def test_coesao_minima_expulsa_membros_fracos():
    from interface_grafica.services.descricao_similarity_service import SIM_CONFIG

    df = pl.DataFrame(
        {
            "id_agrupado": ["1", "2", "3"],
            "descr_padrao": [
                "CERVEJA HEINEKEN LATA 350ML",
                "CERVEJA HEINEKEN 350 ML LATA",
                "AGUA MINERAL CRYSTAL 500ML SEM GAS",
            ],
            "ncm_padrao": ["22030000", "22030000", "22011000"],
            "cest_padrao": ["0302100", "0302100", ""],
            "gtin_padrao": ["7891", "7891", "7892"],
        }
    )
    original = SIM_CONFIG["min_coesao_intra_bloco"]
    try:
        SIM_CONFIG["min_coesao_intra_bloco"] = 80
        out = ordenar_blocos_similaridade_descricao(df)
        bloco_cervejas = out.filter(pl.col("id_agrupado").is_in(["1", "2"]))[
            "sim_bloco"
        ].unique()
        bloco_agua = out.filter(pl.col("id_agrupado") == "3")["sim_bloco"].unique()
        assert len(bloco_cervejas) == 1
        assert bloco_cervejas[0] != bloco_agua[0]
    finally:
        SIM_CONFIG["min_coesao_intra_bloco"] = original
