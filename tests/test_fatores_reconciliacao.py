from pathlib import Path
import sys

import polars as pl

sys.path.insert(0, str(Path("src").resolve()))

from transformacao.rastreabilidade_produtos.fatores_conversao import (
    _reconciliar_fatores_existentes_com_agrupamento_atual,
    _construir_vinculo_unico_por_descricao,
)


def _df_agrupamento_canonico(ids: list[str], descrs: list[str]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id_agrupado": ids,
            "descr_padrao_canonico": descrs,
            "lista_descricoes": [[d] for d in descrs],
            "lista_desc_compl": [[] for _ in descrs],
        }
    )


def _df_fatores(
    id_agrupado: str,
    unid: str,
    fator_manual: bool,
    unid_ref_manual: bool = False,
    descr_padrao: str = "PRODUTO TESTE",
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id_agrupado": [id_agrupado],
            "unid": [unid],
            "unid_ref": ["UN"],
            "fator": [1.0],
            "fator_manual": [fator_manual],
            "unid_ref_manual": [unid_ref_manual],
            "descr_padrao": [descr_padrao],
        }
    )


def test_override_manual_preservado_quando_agrupamento_nao_muda(tmp_path: Path):
    cnpj = "88888888000100"
    pasta_analises = tmp_path / "analises"
    pasta_analises.mkdir(parents=True, exist_ok=True)

    df_existente = _df_fatores("AGR_1", "CX", fator_manual=True, descr_padrao="WHISKY BLACK")
    df_canonico = _df_agrupamento_canonico(["AGR_1"], ["WHISKY BLACK"])

    df_result = _reconciliar_fatores_existentes_com_agrupamento_atual(
        df_existente, df_canonico, pasta_analises, cnpj
    )

    assert df_result.height == 1
    assert df_result["id_agrupado"].to_list() == ["AGR_1"]
    assert df_result["fator_manual"].to_list() == [True]


def test_override_manual_remapeado_quando_descricao_mudou(tmp_path: Path):
    cnpj = "88888888000101"
    pasta_analises = tmp_path / "analises"
    pasta_analises.mkdir(parents=True, exist_ok=True)

    df_existente = _df_fatores("AGR_ANTIGO", "CX", fator_manual=True, descr_padrao="WHISKY BLACK")
    df_canonico = _df_agrupamento_canonico(
        ["AGR_NOVO"],
        ["WHISKY BLACK"],
    )

    df_result = _reconciliar_fatores_existentes_com_agrupamento_atual(
        df_existente, df_canonico, pasta_analises, cnpj
    )

    assert df_result["id_agrupado"].to_list() == ["AGR_NOVO"]
    assert df_result["fator_manual"].to_list() == [True]

    log_path = pasta_analises / f"log_reconciliacao_overrides_fatores_{cnpj}.parquet"
    assert log_path.exists()
    df_log = pl.read_parquet(log_path)
    assert df_log.filter(pl.col("acao") == "remapeado").height == 1


def test_orfao_manual_sem_descricao_correspondente_eh_descartado_e_logado(tmp_path: Path):
    # Quando id_agrupado some do canonico E descr_padrao não encontra novo match,
    # o override é removido do resultado e registrado como "descartado" no log.
    cnpj = "88888888000102"
    pasta_analises = tmp_path / "analises"
    pasta_analises.mkdir(parents=True, exist_ok=True)

    df_existente = _df_fatores("AGR_SUMIU", "CX", fator_manual=True, descr_padrao="PRODUTO RARO")
    df_canonico = _df_agrupamento_canonico(["AGR_OUTRO"], ["PRODUTO DIFERENTE"])

    df_result = _reconciliar_fatores_existentes_com_agrupamento_atual(
        df_existente, df_canonico, pasta_analises, cnpj
    )

    assert df_result.height == 0

    log_path = pasta_analises / f"log_reconciliacao_overrides_fatores_{cnpj}.parquet"
    assert log_path.exists()
    df_log = pl.read_parquet(log_path)
    assert df_log.filter(pl.col("acao") == "descartado").height == 1


def test_descricao_ambigua_nao_vincula_e_gera_log(tmp_path: Path):
    cnpj = "88888888000103"
    pasta_analises = tmp_path / "analises"
    pasta_analises.mkdir(parents=True, exist_ok=True)

    df_input = pl.DataFrame(
        {
            "descricao_normalizada": ["PRODUTO GENERICO", "PRODUTO GENERICO"],
            "id_agrupado": ["AGR_A", "AGR_B"],
            "descr_padrao_calc": ["Produto A", "Produto B"],
        }
    )

    df_result, resumo = _construir_vinculo_unico_por_descricao(
        df_input,
        "descr_padrao_calc",
        "teste",
        pasta_analises=pasta_analises,
        cnpj=cnpj,
    )

    assert df_result.is_empty()
    assert resumo["qtd_descricoes_ambiguas"] == 1
    assert (pasta_analises / f"audit_descricao_ambigua_fatores_{cnpj}.parquet").exists()
