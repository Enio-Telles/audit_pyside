from pathlib import Path
import sys

import polars as pl

sys.path.insert(0, str(Path("src").resolve()))

from transformacao.rastreabilidade_produtos._produtos_final_impl import (
    _aplicar_agrupamento_manual,
    _construir_tabela_ponte,
)


def _df_descricoes(ids: list[str], descs: list[str]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id_descricao": ids,
            "descricao_normalizada": descs,
            "descricao": descs,
            "criterio_agrupamento": ["automatico_descricao_normalizada"] * len(ids),
            "origem_agrupamento": ["automatico"] * len(ids),
            "id_agrupado": ids,
        }
    )


def test_mapeamento_manual_tem_precedencia_sobre_automatico(tmp_path: Path):
    cnpj = "99999999000100"
    pasta_analises = tmp_path / cnpj / "analises" / "produtos"
    pasta_analises.mkdir(parents=True, exist_ok=True)

    df_desc = _df_descricoes(["id_desc_1", "id_desc_2"], ["PRODUTO A", "PRODUTO B"])

    pl.DataFrame(
        {
            "id_descricao": ["id_desc_1"],
            "id_agrupado": ["AGR_MANUAL_X"],
        }
    ).write_parquet(pasta_analises / f"mapa_agrupamento_manual_{cnpj}.parquet")

    df_result = _aplicar_agrupamento_manual(df_desc, pasta_analises, cnpj)

    row_a = df_result.filter(pl.col("id_descricao") == "id_desc_1")
    assert row_a["id_agrupado"].to_list() == ["AGR_MANUAL_X"]
    assert row_a["criterio_agrupamento"].to_list() == ["manual"]


def test_fallback_quando_id_descricao_nao_esta_no_mapa_manual(tmp_path: Path):
    cnpj = "99999999000101"
    pasta_analises = tmp_path / cnpj / "analises" / "produtos"
    pasta_analises.mkdir(parents=True, exist_ok=True)

    df_desc = _df_descricoes(["id_desc_1", "id_desc_2"], ["PRODUTO A", "PRODUTO B"])

    pl.DataFrame(
        {
            "id_descricao": ["id_desc_1"],
            "id_agrupado": ["AGR_MANUAL_X"],
        }
    ).write_parquet(pasta_analises / f"mapa_agrupamento_manual_{cnpj}.parquet")

    df_result = _aplicar_agrupamento_manual(df_desc, pasta_analises, cnpj)

    row_b = df_result.filter(pl.col("id_descricao") == "id_desc_2")
    assert row_b["id_agrupado"].to_list() == ["id_desc_2"]
    assert row_b["criterio_agrupamento"].to_list() == ["automatico_descricao_normalizada"]


def test_sem_mapa_manual_nao_altera_df(tmp_path: Path):
    cnpj = "99999999000102"
    pasta_analises = tmp_path / cnpj / "analises" / "produtos"
    pasta_analises.mkdir(parents=True, exist_ok=True)

    df_desc = _df_descricoes(["id_desc_1"], ["PRODUTO A"])

    df_result = _aplicar_agrupamento_manual(df_desc, pasta_analises, cnpj)

    assert df_result["id_agrupado"].to_list() == ["id_desc_1"]


def test_tabela_ponte_sem_lista_codigo_fonte_gera_codigo_fonte_nulo(capsys):
    df_desc = pl.DataFrame(
        {
            "id_descricao": ["id_desc_1"],
            "id_agrupado": ["AGR_1"],
            "descricao_normalizada": ["PRODUTO A"],
        }
    )

    df_ponte = _construir_tabela_ponte(df_desc)

    assert df_ponte["codigo_fonte"].is_null().all()
    captured = capsys.readouterr()
    assert "lista_codigo_fonte" in captured.out


def test_tabela_ponte_com_lista_codigo_fonte_explode_corretamente():
    df_desc = pl.DataFrame(
        {
            "id_descricao": ["id_desc_1"],
            "id_agrupado": ["AGR_1"],
            "descricao_normalizada": ["PRODUTO A"],
            "lista_codigo_fonte": [["111|001", "222|001"]],
        }
    )

    df_ponte = _construir_tabela_ponte(df_desc)

    assert df_ponte.height == 2
    assert set(df_ponte["codigo_fonte"].to_list()) == {"111|001", "222|001"}
