from datetime import date
from pathlib import Path
import sys

import polars as pl

sys.path.insert(0, str(Path("src/transformacao").resolve()))

import co_sefin_class as mod


def _configurar_ambiente_tmp(tmp_path, monkeypatch):
    dados_dir = tmp_path / "dados"
    refs_dir = dados_dir / "referencias"
    sefin_dir = refs_dir / "CO_SEFIN"
    sefin_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(mod, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(mod, "DADOS_DIR", dados_dir)
    monkeypatch.setattr(mod, "REFS_DIR", refs_dir)

    return dados_dir, sefin_dir


def _salvar_produtos_agrupados(dados_dir: Path, cnpj: str, rows: list[dict]):
    pasta = dados_dir / "CNPJ" / cnpj / "analises" / "produtos"
    pasta.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(pasta / f"produtos_agrupados_{cnpj}.parquet")


def test_enriquecer_co_sefin_class_prioriza_padrao_e_aceita_da_final_nula(tmp_path, monkeypatch):
    dados_dir, sefin_dir = _configurar_ambiente_tmp(tmp_path, monkeypatch)
    cnpj = "12345678000199"

    _salvar_produtos_agrupados(
        dados_dir,
        cnpj,
        [{"id_agrupado": "id_agr_1", "co_sefin_padrao": "999"}],
    )

    pl.DataFrame(
        {
            "it_nu_cest": ["2222"],
            "it_nu_ncm": ["1111"],
            "it_co_sefin": ["123"],
        }
    ).write_parquet(sefin_dir / "sitafe_cest_ncm.parquet")

    pl.DataFrame(
        {
            "it_co_sefin": ["999", "999", "123"],
            "it_da_inicio": ["20240101", "20230101", "20240101"],
            "it_da_final": [None, "20231231", None],
            "it_pc_interna": [17.0, 12.0, 5.0],
        }
    ).write_parquet(sefin_dir / "sitafe_produto_sefin_aux.parquet")

    df_mov = pl.DataFrame(
        {
            "id_agrupado": ["id_agr_1"],
            "ncm_padrao": ["1111"],
            "cest_padrao": ["2222"],
            "Dt_doc": [date(2025, 1, 15)],
            "Dt_e_s": [None],
        }
    )

    resultado = mod.enriquecer_co_sefin_class(df_mov, cnpj)

    assert resultado.height == 1
    assert resultado["co_sefin_agr"][0] == "999"
    assert resultado["it_pc_interna"][0] == 17.0


def test_enriquecer_co_sefin_class_faz_fallback_para_lookup_legado_por_linha(tmp_path, monkeypatch):
    dados_dir, sefin_dir = _configurar_ambiente_tmp(tmp_path, monkeypatch)
    cnpj = "12345678000199"

    _salvar_produtos_agrupados(
        dados_dir,
        cnpj,
        [{"id_agrupado": "id_agr_1", "co_sefin_padrao": None}],
    )

    pl.DataFrame(
        {
            "it_nu_cest": ["2222"],
            "it_nu_ncm": ["1111"],
            "it_co_sefin": ["123"],
        }
    ).write_parquet(sefin_dir / "sitafe_cest_ncm.parquet")

    pl.DataFrame(
        {
            "it_co_sefin": ["123"],
            "it_da_inicio": ["20240101"],
            "it_da_final": [None],
            "it_pc_interna": [9.5],
        }
    ).write_parquet(sefin_dir / "sitafe_produto_sefin_aux.parquet")

    df_mov = pl.DataFrame(
        {
            "id_agrupado": ["id_agr_1"],
            "ncm_padrao": ["1111"],
            "cest_padrao": ["2222"],
            "Dt_doc": [date(2025, 2, 1)],
            "Dt_e_s": [None],
        }
    )

    resultado = mod.enriquecer_co_sefin_class(df_mov, cnpj)

    assert resultado.height == 1
    assert resultado["co_sefin_agr"][0] == "123"
    assert resultado["it_pc_interna"][0] == 9.5


def test_enriquecer_co_sefin_class_nao_preenche_it_pc_interna_sem_vigencia_compativel(tmp_path, monkeypatch):
    dados_dir, sefin_dir = _configurar_ambiente_tmp(tmp_path, monkeypatch)
    cnpj = "12345678000199"

    _salvar_produtos_agrupados(
        dados_dir,
        cnpj,
        [{"id_agrupado": "id_agr_1", "co_sefin_padrao": "9017"}],
    )

    pl.DataFrame(
        {
            "it_nu_cest": ["2222"],
            "it_nu_ncm": ["1111"],
            "it_co_sefin": ["9017"],
        }
    ).write_parquet(sefin_dir / "sitafe_cest_ncm.parquet")

    pl.DataFrame(
        {
            "it_co_sefin": ["9017", "9017"],
            "it_da_inicio": ["20160320", "20240112"],
            "it_da_final": ["20240111", None],
            "it_pc_interna": [17.5, 19.5],
            "it_in_st": ["N", "N"],
        }
    ).write_parquet(sefin_dir / "sitafe_produto_sefin_aux.parquet")

    df_mov = pl.DataFrame(
        {
            "id_agrupado": ["id_agr_1"],
            "ncm_padrao": ["1111"],
            "cest_padrao": ["2222"],
            "Dt_doc": [date(2010, 12, 31)],
            "Dt_e_s": [None],
        }
    )

    resultado = mod.enriquecer_co_sefin_class(df_mov, cnpj)

    assert resultado.height == 1
    assert resultado["co_sefin_agr"][0] == "9017"
    assert resultado["it_pc_interna"][0] is None
