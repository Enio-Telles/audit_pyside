from __future__ import annotations

import sys
from pathlib import Path
from uuid import uuid4

import polars as pl


ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from transformacao.ressarcimento_st import gerar_ressarcimento_st  # noqa: E402


def _pasta_temporaria() -> Path:
    pasta = ROOT_DIR / "_tmp_testes" / f"ressarcimento_st_{uuid4().hex}"
    (pasta / "analises" / "produtos").mkdir(parents=True, exist_ok=True)
    (pasta / "arquivos_parquet").mkdir(parents=True, exist_ok=True)
    return pasta


def test_gerar_ressarcimento_st_materializa_itens_mensal_conciliacao_e_validacoes():
    cnpj = "12345678000199"
    pasta_cnpj = _pasta_temporaria()

    pl.DataFrame(
        {
            "cnpj": [cnpj, cnpj, cnpj],
            "periodo_efd": ["2021/05", "2026/01", "2021/05"],
            "chave_saida": ["S1", "S2", "S3"],
            "num_item_saida": [1, 1, 2],
            "cod_item_ref_saida": ["ITEM1", "ITEM2", "ITEM3"],
            "id_agrupado": ["ID1", "ID2", None],
            "descr_padrao": ["Produto 1", "Produto 2", "Produto 3"],
            "unid_ref": ["UN", "UN", None],
            "fator_saida": [1.0, 1.0, None],
            "fator_entrada_xml": [1.0, 1.0, 1.0],
            "qtd_saida_unid_ref": [5.0, 3.0, 2.0],
            "qtd_entrada_xml": [10.0, 6.0, 4.0],
            "qtd_entrada_xml_unid_ref": [10.0, 6.0, 4.0],
            "chave_nfe_ultima_entrada": ["E1", "E2", "E3"],
            "prod_nitem": [1, 1, 1],
            "vl_unit_ressarcimento_st_unid_ref": [2.0, 4.0, 1.5],
            "vl_unit_icms_proprio_entrada_unid_ref": [1.0, 2.0, 1.0],
        }
    ).write_parquet(pasta_cnpj / "analises" / "produtos" / f"c176_xml_{cnpj}.parquet")

    pl.DataFrame(
        {
            "chave_acesso": ["E1", "E2"],
            "prod_nitem": [1, 1],
            "icms_vbcst": [100.0, 60.0],
            "icms_vicmsst": [30.0, 24.0],
            "icms_vicmsstret": [30.0, 24.0],
        }
    ).write_parquet(pasta_cnpj / "arquivos_parquet" / f"nfe_dados_st_{cnpj}.parquet")

    pl.DataFrame(
        {
            "tipo_operacao": ["0 - ENTRADA", "0 - ENTRADA"],
            "chave_acesso": ["E1", "E2"],
            "num_item": [1, 1],
            "cod_item": ["ITEM1", "ITEM2"],
            "desc_item": ["Produto 1", "Produto 2"],
            "ncm": ["1000", "2000"],
            "cest": ["", ""],
            "qtd_comercial": [10.0, 6.0],
            "valor_produto": [200.0, 120.0],
            "bc_icms_st_destacado": [100.0, 60.0],
            "icms_st_destacado": [20.0, 15.0],
            "co_sefin": ["A", "B"],
            "cod_rotina_calculo": ["ST", "ST"],
            "valor_icms_fronteira": [20.0, 12.0],
        }
    ).write_parquet(pasta_cnpj / "arquivos_parquet" / f"fronteira_{cnpj}.parquet")

    pl.DataFrame(
        {
            "periodo_efd": ["2021/05", "2026/01"],
            "codigo_ajuste": ["RO020022", "RO020047"],
            "valor_ajuste": [10.0, 8.0],
        }
    ).write_parquet(pasta_cnpj / "arquivos_parquet" / f"e111_{cnpj}.parquet")

    assert gerar_ressarcimento_st(cnpj, pasta_cnpj) is True

    pasta_saida = pasta_cnpj / "analises" / "ressarcimento_st"
    df_item = pl.read_parquet(pasta_saida / f"ressarcimento_st_item_{cnpj}.parquet")
    df_mensal = pl.read_parquet(pasta_saida / f"ressarcimento_st_mensal_{cnpj}.parquet")
    df_conc = pl.read_parquet(pasta_saida / f"ressarcimento_st_conciliacao_{cnpj}.parquet")
    df_valid = pl.read_parquet(pasta_saida / f"ressarcimento_st_validacoes_{cnpj}.parquet")

    status_por_saida = {
        linha["chave_saida"]: linha["status_calculo"]
        for linha in df_item.select(["chave_saida", "status_calculo"]).to_dicts()
    }
    assert status_por_saida["S1"] == "ok"
    assert status_por_saida["S2"] == "parcial_pos_2022"
    assert status_por_saida["S3"] == "pendente_conversao"

    linha_s1 = df_item.filter(pl.col("chave_saida") == "S1").row(0, named=True)
    assert linha_s1["vl_st_decl_total_considerado"] == 10.0
    assert linha_s1["vl_st_calc_total_considerado"] == 15.0
    assert linha_s1["vl_st_fronteira_total_considerado"] == 10.0

    linha_2021 = df_mensal.filter(pl.col("mes_ref") == pl.date(2021, 5, 1)).row(0, named=True)
    assert linha_2021["vl_e111_st_mes"] == 10.0
    assert linha_2021["vl_st_calc_total_mes"] == 15.0

    linha_conc_2021 = df_conc.filter(pl.col("mes_ref") == pl.date(2021, 5, 1)).row(0, named=True)
    assert linha_conc_2021["qtd_pendencias_conversao"] == 1
    assert linha_conc_2021["qtd_itens_com_st_calc"] == 1

    assert df_valid.height >= 1
