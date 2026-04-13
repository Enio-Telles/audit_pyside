from pathlib import Path
import sys
from datetime import date

import polars as pl


sys.path.insert(0, str(Path("src").resolve()))

import transformacao.atomizacao_pkg.pipeline_efd_atomizado as atomizacao_modulo


def _criar_parquet(caminho: Path, dados: dict) -> None:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(dados).write_parquet(caminho)


def test_materializar_camadas_atomizadas_enriquece_fontes(monkeypatch, tmp_path: Path):
    cnpj = "12345678000190"
    cnpj_dir = tmp_path / cnpj
    base_atomizada = cnpj_dir / "arquivos_parquet" / "atomizadas"
    referencia = tmp_path / "referencias" / "conditional_descriptions_reference.parquet"
    referencia.parent.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(atomizacao_modulo, "CNPJ_ROOT", tmp_path)
    monkeypatch.setattr(atomizacao_modulo, "REF_PATH", referencia)

    pl.DataFrame(
        {
            "source_field": ["c100.cod_sit", "c100.ind_emit", "c100.ind_oper"],
            "branch_kind": ["WHEN", "WHEN", "WHEN"],
            "match_value": ["00", "0", "0"],
            "description": ["Documento regular", "Emissao propria", "Entrada"],
        }
    ).write_parquet(referencia)

    _criar_parquet(
        base_atomizada / "dimensions" / f"50_reg0200_raw_{cnpj}.parquet",
        {
            "reg_0000_id": [1],
            "cnpj": [cnpj],
            "dt_ini": [date(2024, 1, 1)],
            "dt_fin": [date(2024, 1, 31)],
            "data_entrega_efd_periodo": [date(2024, 2, 10)],
            "cod_fin_efd": ["0"],
            "cod_item": ["ITEM1"],
            "descr_item": ["Produto 1"],
            "tipo_item": ["00"],
            "cod_ncm": ["12345678"],
            "cest": ["1234567"],
            "cod_barra": ["7890001112223"],
        },
    )
    _criar_parquet(
        base_atomizada / "c100" / f"10_c100_raw_{cnpj}.parquet",
        {
            "reg_c100_id": [10],
            "reg_0000_id": [1],
            "cnpj": [cnpj],
            "dt_ini": [date(2024, 1, 1)],
            "dt_fin": [date(2024, 1, 31)],
            "data_entrega_efd_periodo": [date(2024, 2, 10)],
            "cod_fin_efd": ["0"],
            "ind_oper": ["0"],
            "ind_emit": ["0"],
            "cod_part": ["P1"],
            "cod_mod": ["55"],
            "cod_sit": ["00"],
            "ser": ["1"],
            "num_doc": ["100"],
            "chv_nfe": ["CHAVE1"],
            "dt_doc_raw": ["01012024"],
            "dt_e_s_raw": ["02012024"],
            "vl_doc": [100.0],
            "ind_pgto": ["0"],
            "vl_desc": [0.0],
            "vl_abat_nt": [0.0],
            "vl_merc": [100.0],
            "ind_frt": ["0"],
            "vl_frt": [0.0],
            "vl_seg": [0.0],
            "vl_out_da": [0.0],
            "vl_bc_icms": [100.0],
            "vl_icms": [18.0],
            "vl_bc_icms_st": [0.0],
            "vl_icms_st": [0.0],
            "vl_ipi": [0.0],
            "vl_pis": [0.0],
            "vl_cofins": [0.0],
            "vl_pis_st": [0.0],
            "vl_cofins_st": [0.0],
        },
    )
    _criar_parquet(
        base_atomizada / "c170" / f"20_c170_raw_{cnpj}.parquet",
        {
            "reg_c170_id": [100],
            "reg_c100_id": [10],
            "reg_0000_id": [1],
            "cnpj": [cnpj],
            "dt_ini": [date(2024, 1, 1)],
            "dt_fin": [date(2024, 1, 31)],
            "data_entrega_efd_periodo": [date(2024, 2, 10)],
            "num_item": [1],
            "cod_item": ["ITEM1"],
            "descr_compl": ["Compl"],
            "cfop": ["5102"],
            "cst_icms": ["000"],
            "qtd": [2.0],
            "unid": ["UN"],
            "vl_item": [100.0],
            "vl_desc": [0.0],
            "vl_icms": [18.0],
            "vl_bc_icms": [100.0],
            "aliq_icms": [18.0],
            "vl_bc_icms_st": [0.0],
            "vl_icms_st": [0.0],
            "aliq_st": [0.0],
        },
    )
    _criar_parquet(
        base_atomizada / "c176" / f"30_c176_raw_{cnpj}.parquet",
        {
            "reg_c100_id": [10],
            "reg_c170_id": [100],
            "reg_0000_id": [1],
            "cnpj": [cnpj],
            "dt_ini": [date(2024, 1, 1)],
            "dt_fin": [date(2024, 1, 31)],
            "data_entrega_efd_periodo": [date(2024, 2, 10)],
            "cod_fin_efd": ["0"],
            "cod_mot_res": ["1"],
            "chave_nfe_ult": ["CHAVE-ENTRADA"],
            "num_item_ult_e": [1],
            "dt_ult_e_raw": ["03012024"],
            "vl_unit_ult_e": [40.0],
            "vl_unit_icms_ult_e": [7.2],
            "vl_unit_res": [5.0],
        },
    )
    _criar_parquet(
        base_atomizada / "bloco_h" / f"40_h005_raw_{cnpj}.parquet",
        {
            "reg_h005_id": [500],
            "reg_0000_id": [1],
            "cnpj": [cnpj],
            "dt_ini": [date(2024, 1, 1)],
            "dt_fin": [date(2024, 1, 31)],
            "data_entrega_efd_periodo": [date(2024, 2, 10)],
            "dt_inv_raw": ["31012024"],
            "vl_inv_raw": [500.0],
            "mot_inv": ["01"],
        },
    )
    _criar_parquet(
        base_atomizada / "bloco_h" / f"41_h010_raw_{cnpj}.parquet",
        {
            "reg_h010_id": [510],
            "reg_h005_id": [500],
            "reg_0000_id": [1],
            "cnpj": [cnpj],
            "dt_ini": [date(2024, 1, 1)],
            "dt_fin": [date(2024, 1, 31)],
            "cod_item": ["ITEM1"],
            "unid": ["UN"],
            "qtd": [5.0],
            "vl_unit": [20.0],
            "vl_item": [100.0],
            "ind_prop": ["0"],
            "cod_part": [None],
            "txt_compl": ["Inventario"],
        },
    )
    _criar_parquet(
        base_atomizada / "bloco_h" / f"42_h020_raw_{cnpj}.parquet",
        {
            "reg_h010_id": [510],
            "reg_0000_id": [1],
            "cnpj": [cnpj],
            "dt_ini": [date(2024, 1, 1)],
            "dt_fin": [date(2024, 1, 31)],
            "cst_icms": ["060"],
            "bc_icms": [80.0],
            "vl_icms": [14.4],
        },
    )

    caminhos = atomizacao_modulo.materializar_camadas_atomizadas(cnpj)

    assert len(caminhos) == 8

    pasta_analises = tmp_path / cnpj / "analises" / "atomizadas"
    c170 = pl.read_parquet(pasta_analises / f"c170_tipado_{cnpj}.parquet")
    c176 = pl.read_parquet(pasta_analises / f"c176_tipado_{cnpj}.parquet")
    bloco_h = pl.read_parquet(pasta_analises / f"bloco_h_tipado_{cnpj}.parquet")

    linha_c170 = c170.row(0, named=True)
    assert linha_c170["descr_item"] == "Produto 1"
    assert linha_c170["codigo_fonte"] == f"{cnpj}|ITEM1"

    linha_c176 = c176.row(0, named=True)
    assert linha_c176["cod_item"] == "ITEM1"
    assert linha_c176["num_doc"] == "100"

    linha_h = bloco_h.row(0, named=True)
    assert linha_h["descr_item"] == "Produto 1"
    assert linha_h["cst_icms"] == "060"
    assert linha_h["vl_inv"] == 500.0
