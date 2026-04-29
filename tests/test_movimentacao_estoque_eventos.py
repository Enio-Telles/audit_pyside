from pathlib import Path
import sys

import polars as pl

sys.path.insert(0, str(Path("src").resolve()))

from transformacao.movimentacao_estoque_pkg.calculo_saldos import gerar_eventos_estoque  # noqa: E402


def test_gerar_eventos_estoque_expone_origem_e_evento_sintetico():
    df = pl.DataFrame(
        {
            "id_agrupado": ["AGR_1", "AGR_1"],
            "Tipo_operacao": ["1 - ENTRADA", "inventario"],
            "Dt_doc": ["2024-05-10", "2024-12-31"],
            "Dt_e_s": ["2024-05-10", "2024-12-31"],
            "fonte": ["nfe", "bloco_h"],
            "ncm_padrao": ["22083000", "22083000"],
            "cest_padrao": ["0300700", "0300700"],
            "descr_padrao": ["Produto A", "Produto A"],
            "Cod_item": ["0001", "0001"],
            "Cod_barra": ["", ""],
            "Ncm": ["22083000", "22083000"],
            "Cest": ["0300700", "0300700"],
            "Tipo_item": ["00", "00"],
            "Descr_item": ["Produto A", "Produto A"],
            "Cfop": ["1102", None],
            "co_sefin_agr": ["123", "123"],
            "unid_ref": ["UN", "UN"],
            "fator": [1.0, 1.0],
            "Qtd": [10.0, 12.0],
            "Vl_item": [100.0, 120.0],
            "Unid": ["UN", "UN"],
            "Ser": ["1", "1"],
        }
    ).with_columns(
        [
            pl.col("Dt_doc").str.strptime(pl.Datetime, "%Y-%m-%d", strict=False),
            pl.col("Dt_e_s").str.strptime(pl.Datetime, "%Y-%m-%d", strict=False),
        ]
    )

    result = gerar_eventos_estoque(df)
    assert "origem_evento_estoque" in result.columns
    assert "evento_sintetico" in result.columns

    origens = set(result["origem_evento_estoque"].drop_nulls().to_list())
    assert "registro" in origens
    assert "inventario_bloco_h" in origens
    assert "estoque_inicial_derivado" in origens
