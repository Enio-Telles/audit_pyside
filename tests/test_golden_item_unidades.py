from pathlib import Path
import sys

import polars as pl
import pytest

PROJECT_ROOT = Path(__file__).parent.parent
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from transformacao.tabelas_base.item_unidades import gerar_item_unidades
from tests.utils_golden import verify_golden_hash


def test_golden_item_unidades_output(tmp_path, monkeypatch):
    """Protect the deterministic base identity emitted by gerar_item_unidades."""
    cnpj = "12345678000195"
    root = tmp_path

    cnpj_path = root / "CNPJ" / cnpj
    (cnpj_path / "extraidos").mkdir(parents=True)
    (cnpj_path / "arquivos_parquet").mkdir(parents=True)
    (cnpj_path / "base").mkdir(parents=True)

    refs_path = root / "referencias" / "cfop"
    refs_path.mkdir(parents=True)
    pl.DataFrame(
        {
            "co_cfop": ["5101", "5102", "1101", "1102"],
            "operacao_mercantil": ["X", "X", "", ""],
        }
    ).write_parquet(refs_path / "cfop_bi.parquet")

    df_input = pl.DataFrame(
        {
            "cod_item": ["PROD001", "PROD001", "PROD002"],
            "descr_item": ["Produto Teste 1", "Produto Teste 1", "Produto Teste 2"],
            "unid": ["UN", "CX", "UN"],
            "cod_ncm": ["12345678", "12345678", "87654321"],
            "qtd": [10.0, 1.0, 5.0],
            "vl_item": [100.0, 110.0, 50.0],
            "cfop": ["1102", "1102", "1102"],
            "ind_oper": ["0", "0", "0"],
            "codigo_fonte": ["S1", "S1", "S1"],
        }
    )
    df_input.write_parquet(cnpj_path / "arquivos_parquet" / f"c170_{cnpj}.parquet")

    from transformacao.tabelas_base.item_unidades import _module as mod_impl

    monkeypatch.setattr(mod_impl, "CNPJ_ROOT", root / "CNPJ")
    monkeypatch.setattr(mod_impl, "REFS_DIR", root / "referencias")

    sucesso = gerar_item_unidades(cnpj)
    assert sucesso is True

    output_file = cnpj_path / "analises" / "produtos" / f"item_unidades_{cnpj}.parquet"
    assert output_file.exists()

    df_output = pl.read_parquet(output_file)
    verify_golden_hash(
        df_output,
        "item_unidades_base_v1",
        cols=["id_item_unid", "codigo", "unid", "ncm"],
    )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
