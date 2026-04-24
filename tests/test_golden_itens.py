from pathlib import Path
import sys

import polars as pl
import pytest

PROJECT_ROOT = Path(__file__).parent.parent
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from transformacao.tabelas_base.itens import gerar_itens
from tests.utils_golden import verify_golden_hash


def test_golden_itens_output(tmp_path, monkeypatch):
    """Protect the deterministic consolidation emitted by gerar_itens."""
    cnpj = "12345678000195"
    root = tmp_path

    produtos_path = root / "CNPJ" / cnpj / "analises" / "produtos"
    produtos_path.mkdir(parents=True)

    pl.DataFrame(
        {
            "id_item_unid": ["id_item_unid_1", "id_item_unid_2", "id_item_unid_3"],
            "codigo": ["PROD001", "PROD001", "PROD002"],
            "descricao": ["Acucar Mascavo", "Acucar   Mascavo", "Cafe Torrado"],
            "descr_compl": [None, "Pacote 1kg", None],
            "tipo_item": ["00", "00", "01"],
            "ncm": ["12345678", "12345678", "87654321"],
            "cest": [None, None, "123"],
            "co_sefin_item": [None, None, "999"],
            "gtin": [None, "789", None],
            "unid": ["UN", "CX", "UN"],
            "fontes": [["SPED"], ["SPED", "XML"], ["XML"]],
        }
    ).write_parquet(produtos_path / f"item_unidades_{cnpj}.parquet")

    from transformacao.tabelas_base.itens import _module as mod_impl

    monkeypatch.setattr(mod_impl, "CNPJ_ROOT", root / "CNPJ")

    sucesso = gerar_itens(cnpj)
    assert sucesso is True

    output_file = produtos_path / f"itens_{cnpj}.parquet"
    assert output_file.exists()

    df_output = pl.read_parquet(output_file)
    verify_golden_hash(
        df_output,
        "itens_base_v1",
        cols=[
            "id_item",
            "codigo",
            "descricao_normalizada",
            "lista_unid",
            "lista_id_item_unid",
        ],
    )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
