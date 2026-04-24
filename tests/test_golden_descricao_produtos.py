from pathlib import Path
import sys

import polars as pl
import pytest

PROJECT_ROOT = Path(__file__).parent.parent
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from transformacao.descricao_produtos import gerar_descricao_produtos
from tests.utils_golden import verify_golden_hash


def test_golden_descricao_produtos_output(tmp_path):
    """Protect the deterministic consolidation emitted by gerar_descricao_produtos."""
    cnpj = "12345678000195"
    produtos_path = tmp_path / "CNPJ" / cnpj / "analises" / "produtos"
    produtos_path.mkdir(parents=True)

    pl.DataFrame(
        {
            "id_item_unid": ["id_item_unid_1", "id_item_unid_2", "id_item_unid_3"],
            "codigo": ["PROD001", "PROD001", "PROD002"],
            "descricao": ["Acucar Mascavo", "Acucar Mascavo", "Cafe Torrado"],
            "descr_compl": [None, "Pacote 1kg", None],
            "tipo_item": ["00", "00", "01"],
            "ncm": ["12345678", "12345678", "87654321"],
            "cest": [None, None, "123"],
            "co_sefin_item": [None, None, "999"],
            "gtin": [None, "789", None],
            "unid": ["UN", "CX", "UN"],
            "lista_codigo_fonte": [["SPED"], ["SPED", "XML"], ["XML"]],
            "fontes": [["SPED"], ["SPED", "XML"], ["XML"]],
        }
    ).write_parquet(produtos_path / f"item_unidades_{cnpj}.parquet")

    pl.DataFrame(
        {
            "id_item": ["id_item_1", "id_item_2"],
            "descricao_normalizada": ["ACUCAR MASCAVO", "CAFE TORRADO"],
            "descricao": ["Acucar Mascavo", "Cafe Torrado"],
            "codigo": ["PROD001", "PROD002"],
            "descr_compl": ["Pacote 1kg", None],
            "tipo_item": ["00", "01"],
            "ncm": ["12345678", "87654321"],
            "cest": [None, "123"],
            "co_sefin_item": [None, "999"],
            "gtin": ["789", None],
            "lista_unid": [["CX", "UN"], ["UN"]],
            "fontes": [["SPED", "XML"], ["XML"]],
            "lista_id_item_unid": [["id_item_unid_1", "id_item_unid_2"], ["id_item_unid_3"]],
        }
    ).write_parquet(produtos_path / f"itens_{cnpj}.parquet")

    sucesso = gerar_descricao_produtos(cnpj, tmp_path / "CNPJ" / cnpj)
    assert sucesso is True

    output_file = produtos_path / f"descricao_produtos_{cnpj}.parquet"
    assert output_file.exists()

    df_output = pl.read_parquet(output_file)
    verify_golden_hash(
        df_output,
        "descricao_produtos_base_v1",
        cols=[
            "id_descricao",
            "descricao_normalizada",
            "id_agrupado_base",
            "lista_codigos",
            "lista_unid",
            "lista_id_item",
        ],
    )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
