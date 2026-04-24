from pathlib import Path
import sys

import polars as pl
import pytest

PROJECT_ROOT = Path(__file__).parent.parent
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from transformacao.produtos_itens import gerar_produtos_itens
from tests.utils_golden import verify_golden_hash


def test_golden_produtos_itens_output(tmp_path):
    """Protect the deterministic join between produtos and produtos_unidades."""
    cnpj = "12345678000195"
    produtos_path = tmp_path / "CNPJ" / cnpj / "analises" / "produtos"
    produtos_path.mkdir(parents=True)

    pl.DataFrame(
        {
            "descricao": ["Acucar Mascavo", "Cafe Torrado", "Acucar Mascavo"],
            "codigo": ["PROD001", "PROD002", "PROD001"],
            "descr_compl": [None, None, "Pacote 1kg"],
            "tipo_item": ["00", "01", "00"],
            "ncm": ["12345678", "87654321", "12345678"],
            "cest": [None, "123", None],
            "gtin": [None, None, "789"],
            "unid": ["UN", "UN", "CX"],
        }
    ).write_parquet(produtos_path / f"produtos_unidades_{cnpj}.parquet")

    pl.DataFrame(
        {
            "chave_item": ["item_acucar", "item_cafe"],
            "chave_produto": ["produto_acucar", "produto_cafe"],
            "descricao_normalizada": ["ACUCAR MASCAVO", "CAFE TORRADO"],
        }
    ).write_parquet(produtos_path / f"produtos_{cnpj}.parquet")

    sucesso = gerar_produtos_itens(cnpj, tmp_path / "CNPJ" / cnpj)
    assert sucesso is True

    output_file = produtos_path / f"produtos_itens_{cnpj}.parquet"
    assert output_file.exists()

    df_output = pl.read_parquet(output_file)
    verify_golden_hash(
        df_output,
        "produtos_itens_base_v1",
        cols=["chave_item", "chave_produto", "descricao", "codigo", "lista_unid"],
    )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
