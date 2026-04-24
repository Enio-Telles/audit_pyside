from pathlib import Path
import sys

import polars as pl
import pytest

PROJECT_ROOT = Path(__file__).parent.parent
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from transformacao.id_agrupados import gerar_id_agrupados
from tests.utils_golden import verify_golden_hash


def test_golden_id_agrupados_output(tmp_path):
    """Protect the deterministic grouped identity emitted by gerar_id_agrupados."""
    cnpj = "12345678000190"
    pasta_cnpj = tmp_path / cnpj
    pasta_prod = pasta_cnpj / "analises" / "produtos"
    pasta_prod.mkdir(parents=True, exist_ok=True)

    pl.DataFrame(
        {
            "id_agrupado": ["AGR_1", "AGR_1", "AGR_2"],
            "descr_padrao": ["Produto A", "Produto A", "Produto B"],
            "descricao_final": ["Produto A", "Produto A", "Produto B Final"],
            "descricao": ["Produto A", "Produto A variante", "Produto B"],
            "lista_desc_compl": [["Comp 1"], ["Comp 2"], ["Comp B"]],
            "lista_codigos": [["COD1"], ["COD2"], []],
            "lista_unid": [["UN"], ["CX"], ["KG"]],
            "lista_unidades_agr": [["UN"], ["CX"], ["KG"]],
            "lista_descricoes": [["Produto A"], ["Produto A variante"], ["Produto B"]],
            "unid_ref_sugerida": ["UN", "UN", "KG"],
        }
    ).write_parquet(pasta_prod / f"produtos_final_{cnpj}.parquet")

    pl.DataFrame(
        {
            "id_agrupado": ["AGR_2", "AGR_2", "AGR_1"],
            "codigo_fonte": ["CODB1", "CODB2", "COD1"],
        }
    ).write_parquet(pasta_prod / f"map_produto_agrupado_{cnpj}.parquet")

    sucesso = gerar_id_agrupados(cnpj, pasta_cnpj=pasta_cnpj)
    assert sucesso is True

    output_file = pasta_prod / f"id_agrupados_{cnpj}.parquet"
    assert output_file.exists()

    df_output = pl.read_parquet(output_file)
    verify_golden_hash(
        df_output,
        "id_agrupados_base_v1",
        cols=[
            "id_agrupado",
            "descr_padrao",
            "lista_descricoes",
            "lista_codigos",
            "lista_unidades",
        ],
    )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
