from pathlib import Path
import sys

import polars as pl


sys.path.insert(0, str(Path("src").resolve()))

import transformacao.rastreabilidade_produtos.id_agrupados as id_agrupados_module
from transformacao.rastreabilidade_produtos.id_agrupados import gerar_id_agrupados


def test_gerar_id_agrupados_separa_descricoes_de_complementos(monkeypatch, tmp_path: Path):
    cnpj = "12345678000190"
    pasta_cnpj = tmp_path / cnpj
    pasta_prod = pasta_cnpj / "analises" / "produtos"
    pasta_prod.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(id_agrupados_module, "CNPJ_ROOT", tmp_path)

    pl.DataFrame(
        {
            "id_agrupado": ["AGR_1", "AGR_1"],
            "descr_padrao": ["Produto A", "Produto A"],
            "descricao_final": ["Produto A", "Produto A"],
            "descricao": ["Produto A", "Produto A variante"],
            "lista_desc_compl": [["Comp 1"], ["Comp 2"]],
            "lista_codigos": [["COD1"], ["COD2"]],
            "lista_unid": [["UN"], ["CX"]],
            "lista_unidades_agr": [["UN"], ["CX"]],
            "unid_ref_sugerida": ["UN", "UN"],
        }
    ).write_parquet(pasta_prod / f"produtos_final_{cnpj}.parquet")

    assert gerar_id_agrupados(cnpj, pasta_cnpj=pasta_cnpj) is True

    df = pl.read_parquet(pasta_prod / f"id_agrupados_{cnpj}.parquet")
    row = df.row(0, named=True)

    assert row["lista_descricoes"] == ["Produto A", "Produto A variante"]
    assert row["lista_desc_compl"] == ["Comp 1", "Comp 2"]
