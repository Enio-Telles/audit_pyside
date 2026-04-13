from pathlib import Path
import sys

import polars as pl


sys.path.insert(0, str(Path("src").resolve()))

from interface_grafica.services.parquet_service import FilterCondition, ParquetService


def test_load_dataset_filtra_coluna_lista_com_contem(tmp_path: Path):
    path = tmp_path / "dados.parquet"
    pl.DataFrame(
        {
            "id_agrupado": ["AGR_1", "AGR_2"],
            "lista_ncm": [["1000", "1001"], ["2000"]],
            "lista_descricoes": [["Produto A", "Garrafa"], ["Produto B"]],
        }
    ).write_parquet(path)

    service = ParquetService(root=tmp_path)
    df_ncm = service.load_dataset(path, [FilterCondition(column="lista_ncm", operator="contem", value="1000")])
    df_descr = service.load_dataset(path, [FilterCondition(column="lista_descricoes", operator="contem", value="garrafa")])

    assert df_ncm["id_agrupado"].to_list() == ["AGR_1"]
    assert df_descr["id_agrupado"].to_list() == ["AGR_1"]
