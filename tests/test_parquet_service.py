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
    df_ncm = service.load_dataset(
        path, [FilterCondition(column="lista_ncm", operator="contem", value="1000")]
    )
    df_descr = service.load_dataset(
        path,
        [
            FilterCondition(
                column="lista_descricoes", operator="contem", value="garrafa"
            )
        ],
    )

    assert df_ncm["id_agrupado"].to_list() == ["AGR_1"]
    assert df_descr["id_agrupado"].to_list() == ["AGR_1"]


def test_v2_fallback_quando_v2_existe(tmp_path: Path):
    """build_lazyframe usa v2 quando disponivel, sem quebrar v1."""
    root = tmp_path / "v1"
    root.mkdir()
    v2_root = tmp_path / "v2"
    v2_root.mkdir()

    # v1
    df_v1 = pl.DataFrame({"cfop": ["5102"], "id_agrupado": ["id_1"]})
    v1_path = root / "c170_xml.parquet"
    df_v1.write_parquet(v1_path)

    # v2 (com coluna tipada)
    df_v2 = pl.DataFrame({
        "cfop": ["5102"],
        "id_agrupado": ["id_1"],
    })
    v2_path = v2_root / "c170_xml.parquet"
    v2_path.parent.mkdir(parents=True, exist_ok=True)
    df_v2.write_parquet(v2_path)

    service = ParquetService(root=root, v2_root=v2_root)
    lf = service.build_lazyframe(v1_path)
    df = lf.collect()
    assert df["cfop"].to_list() == ["5102"]
    assert df["id_agrupado"].to_list() == ["id_1"]


def test_get_schema_usa_v2_quando_disponivel(tmp_path: Path):
    """get_schema consulta o schema do arquivo efetivamente usado."""
    root = tmp_path / "v1"
    root.mkdir()
    v2_root = tmp_path / "v2"
    v2_root.mkdir()

    v1_path = root / "c170_xml.parquet"
    pl.DataFrame({"cfop": ["5102"], "id_agrupado": ["id_1"]}).write_parquet(v1_path)

    v2_path = v2_root / "c170_xml.parquet"
    pl.DataFrame(
        {
            "cfop": ["5102"],
            "id_agrupado": ["id_1"],
            "coluna_v2": ["presente"],
        }
    ).write_parquet(v2_path)

    service = ParquetService(root=root, v2_root=v2_root)

    assert service.get_schema(v1_path) == ["cfop", "id_agrupado", "coluna_v2"]


def test_v2_fallback_quando_v2_nao_existe(tmp_path: Path):
    """build_lazyframe usa v1 quando v2 nao existe."""
    root = tmp_path / "v1"
    root.mkdir()
    v2_root = tmp_path / "v2"

    df_v1 = pl.DataFrame({"cfop": ["5102"], "id_agrupado": ["id_1"]})
    v1_path = root / "c170_xml.parquet"
    df_v1.write_parquet(v1_path)

    service = ParquetService(root=root, v2_root=v2_root)
    lf = service.build_lazyframe(v1_path)
    df = lf.collect()
    assert df["cfop"].to_list() == ["5102"]


def test_v2_fallback_sem_v2_root(tmp_path: Path):
    """build_lazyframe funciona sem v2_root configurado."""
    root = tmp_path / "v1"
    root.mkdir()

    df_v1 = pl.DataFrame({"cfop": ["5102"], "id_agrupado": ["id_1"]})
    v1_path = root / "c170_xml.parquet"
    df_v1.write_parquet(v1_path)

    service = ParquetService(root=root)
    lf = service.build_lazyframe(v1_path)
    df = lf.collect()
    assert df["cfop"].to_list() == ["5102"]
