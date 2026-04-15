from pathlib import Path
import pytest
import polars as pl
from backend.services.parquet_service import ParquetService

def test_get_schema(tmp_path: Path):
    path = tmp_path / "test_schema.parquet"
    df = pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["A", "B", "C"],
        "active": [True, False, True]
    })
    df.write_parquet(path)

    service = ParquetService(root=tmp_path)
    schema = service.get_schema(path)

    assert schema == ["id", "name", "active"]

def test_get_schema_empty_file(tmp_path: Path):
    path = tmp_path / "empty.parquet"
    df = pl.DataFrame(schema={"col1": pl.Utf8, "col2": pl.Int64})
    df.write_parquet(path)

    service = ParquetService(root=tmp_path)
    schema = service.get_schema(path)

    assert schema == ["col1", "col2"]

def test_get_schema_file_not_found(tmp_path: Path):
    path = tmp_path / "non_existent.parquet"
    service = ParquetService(root=tmp_path)

    with pytest.raises(Exception):
        service.get_schema(path)
