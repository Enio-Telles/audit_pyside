import polars as pl
from utilitarios.normalizar_parquet import normalizar_colunas_parquet

def test_normalizar_colunas_none():
    assert normalizar_colunas_parquet(None) is None

def test_normalizar_colunas_empty_dataframe():
    df = pl.DataFrame({"A": [], "B": []})
    result = normalizar_colunas_parquet(df)
    assert result.columns == ["A", "B"]

def test_normalizar_colunas_dataframe():
    df = pl.DataFrame({"COL1": [1, 2], "Col2": [3, 4], "col3": [5, 6]})
    result = normalizar_colunas_parquet(df)
    assert result.columns == ["col1", "col2", "col3"]

def test_normalizar_colunas_lazyframe():
    df = pl.LazyFrame({"COL1": [1, 2], "Col2": [3, 4], "col3": [5, 6]})
    result = normalizar_colunas_parquet(df)
    assert result.collect_schema().names() == ["col1", "col2", "col3"]
