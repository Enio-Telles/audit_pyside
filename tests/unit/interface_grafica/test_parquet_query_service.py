"""
Testes para ParquetQueryService — Phase 1.2.

Verifica:
- usa_duckdb: threshold correto (size, dir)
- get_schema roteia para o backend certo
- get_count roteia (Polars e DuckDB)
- get_page roteia (Polars e DuckDB)
- get_distinct_values roteia (Polars e DuckDB, com search)
- export_to_parquet e export_to_csv (ambos os backends)
- Injecao de dependencia via constructor
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from interface_grafica.services.parquet_query_service import ParquetQueryService
from interface_grafica.services.parquet_service import FilterCondition, ParquetService


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def parquet_pequeno(tmp_path: Path) -> Path:
    """Parquet sintetico pequeno (abaixo do threshold default)."""
    path = tmp_path / "pequeno.parquet"
    pl.DataFrame(
        {
            "id": list(range(1, 21)),
            "nome": [f"item_{i:02d}" for i in range(1, 21)],
            "valor": [float(i * 5) for i in range(1, 21)],
            "categoria": [f"cat_{(i % 3) + 1}" for i in range(1, 21)],
        }
    ).write_parquet(path)
    return path


@pytest.fixture()
def parquet_grande(tmp_path: Path) -> Path:
    """Parquet sintetico que simula arquivo grande (threshold_mb=0 no servico)."""
    path = tmp_path / "grande.parquet"
    pl.DataFrame(
        {
            "id": list(range(1, 101)),
            "nome": [f"produto_{i:03d}" for i in range(1, 101)],
            "valor": [float(i * 10) for i in range(1, 101)],
            "categoria": [f"cat_{(i % 4) + 1}" for i in range(1, 101)],
        }
    ).write_parquet(path)
    return path


@pytest.fixture()
def svc_polars(parquet_pequeno: Path) -> ParquetQueryService:
    """Servico configurado para sempre usar Polars (threshold muito alto)."""
    return ParquetQueryService(threshold_mb=999_999)


@pytest.fixture()
def svc_duckdb(parquet_grande: Path) -> ParquetQueryService:
    """Servico configurado para sempre usar DuckDB (threshold zero)."""
    return ParquetQueryService(threshold_mb=0)


# ---------------------------------------------------------------------------
# Testes: roteamento usa_duckdb
# ---------------------------------------------------------------------------


def test_usa_duckdb_arquivo_pequeno(parquet_pequeno: Path) -> None:
    svc = ParquetQueryService(threshold_mb=999_999)
    assert svc.usa_duckdb(parquet_pequeno) is False


def test_usa_duckdb_arquivo_grande_via_threshold(parquet_pequeno: Path) -> None:
    svc = ParquetQueryService(threshold_mb=0)
    assert svc.usa_duckdb(parquet_pequeno) is True


def test_usa_duckdb_diretorio(tmp_path: Path) -> None:
    svc = ParquetQueryService(threshold_mb=999_999)
    assert svc.usa_duckdb(tmp_path) is True


def test_usa_duckdb_path_inexistente_retorna_false(tmp_path: Path) -> None:
    svc = ParquetQueryService(threshold_mb=0)
    inexistente = tmp_path / "nao_existe.parquet"
    # threshold=0 mas stat() levanta OSError para path inexistente
    assert svc.usa_duckdb(inexistente) is False


def test_threshold_exato_usa_polars(tmp_path: Path) -> None:
    """Arquivo exatamente no limite usa Polars (nao estritamente maior)."""
    path = tmp_path / "limite.parquet"
    pl.DataFrame({"x": [1]}).write_parquet(path)
    size_mb = path.stat().st_size / (1024 * 1024)
    svc = ParquetQueryService(threshold_mb=int(size_mb) + 1)
    assert svc.usa_duckdb(path) is False


# ---------------------------------------------------------------------------
# Testes: get_schema
# ---------------------------------------------------------------------------


def test_get_schema_polars(parquet_pequeno: Path, svc_polars: ParquetQueryService) -> None:
    colunas = svc_polars.get_schema(parquet_pequeno)
    assert set(colunas) == {"id", "nome", "valor", "categoria"}


def test_get_schema_duckdb(parquet_grande: Path, svc_duckdb: ParquetQueryService) -> None:
    colunas = svc_duckdb.get_schema(parquet_grande)
    assert set(colunas) == {"id", "nome", "valor", "categoria"}


def test_get_schema_delega_para_polars_service(parquet_pequeno: Path) -> None:
    mock_polars = MagicMock()
    mock_polars.get_schema.return_value = ["id", "nome"]
    svc = ParquetQueryService(polars_service=mock_polars, threshold_mb=999_999)
    result = svc.get_schema(parquet_pequeno)
    mock_polars.get_schema.assert_called_once_with(parquet_pequeno)
    assert result == ["id", "nome"]


def test_get_schema_delega_para_duckdb_service(parquet_pequeno: Path) -> None:
    mock_duckdb = MagicMock()
    mock_duckdb.get_schema.return_value = ["id", "nome"]
    svc = ParquetQueryService(duckdb_service=mock_duckdb, threshold_mb=0)
    result = svc.get_schema(parquet_pequeno)
    mock_duckdb.get_schema.assert_called_once_with(parquet_pequeno)
    assert result == ["id", "nome"]


# ---------------------------------------------------------------------------
# Testes: get_count
# ---------------------------------------------------------------------------


def test_get_count_polars_sem_filtros(parquet_pequeno: Path, svc_polars: ParquetQueryService) -> None:
    assert svc_polars.get_count(parquet_pequeno) == 20


def test_get_count_polars_com_filtro(parquet_pequeno: Path, svc_polars: ParquetQueryService) -> None:
    filtros = [FilterCondition(column="categoria", operator="igual", value="cat_1")]
    # cat_1 = (i % 3) + 1 == 1 → i % 3 == 0 → i=3,6,9,12,15,18 → 6 itens (i de 1 a 20, i%3==0)
    assert svc_polars.get_count(parquet_pequeno, filtros) == 6


def test_get_count_duckdb_sem_filtros(parquet_grande: Path, svc_duckdb: ParquetQueryService) -> None:
    assert svc_duckdb.get_count(parquet_grande) == 100


def test_get_count_duckdb_com_filtro(parquet_grande: Path, svc_duckdb: ParquetQueryService) -> None:
    filtros = [FilterCondition(column="valor", operator=">", value="900")]
    # valor = i * 10; >900 → i > 90 → 10 itens
    assert svc_duckdb.get_count(parquet_grande, filtros) == 10


# ---------------------------------------------------------------------------
# Testes: get_page
# ---------------------------------------------------------------------------


def test_get_page_polars_retorna_fatia(parquet_pequeno: Path, svc_polars: ParquetQueryService) -> None:
    result = svc_polars.get_page(parquet_pequeno, None, None, page=1, page_size=5)
    assert result.df_all_columns.height == 5
    assert result.total_rows == 20


def test_get_page_duckdb_retorna_fatia(parquet_grande: Path, svc_duckdb: ParquetQueryService) -> None:
    result = svc_duckdb.get_page(parquet_grande, None, None, page=1, page_size=10)
    assert result.df_all_columns.height == 10
    assert result.total_rows == 100


def test_get_page_polars_projection(parquet_pequeno: Path, svc_polars: ParquetQueryService) -> None:
    result = svc_polars.get_page(parquet_pequeno, None, ["id", "nome"], page=1, page_size=5)
    assert "id" in result.df_visible.columns
    assert "nome" in result.df_visible.columns
    assert "valor" not in result.df_visible.columns


def test_get_page_duckdb_projection(parquet_grande: Path, svc_duckdb: ParquetQueryService) -> None:
    result = svc_duckdb.get_page(parquet_grande, None, ["id", "nome"], page=1, page_size=5)
    assert "id" in result.df_visible.columns
    assert "nome" in result.df_visible.columns
    assert "valor" not in result.df_visible.columns


def test_get_page_delega_para_polars(parquet_pequeno: Path) -> None:
    mock_polars = MagicMock()
    mock_polars.get_page.return_value = MagicMock()
    svc = ParquetQueryService(polars_service=mock_polars, threshold_mb=999_999)
    svc.get_page(parquet_pequeno, None, None, page=1, page_size=10)
    mock_polars.get_page.assert_called_once()


def test_get_page_delega_para_duckdb(parquet_pequeno: Path) -> None:
    mock_duckdb = MagicMock()
    mock_duckdb.get_page.return_value = MagicMock()
    svc = ParquetQueryService(duckdb_service=mock_duckdb, threshold_mb=0)
    svc.get_page(parquet_pequeno, None, None, page=1, page_size=10)
    mock_duckdb.get_page.assert_called_once()


def test_query_service_duckdb_usa_v2_quando_disponivel(tmp_path: Path) -> None:
    """Arquivos grandes devem ser roteados ao DuckDB usando o path v2."""
    root = tmp_path / "v1"
    v2_root = tmp_path / "v2"
    root.mkdir()
    v2_root.mkdir()

    v1_path = root / "dados.parquet"
    v2_path = v2_root / "dados.parquet"
    pl.DataFrame({"id": [1], "origem": ["v1"]}).write_parquet(v1_path)
    pl.DataFrame({"id": [2], "origem": ["v2"]}).write_parquet(v2_path)

    mock_duckdb = MagicMock()
    mock_duckdb.get_count.return_value = 1
    polars_service = ParquetService(root=root, v2_root=v2_root)
    svc = ParquetQueryService(
        polars_service=polars_service,
        duckdb_service=mock_duckdb,
        threshold_mb=0,
        v2_root=v2_root,
    )

    assert svc.get_count(v1_path) == 1
    mock_duckdb.get_count.assert_called_once_with(v2_path, None)


# ---------------------------------------------------------------------------
# Testes: get_distinct_values
# ---------------------------------------------------------------------------


def test_get_distinct_values_polars(parquet_pequeno: Path, svc_polars: ParquetQueryService) -> None:
    valores = svc_polars.get_distinct_values(parquet_pequeno, "categoria")
    assert set(valores) == {"cat_1", "cat_2", "cat_3"}


def test_get_distinct_values_duckdb(parquet_grande: Path, svc_duckdb: ParquetQueryService) -> None:
    valores = svc_duckdb.get_distinct_values(parquet_grande, "categoria")
    assert set(valores) == {"cat_1", "cat_2", "cat_3", "cat_4"}


def test_get_distinct_values_polars_com_search(
    parquet_pequeno: Path, svc_polars: ParquetQueryService
) -> None:
    valores = svc_polars.get_distinct_values(parquet_pequeno, "categoria", search="cat_1")
    assert valores == ["cat_1"]


def test_get_distinct_values_duckdb_com_search(
    parquet_grande: Path, svc_duckdb: ParquetQueryService
) -> None:
    valores = svc_duckdb.get_distinct_values(parquet_grande, "categoria", search="cat_2")
    assert valores == ["cat_2"]


def test_get_distinct_values_polars_coluna_inexistente(
    parquet_pequeno: Path, svc_polars: ParquetQueryService
) -> None:
    valores = svc_polars.get_distinct_values(parquet_pequeno, "coluna_inexistente")
    assert valores == []


def test_get_distinct_values_polars_respeita_limit(
    parquet_pequeno: Path, svc_polars: ParquetQueryService
) -> None:
    valores = svc_polars.get_distinct_values(parquet_pequeno, "nome", limit=2)
    assert len(valores) <= 2


# ---------------------------------------------------------------------------
# Testes: export_to_parquet
# ---------------------------------------------------------------------------


def test_export_to_parquet_polars(
    parquet_pequeno: Path, tmp_path: Path, svc_polars: ParquetQueryService
) -> None:
    target = tmp_path / "saida.parquet"
    svc_polars.export_to_parquet(parquet_pequeno, None, None, target)
    assert target.exists()
    assert pl.read_parquet(target).height == 20


def test_export_to_parquet_duckdb(
    parquet_grande: Path, tmp_path: Path, svc_duckdb: ParquetQueryService
) -> None:
    target = tmp_path / "saida.parquet"
    svc_duckdb.export_to_parquet(parquet_grande, None, None, target)
    assert target.exists()
    assert pl.read_parquet(target).height == 100


def test_export_to_parquet_polars_com_filtro(
    parquet_pequeno: Path, tmp_path: Path, svc_polars: ParquetQueryService
) -> None:
    target = tmp_path / "filtrado.parquet"
    filtros = [FilterCondition(column="categoria", operator="igual", value="cat_1")]
    svc_polars.export_to_parquet(parquet_pequeno, filtros, ["id", "nome"], target)
    df = pl.read_parquet(target)
    assert set(df.columns) == {"id", "nome"}
    assert df.height == 6


def test_export_to_parquet_duckdb_com_filtro(
    parquet_grande: Path, tmp_path: Path, svc_duckdb: ParquetQueryService
) -> None:
    target = tmp_path / "filtrado.parquet"
    filtros = [FilterCondition(column="valor", operator=">", value="900")]
    svc_duckdb.export_to_parquet(parquet_grande, filtros, ["id", "valor"], target)
    df = pl.read_parquet(target)
    assert set(df.columns) == {"id", "valor"}
    assert df.height == 10


# ---------------------------------------------------------------------------
# Testes: export_to_csv
# ---------------------------------------------------------------------------


def test_export_to_csv_polars(
    parquet_pequeno: Path, tmp_path: Path, svc_polars: ParquetQueryService
) -> None:
    target = tmp_path / "saida.csv"
    svc_polars.export_to_csv(parquet_pequeno, None, None, target)
    assert target.exists()
    linhas = target.read_text(encoding="utf-8").splitlines()
    assert len(linhas) == 21  # 1 header + 20 dados


def test_export_to_csv_duckdb(
    parquet_grande: Path, tmp_path: Path, svc_duckdb: ParquetQueryService
) -> None:
    target = tmp_path / "saida.csv"
    svc_duckdb.export_to_csv(parquet_grande, None, None, target)
    assert target.exists()
    linhas = target.read_text(encoding="utf-8").splitlines()
    assert len(linhas) == 101  # 1 header + 100 dados


# ---------------------------------------------------------------------------
# Testes: diretorio particionado
# ---------------------------------------------------------------------------


def test_diretorio_usa_duckdb_independente_do_threshold(tmp_path: Path) -> None:
    svc = ParquetQueryService(threshold_mb=999_999)
    parquet_dir = tmp_path / "dataset"
    parquet_dir.mkdir()
    assert svc.usa_duckdb(parquet_dir) is True


def test_get_count_diretorio_particionado(tmp_path: Path) -> None:
    parquet_dir = tmp_path / "dataset"
    (parquet_dir / "ano=2023").mkdir(parents=True)
    pl.DataFrame({"id": [1, 2, 3], "val": [10.0, 20.0, 30.0]}).write_parquet(
        parquet_dir / "ano=2023" / "part-000.parquet"
    )
    svc = ParquetQueryService(threshold_mb=999_999)
    assert svc.get_count(parquet_dir) == 3
