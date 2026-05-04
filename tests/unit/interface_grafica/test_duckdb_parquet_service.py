"""
Testes para DuckDBParquetService — Phase 1.1.

Verifica:
- get_schema retorna colunas corretas
- get_count com/sem filtros retorna contagem correta
- get_page retorna fatia correta sem materializar o arquivo inteiro
- get_distinct_values com search retorna ate limit
- export_query_to_parquet gera arquivo sem coletar tudo em Python
- export_query_to_csv gera arquivo sem coletar tudo em Python
- Cada chamada usa conexao independente (thread safety)
- get_page nao materializa arquivo inteiro (projection pushdown)
"""
from __future__ import annotations

import threading
from pathlib import Path

import polars as pl
import pytest

from interface_grafica.services.duckdb_parquet_service import DuckDBParquetService
from interface_grafica.services.parquet_service import FilterCondition


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def fixture_parquet(tmp_path: Path) -> Path:
    """Parquet sintetico com dados variadosobre 3 colunas."""
    path = tmp_path / "fixture.parquet"
    pl.DataFrame(
        {
            "id": list(range(1, 51)),
            "nome": [f"produto_{i:02d}" for i in range(1, 51)],
            "valor": [float(i * 10) for i in range(1, 51)],
        }
    ).write_parquet(path)
    return path


@pytest.fixture()
def fixture_parquet_grande(tmp_path: Path) -> Path:
    """Parquet sintetico com mais linhas para testar paginacao."""
    path = tmp_path / "grande.parquet"
    pl.DataFrame(
        {
            "id": list(range(1, 1001)),
            "nome": [f"item_{i:04d}" for i in range(1, 1001)],
            "valor": [float(i) for i in range(1, 1001)],
            "categoria": [f"cat_{(i % 5) + 1}" for i in range(1, 1001)],
        }
    ).write_parquet(path)
    return path


@pytest.fixture()
def svc() -> DuckDBParquetService:
    return DuckDBParquetService()


# ---------------------------------------------------------------------------
# Testes: get_schema
# ---------------------------------------------------------------------------


def test_get_schema_retorna_colunas_corretas(svc: DuckDBParquetService, fixture_parquet: Path) -> None:
    colunas = svc.get_schema(fixture_parquet)
    assert set(colunas) == {"id", "nome", "valor"}


def test_get_schema_nao_materializa_dados(svc: DuckDBParquetService, fixture_parquet: Path) -> None:
    """get_schema deve ser rapido — nao carrega linhas."""
    colunas = svc.get_schema(fixture_parquet)
    assert len(colunas) == 3


# ---------------------------------------------------------------------------
# Testes: get_count
# ---------------------------------------------------------------------------


def test_get_count_sem_filtros(svc: DuckDBParquetService, fixture_parquet: Path) -> None:
    assert svc.get_count(fixture_parquet) == 50


def test_get_count_com_filtro_igual(svc: DuckDBParquetService, fixture_parquet: Path) -> None:
    filtros = [FilterCondition(column="nome", operator="igual", value="produto_01")]
    assert svc.get_count(fixture_parquet, filtros) == 1


def test_get_count_com_filtro_contem(svc: DuckDBParquetService, fixture_parquet: Path) -> None:
    filtros = [FilterCondition(column="nome", operator="contem", value="produto_0")]
    contagem = svc.get_count(fixture_parquet, filtros)
    # produto_01 a produto_09 = 9 itens
    assert contagem == 9


def test_get_count_com_filtro_maior(svc: DuckDBParquetService, fixture_parquet: Path) -> None:
    filtros = [FilterCondition(column="valor", operator=">", value="400")]
    contagem = svc.get_count(fixture_parquet, filtros)
    # valor = id * 10; valor > 400 -> id > 40 -> 41..50 = 10
    assert contagem == 10


def test_get_count_coluna_inexistente_ignorada(svc: DuckDBParquetService, fixture_parquet: Path) -> None:
    filtros = [FilterCondition(column="coluna_que_nao_existe", operator="igual", value="x")]
    assert svc.get_count(fixture_parquet, filtros) == 50


# ---------------------------------------------------------------------------
# Testes: get_page
# ---------------------------------------------------------------------------


def test_get_page_retorna_fatia_correta(svc: DuckDBParquetService, fixture_parquet_grande: Path) -> None:
    result = svc.get_page(fixture_parquet_grande, None, None, page=1, page_size=10)
    assert result.df_all_columns.height == 10
    assert result.total_rows == 1000


def test_get_page_segunda_pagina(svc: DuckDBParquetService, fixture_parquet_grande: Path) -> None:
    result = svc.get_page(fixture_parquet_grande, None, None, page=2, page_size=10)
    assert result.df_all_columns.height == 10
    assert result.total_rows == 1000


def test_get_page_projection_pushdown(svc: DuckDBParquetService, fixture_parquet_grande: Path) -> None:
    """get_page deve retornar apenas as colunas visiveis solicitadas."""
    result = svc.get_page(
        fixture_parquet_grande, None, visible_columns=["id", "nome"], page=1, page_size=5
    )
    assert "id" in result.df_visible.columns
    assert "nome" in result.df_visible.columns
    # 'valor' e 'categoria' nao foram solicitadas
    assert "valor" not in result.df_visible.columns
    assert "categoria" not in result.df_visible.columns


def test_get_page_schema_completo_disponivel(svc: DuckDBParquetService, fixture_parquet_grande: Path) -> None:
    """get_page deve expor schema completo mesmo com projection pushdown."""
    result = svc.get_page(
        fixture_parquet_grande, None, visible_columns=["id"], page=1, page_size=5
    )
    assert set(result.columns) == {"id", "nome", "valor", "categoria"}


def test_get_page_com_filtro(svc: DuckDBParquetService, fixture_parquet_grande: Path) -> None:
    filtros = [FilterCondition(column="categoria", operator="igual", value="cat_1")]
    result = svc.get_page(fixture_parquet_grande, filtros, None, page=1, page_size=50)
    # cat_1 = indices onde (i % 5) + 1 == 1, ou seja i % 5 == 0: i=5,10,...,1000 = 200 itens
    assert result.total_rows == 200


def test_get_page_com_sort(svc: DuckDBParquetService, fixture_parquet_grande: Path) -> None:
    result = svc.get_page(
        fixture_parquet_grande, None, ["id", "valor"],
        page=1, page_size=5, sort_by="valor", sort_desc=True
    )
    valores = result.df_visible["valor"].to_list()
    assert valores == sorted(valores, reverse=True)


def test_get_page_nao_materializa_arquivo_completo(
    svc: DuckDBParquetService, fixture_parquet_grande: Path
) -> None:
    """get_page deve retornar apenas page_size linhas, nao 1000."""
    result = svc.get_page(fixture_parquet_grande, None, None, page=1, page_size=20)
    assert result.df_all_columns.height == 20


def test_get_page_ultima_pagina_parcial(svc: DuckDBParquetService, fixture_parquet: Path) -> None:
    """Ultima pagina pode ter menos linhas que page_size."""
    result = svc.get_page(fixture_parquet, None, None, page=3, page_size=20)
    # 50 linhas: pagina 3 offset 40 -> 10 linhas restantes
    assert result.df_all_columns.height == 10


# ---------------------------------------------------------------------------
# Testes: get_distinct_values
# ---------------------------------------------------------------------------


def test_get_distinct_values_sem_search(svc: DuckDBParquetService, fixture_parquet_grande: Path) -> None:
    valores = svc.get_distinct_values(fixture_parquet_grande, "categoria")
    assert set(valores) == {"cat_1", "cat_2", "cat_3", "cat_4", "cat_5"}


def test_get_distinct_values_com_search(svc: DuckDBParquetService, fixture_parquet_grande: Path) -> None:
    valores = svc.get_distinct_values(fixture_parquet_grande, "categoria", search="cat_1")
    assert valores == ["cat_1"]


def test_get_distinct_values_respeita_limit(svc: DuckDBParquetService, fixture_parquet_grande: Path) -> None:
    valores = svc.get_distinct_values(fixture_parquet_grande, "nome", limit=10)
    assert len(valores) <= 10


def test_get_distinct_values_coluna_inexistente(
    svc: DuckDBParquetService, fixture_parquet_grande: Path
) -> None:
    valores = svc.get_distinct_values(fixture_parquet_grande, "coluna_inexistente")
    assert valores == []


# ---------------------------------------------------------------------------
# Testes: export_query_to_parquet
# ---------------------------------------------------------------------------


def test_export_to_parquet_gera_arquivo(svc: DuckDBParquetService, fixture_parquet: Path, tmp_path: Path) -> None:
    target = tmp_path / "saida.parquet"
    svc.export_query_to_parquet(fixture_parquet, None, None, target)
    assert target.exists()
    df = pl.read_parquet(target)
    assert df.height == 50


def test_export_to_parquet_com_filtro(svc: DuckDBParquetService, fixture_parquet: Path, tmp_path: Path) -> None:
    target = tmp_path / "filtrado.parquet"
    filtros = [FilterCondition(column="valor", operator=">", value="400")]
    svc.export_query_to_parquet(fixture_parquet, filtros, ["id", "nome", "valor"], target)
    df = pl.read_parquet(target)
    assert df.height == 10
    assert all(v > 400 for v in df["valor"].to_list())


def test_export_to_parquet_projection(svc: DuckDBParquetService, fixture_parquet: Path, tmp_path: Path) -> None:
    target = tmp_path / "projetado.parquet"
    svc.export_query_to_parquet(fixture_parquet, None, ["id", "nome"], target)
    df = pl.read_parquet(target)
    assert set(df.columns) == {"id", "nome"}


# ---------------------------------------------------------------------------
# Testes: export_query_to_csv
# ---------------------------------------------------------------------------


def test_export_to_csv_gera_arquivo(svc: DuckDBParquetService, fixture_parquet: Path, tmp_path: Path) -> None:
    target = tmp_path / "saida.csv"
    svc.export_query_to_csv(fixture_parquet, None, None, target)
    assert target.exists()
    linhas = target.read_text(encoding="utf-8").splitlines()
    # 1 header + 50 dados
    assert len(linhas) == 51


def test_export_to_csv_com_filtro(svc: DuckDBParquetService, fixture_parquet: Path, tmp_path: Path) -> None:
    target = tmp_path / "filtrado.csv"
    filtros = [FilterCondition(column="nome", operator="igual", value="produto_01")]
    svc.export_query_to_csv(fixture_parquet, filtros, None, target)
    linhas = target.read_text(encoding="utf-8").splitlines()
    assert len(linhas) == 2  # header + 1 linha


# ---------------------------------------------------------------------------
# Testes: thread safety
# ---------------------------------------------------------------------------


def test_conexoes_independentes_por_thread(
    fixture_parquet: Path, tmp_path: Path
) -> None:
    """Duas threads usando DuckDBParquetService nao devem compartilhar conexao."""
    resultados: list[int] = []
    erros: list[Exception] = []

    def worker(page: int) -> None:
        try:
            svc = DuckDBParquetService()
            result = svc.get_page(fixture_parquet, None, None, page=page, page_size=10)
            resultados.append(result.df_all_columns.height)
        except Exception as exc:
            erros.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(1, 4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not erros, f"Erros em threads: {erros}"
    assert all(r == 10 for r in resultados)


# ---------------------------------------------------------------------------
# Testes: diretorio particionado
# ---------------------------------------------------------------------------


def test_get_count_diretorio_particionado(tmp_path: Path) -> None:
    """DuckDBParquetService deve aceitar diretorio particionado."""
    parquet_dir = tmp_path / "dataset"
    (parquet_dir / "ano=2023").mkdir(parents=True)
    (parquet_dir / "ano=2024").mkdir(parents=True)
    pl.DataFrame({"id": [1, 2], "val": [10.0, 20.0]}).write_parquet(
        parquet_dir / "ano=2023" / "part-000.parquet"
    )
    pl.DataFrame({"id": [3, 4], "val": [30.0, 40.0]}).write_parquet(
        parquet_dir / "ano=2024" / "part-000.parquet"
    )

    svc = DuckDBParquetService()
    total = svc.get_count(parquet_dir)
    assert total == 4


def test_get_page_diretorio_particionado(tmp_path: Path) -> None:
    parquet_dir = tmp_path / "dataset"
    (parquet_dir / "ano=2023").mkdir(parents=True)
    pl.DataFrame({"id": list(range(1, 11)), "val": [float(i) for i in range(1, 11)]}).write_parquet(
        parquet_dir / "ano=2023" / "part-000.parquet"
    )

    svc = DuckDBParquetService()
    result = svc.get_page(parquet_dir, None, None, page=1, page_size=5)
    assert result.df_all_columns.height == 5
    assert result.total_rows == 10
