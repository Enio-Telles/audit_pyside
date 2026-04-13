from __future__ import annotations

import sys
from pathlib import Path
from uuid import uuid4

import polars as pl

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from backend.routers import estoque


def _criar_pasta_temporaria_local() -> Path:
    pasta = ROOT_DIR / "_tmp_testes" / f"estoque_router_{uuid4().hex}"
    pasta.mkdir(parents=True, exist_ok=True)
    return pasta


def test_ler_tabela_estoque_ou_vazia_retorna_resposta_vazia_quando_parquet_nao_existe():
    pasta_temporaria = _criar_pasta_temporaria_local()
    resposta = estoque._ler_tabela_estoque_ou_vazia(
        pasta_temporaria / "inexistente.parquet",
        page=2,
        page_size=100,
    )

    assert resposta == {
        "total_rows": 0,
        "page": 2,
        "page_size": 100,
        "total_pages": 1,
        "columns": [],
        "rows": [],
    }


def test_ler_tabela_estoque_ou_vazia_mantem_paginacao_quando_parquet_existe():
    pasta_temporaria = _criar_pasta_temporaria_local()
    caminho_parquet = pasta_temporaria / "mov_estoque_123.parquet"
    pl.DataFrame(
        {
            "id_agrupado": ["A", "B"],
            "saldo": [10.0, 20.0],
        }
    ).write_parquet(caminho_parquet)

    resposta = estoque._ler_tabela_estoque_ou_vazia(
        caminho_parquet,
        page=1,
        page_size=1,
    )

    assert resposta["total_rows"] == 2
    assert resposta["page"] == 1
    assert resposta["page_size"] == 1
    assert resposta["total_pages"] == 2
    assert resposta["columns"] == ["id_agrupado", "saldo"]
    assert resposta["rows"] == [{"id_agrupado": "A", "saldo": 10.0}]
