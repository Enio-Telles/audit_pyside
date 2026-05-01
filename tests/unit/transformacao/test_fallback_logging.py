"""Tests for structured fallback logging in transformacao modules.

Instruments:
- fatores_conversao.py  → event "fatores_conversao.fallback"
- movimentacao_estoque.py → event "mov_estoque.fallback"

No Oracle, no real Parquet data, no GUI.
"""
from __future__ import annotations

import datetime
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest
import structlog
from structlog.testing import capture_logs

import transformacao.movimentacao_estoque_pkg.movimentacao_estoque as _me
import transformacao.rastreabilidade_produtos.fatores_conversao as _fc
from transformacao.rastreabilidade_produtos.fatores_conversao import calcular_fatores_conversao


@pytest.fixture(autouse=True)
def _reset_structlog_cache(monkeypatch):
    """Replace module-level log proxies with fresh instances before each test.

    When configure_structlog() runs earlier in the suite with
    cache_logger_on_first_use=True, BoundLoggerLazyProxy stores a finalised
    bind() on itself as an instance attribute.  capture_logs() patches the
    global processor list but cannot reach already-cached loggers.  Replacing
    the proxy with a fresh get_logger() call gives each test an uncached proxy
    that capture_logs() can intercept correctly, regardless of suite order.
    """
    monkeypatch.setattr(_fc, "log", structlog.get_logger(_fc.__name__))
    monkeypatch.setattr(_me, "log", structlog.get_logger(_me.__name__))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _preparar_contexto_basico(tmp_path: Path, cnpj: str) -> Path:
    """Create minimal Parquet files needed by calcular_fatores_conversao."""
    pasta_cnpj = tmp_path / cnpj
    pasta_analises = pasta_cnpj / "analises" / "produtos"
    pasta_analises.mkdir(parents=True, exist_ok=True)

    pl.DataFrame(
        {
            "descricao": ["Produto A", "Produto A"],
            "unid": ["UN", "CX"],
            "compras": [100.0, 200.0],
            "vendas": [0.0, 0.0],
            "qtd_compras": [10.0, 5.0],
            "qtd_vendas": [0.0, 0.0],
        }
    ).write_parquet(pasta_analises / f"item_unidades_{cnpj}.parquet")

    pl.DataFrame(
        {
            "id_agrupado": ["AGR_1"],
            "descricao_normalizada": ["PRODUTO A"],
            "descricao_final": ["Produto A"],
            "descr_padrao": ["Produto A"],
            "unid_ref_sugerida": ["UN"],
        }
    ).write_parquet(pasta_analises / f"produtos_final_{cnpj}.parquet")

    return pasta_cnpj


def _preparar_contexto_sem_preco(tmp_path: Path, cnpj: str) -> Path:
    """Create context where no price data exists → forces fallback_sem_preco."""
    pasta_cnpj = tmp_path / cnpj
    pasta_analises = pasta_cnpj / "analises" / "produtos"
    pasta_analises.mkdir(parents=True, exist_ok=True)

    pl.DataFrame(
        {
            "descricao": ["Produto B", "Produto B"],
            "unid": ["UN", "KG"],
            "compras": [0.0, 0.0],
            "vendas": [0.0, 0.0],
            "qtd_compras": [0.0, 0.0],
            "qtd_vendas": [0.0, 0.0],
        }
    ).write_parquet(pasta_analises / f"item_unidades_{cnpj}.parquet")

    pl.DataFrame(
        {
            "id_agrupado": ["AGR_2"],
            "descricao_normalizada": ["PRODUTO B"],
            "descricao_final": ["Produto B"],
            "descr_padrao": ["Produto B"],
            "unid_ref_sugerida": ["UN"],
        }
    ).write_parquet(pasta_analises / f"produtos_final_{cnpj}.parquet")

    return pasta_cnpj


# ---------------------------------------------------------------------------
# fatores_conversao.fallback
# ---------------------------------------------------------------------------


def test_fatores_conversao_sem_fallback_nao_emite_evento(tmp_path: Path) -> None:
    """When prices are available, fator_origem == 'preco' — no fallback event."""
    cnpj = "11111111000191"
    pasta_cnpj = _preparar_contexto_basico(tmp_path, cnpj)

    with capture_logs() as entries:
        calcular_fatores_conversao(cnpj, pasta_cnpj=pasta_cnpj)

    fallback_events = [e for e in entries if e.get("event") == "fatores_conversao.fallback"]
    assert fallback_events == [], f"Unexpected fallback events: {fallback_events}"


def test_fatores_conversao_sem_preco_emite_fallback_sem_preco(tmp_path: Path) -> None:
    """When no price data exists, fator_origem == 'fallback_sem_preco' → event emitted."""
    cnpj = "22222222000191"
    pasta_cnpj = _preparar_contexto_sem_preco(tmp_path, cnpj)

    with capture_logs() as entries:
        calcular_fatores_conversao(cnpj, pasta_cnpj=pasta_cnpj)

    fallback_events = [e for e in entries if e.get("event") == "fatores_conversao.fallback"]
    assert len(fallback_events) >= 1
    motivos = {e["motivo"] for e in fallback_events}
    assert motivos & {"fallback_sem_preco", "fallback_sem_preco_ref"}, (
        f"Expected a fallback_sem_preco* motivo, got: {motivos}"
    )
    assert all(e["cnpj"] == cnpj for e in fallback_events)


def test_fatores_conversao_fallback_evento_tem_campos_obrigatorios(tmp_path: Path) -> None:
    """Each fallback event must include: event, motivo, n_linhas, cnpj."""
    cnpj = "33333333000191"
    pasta_cnpj = _preparar_contexto_sem_preco(tmp_path, cnpj)

    with capture_logs() as entries:
        calcular_fatores_conversao(cnpj, pasta_cnpj=pasta_cnpj)

    fallback_events = [e for e in entries if e.get("event") == "fatores_conversao.fallback"]
    assert fallback_events, (
        "Expected at least one fatores_conversao.fallback event to be emitted, "
        "but none was found. Check that the dataset forces fator_origem != 'preco'."
    )
    for evt in fallback_events:
        assert "motivo" in evt, f"Missing 'motivo' in event: {evt}"
        assert "n_linhas" in evt, f"Missing 'n_linhas' in event: {evt}"
        assert "cnpj" in evt, f"Missing 'cnpj' in event: {evt}"
        assert evt["n_linhas"] > 0, f"n_linhas must be > 0, got: {evt}"


# ---------------------------------------------------------------------------
# mov_estoque.fallback
# ---------------------------------------------------------------------------


def _preparar_contexto_mov_estoque(tmp_path: Path, cnpj: str) -> Path:
    """Create the minimal Parquet environment for gerar_movimentacao_estoque.

    Provides:
    - produtos_final_{cnpj}.parquet  (required schema)
    - fatores_conversao_{cnpj}.parquet  (required schema)
    - c170_{cnpj}.parquet  (one row, so df_parts is non-empty)

    The c170 row has all mapping keys from map_estoque.json as null/empty so
    the pipeline can build df_mov and reach the apply_conversion_factors call.
    """
    pasta_cnpj = tmp_path / cnpj
    pasta_analises = pasta_cnpj / "analises" / "produtos"
    pasta_brutos = pasta_cnpj / "arquivos_parquet"
    pasta_analises.mkdir(parents=True, exist_ok=True)
    pasta_brutos.mkdir(parents=True, exist_ok=True)

    pl.DataFrame(
        {
            "id_agrupado": ["AGR_T"],
            "descricao_normalizada": ["PRODUTO T"],
            "descr_padrao": ["Produto T"],
            "ncm_padrao": ["00000000"],
            "cest_padrao": [None],
            "descricao_final": ["Produto T"],
            "co_sefin_final": [None],
            "unid_ref_sugerida": ["UN"],
        }
    ).write_parquet(pasta_analises / f"produtos_final_{cnpj}.parquet")

    pl.DataFrame(
        {
            "id_agrupado": ["AGR_T"],
            "unid": ["UN"],
            "unid_ref": ["UN"],
            "fator": [1.0],
            "fator_origem": ["preco"],
        }
    ).write_parquet(pasta_analises / f"fatores_conversao_{cnpj}.parquet")

    # Minimal c170 row — dt_doc and dt_e_s must be real dates so that
    # calculo_saldos._gerar_eventos_estoque() can call .dt.year() without error.
    _dt = datetime.date(2023, 6, 15)
    pl.DataFrame(
        {
            "ind_oper": ["1"],
            "cod_item": [None],
            "descr_item": [None],
            "unid": ["UN"],
            "qtd": [1.0],
            "vl_item": [0.0],
            "vl_desc": [0.0],
            "cfop": [None],
            "cst_icms": [None],
            "aliq_icms": [None],
            "vl_icms": [None],
            "aliq_st": [None],
            "vl_bc_icms_st": [None],
            "vl_icms_st": [None],
            "vl_bc_icms": [None],
            "cest": [None],
            "cod_ncm": [None],
            "nsu": [None],
            "num_doc": [None],
            "num_item": [None],
            "ser": [None],
            "dt_doc": [_dt],
            "dt_e_s": [_dt],
            "tipo_item": [None],
            "descr_compl": [None],
            "it_in_st": [None],
            "it_in_combustivel": [None],
            "it_in_isento_icms": [None],
            "it_in_mva_ajustado": [None],
            "it_in_pmpf": [None],
            "it_in_reducao_credito": [None],
            "it_pc_interna": [None],
            "it_pc_mva": [None],
            "it_pc_reducao": [None],
            "cod_barra": [None],
            "chv_nfe": pl.Series([""], dtype=pl.String),
            "co_sefin_agr": [None],
            "id_agrupado": ["AGR_T"],
        }
    ).write_parquet(pasta_brutos / f"c170_{cnpj}.parquet")

    return pasta_cnpj


def test_mov_estoque_fallback_emitido_quando_apply_conversion_factors_falha(
    tmp_path: Path, monkeypatch
) -> None:
    """When apply_conversion_factors raises, 'mov_estoque.fallback' is emitted.

    Exercises the real production path: gerar_movimentacao_estoque reaches the
    try/except around MovimentacaoService.apply_conversion_factors, which raises,
    triggering the structured log.warning call.
    """
    cnpj = "99999999000191"
    pasta_cnpj = _preparar_contexto_mov_estoque(tmp_path, cnpj)

    # Patch apply_conversion_factors to raise so the except branch is reached.
    monkeypatch.setattr(
        _me.MovimentacaoService,
        "apply_conversion_factors",
        staticmethod(lambda df, df_prod_final=None: (_ for _ in ()).throw(
            RuntimeError("fator ausente simulado")
        )),
    )

    # Patch salvar_para_parquet to avoid touching the filesystem after the test.
    monkeypatch.setattr(_me, "salvar_para_parquet", lambda *a, **kw: True)

    with capture_logs() as entries:
        try:
            _me.gerar_movimentacao_estoque(cnpj, pasta_cnpj=pasta_cnpj)
        except Exception:
            # Subsequent DataFrame operations may fail on minimal test data;
            # the mov_estoque.fallback log.warning was already emitted before
            # the ComputeError propagates from later stages.
            pass

    fallback_events = [e for e in entries if e.get("event") == "mov_estoque.fallback"]
    assert len(fallback_events) == 1, (
        f"Expected exactly 1 mov_estoque.fallback event, got: {fallback_events}"
    )
    evt = fallback_events[0]
    assert evt["motivo"] == "apply_conversion_factors_falhou"
    assert evt["exc_type"] == "RuntimeError"
    assert evt["cnpj"] == cnpj
