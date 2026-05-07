"""
Testes para PR 0.1 — instrumentacao de leitura de Parquet grande.

Verifica:
- log_parquet_open recebe path, method e metricas corretas
- is_large_parquet retorna True acima do threshold e False abaixo
- load_dataset nao bloqueia arquivos grandes (apenas loga e marca)
- Threshold LARGE_PARQUET_THRESHOLD_MB = 512 esta exposto em config
"""
from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import polars as pl
import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def parquet_pequeno(tmp_path: Path) -> Path:
    """Fixture: Parquet sintetico pequeno (~3 KB)."""
    path = tmp_path / "pequeno.parquet"
    pl.DataFrame({"a": list(range(100)), "b": ["x"] * 100}).write_parquet(path)
    return path


@pytest.fixture()
def parquet_grande_simulado(tmp_path: Path, monkeypatch) -> Path:
    """Fixture: Parquet sintetico pequeno com stat.st_size fingido acima de 512 MB."""
    path = tmp_path / "grande.parquet"
    pl.DataFrame({"a": list(range(10)), "b": ["y"] * 10}).write_parquet(path)

    _threshold_bytes = (512 + 1) * 1024 * 1024
    original_stat = Path.stat

    def fake_stat(self, *args, **kwargs):
        result = original_stat(self, *args, **kwargs)
        if self == path:
            import os
            return os.stat_result(
                (
                    result.st_mode,
                    result.st_ino,
                    result.st_dev,
                    result.st_nlink,
                    result.st_uid,
                    result.st_gid,
                    _threshold_bytes,  # st_size fingido
                    result.st_atime,
                    result.st_mtime,
                    result.st_ctime,
                )
            )
        return result

    monkeypatch.setattr(Path, "stat", fake_stat)
    return path


# ---------------------------------------------------------------------------
# Testes: config
# ---------------------------------------------------------------------------


def test_large_parquet_threshold_mb_exposto_no_config():
    from interface_grafica.config import LARGE_PARQUET_THRESHOLD_MB

    assert LARGE_PARQUET_THRESHOLD_MB == 512


# ---------------------------------------------------------------------------
# Testes: log_parquet_open
# ---------------------------------------------------------------------------


def test_log_parquet_open_registra_evento(tmp_path: Path):
    """log_parquet_open deve chamar registrar_evento_performance com path e metricas."""
    import utilitarios.perf_monitor as mod
    from utilitarios.perf_monitor import log_parquet_open

    path = tmp_path / "arq.parquet"
    pl.DataFrame({"x": [1, 2, 3]}).write_parquet(path)

    eventos_capturados: list[dict[str, Any]] = []

    def fake_registrar(evento, duracao_s=None, contexto=None, status="ok"):
        eventos_capturados.append(
            {"evento": evento, "duracao_s": duracao_s, "contexto": contexto, "status": status}
        )

    with patch.object(mod, "registrar_evento_performance", fake_registrar):
        log_parquet_open(path, "load_dataset", rows=3, cols=1, elapsed_ms=42.5)

    assert len(eventos_capturados) == 1
    ev = eventos_capturados[0]
    assert ev["evento"] == "parquet_open"
    ctx = ev["contexto"]
    assert ctx["method"] == "load_dataset"
    assert ctx["rows"] == 3
    assert ctx["cols"] == 1
    assert ctx["elapsed_ms"] == 42.5
    assert "path" in ctx
    assert "size_mb" in ctx


def test_log_parquet_open_rss_negativo_quando_psutil_ausente(tmp_path: Path):
    """Quando psutil nao disponivel, rss_before_mb e rss_after_mb devem ser -1."""
    import utilitarios.perf_monitor as mod
    from utilitarios.perf_monitor import log_parquet_open

    path = tmp_path / "arq2.parquet"
    pl.DataFrame({"x": [1]}).write_parquet(path)

    eventos_capturados: list[dict[str, Any]] = []

    def fake_registrar(evento, duracao_s=None, contexto=None, status="ok"):
        eventos_capturados.append({"evento": evento, "contexto": contexto})

    with patch.object(mod, "_HAS_PSUTIL", False):
        with patch.object(mod, "registrar_evento_performance", fake_registrar):
            log_parquet_open(path, "get_page", rows=1, cols=1, elapsed_ms=10.0)

    ctx = eventos_capturados[0]["contexto"]
    assert ctx["rss_before_mb"] == -1
    assert ctx["rss_after_mb"] == -1


def test_log_parquet_open_aceita_rss_explicito(tmp_path: Path):
    """Quando rss_before e rss_after sao fornecidos, devem aparecer convertidos para MB."""
    import utilitarios.perf_monitor as mod
    from utilitarios.perf_monitor import log_parquet_open

    path = tmp_path / "arq3.parquet"
    pl.DataFrame({"x": [1]}).write_parquet(path)

    capturado: list[dict] = []

    def fake_registrar(evento, duracao_s=None, contexto=None, status="ok"):
        capturado.append({"contexto": contexto})

    with patch.object(mod, "registrar_evento_performance", fake_registrar):
        log_parquet_open(
            path,
            "get_page",
            rows=1,
            cols=1,
            elapsed_ms=5.0,
            rss_before=100 * 1024 * 1024,  # 100 MB
            rss_after=200 * 1024 * 1024,   # 200 MB
        )

    ctx = capturado[0]["contexto"]
    assert abs(ctx["rss_before_mb"] - 100.0) < 0.1
    assert abs(ctx["rss_after_mb"] - 200.0) < 0.1


# ---------------------------------------------------------------------------
# Testes: is_large_parquet
# ---------------------------------------------------------------------------


def test_is_large_parquet_false_para_arquivo_pequeno(parquet_pequeno: Path):
    from interface_grafica.services.parquet_service import ParquetService

    assert ParquetService.is_large_parquet(parquet_pequeno) is False


def test_is_large_parquet_true_para_arquivo_acima_threshold(
    parquet_grande_simulado: Path,
):
    from interface_grafica.services.parquet_service import ParquetService

    assert ParquetService.is_large_parquet(parquet_grande_simulado) is True


def test_is_large_parquet_false_para_path_inexistente(tmp_path: Path):
    from interface_grafica.services.parquet_service import ParquetService

    assert ParquetService.is_large_parquet(tmp_path / "nao_existe.parquet") is False


# ---------------------------------------------------------------------------
# Testes: load_dataset bloqueia arquivo grande (PR 0.2 substituiu comportamento PR 0.1)
# ---------------------------------------------------------------------------


def test_load_dataset_nao_bloqueia_arquivo_grande(
    parquet_grande_simulado: Path, tmp_path: Path
):
    """A partir da PR 0.2 load_dataset levanta LargeParquetForbiddenError para arquivo grande.

    O nome do teste e preservado para compatibilidade de historico, mas a expectativa mudou:
    arquivos grandes sao bloqueados por padrao (allow_full_load=False).
    """
    from interface_grafica.services.parquet_service import LargeParquetForbiddenError, ParquetService

    svc = ParquetService(root=tmp_path)
    with pytest.raises(LargeParquetForbiddenError):
        svc.load_dataset(parquet_grande_simulado)


def test_load_dataset_arquivo_grande_evento_tem_status_aviso(
    parquet_grande_simulado: Path, tmp_path: Path
):
    """O evento 'arquivo_grande' deve ser emitido com status='aviso' mesmo ao bloquear."""
    import interface_grafica.services.parquet_service as mod

    eventos_completos: list[dict] = []

    def fake_registrar(evento, duracao_s=None, contexto=None, status="ok"):
        eventos_completos.append({"evento": evento, "status": status, "contexto": contexto})

    from interface_grafica.services.parquet_service import LargeParquetForbiddenError, ParquetService

    with patch.object(mod, "registrar_evento_performance", fake_registrar):
        svc = ParquetService(root=tmp_path)
        with pytest.raises(LargeParquetForbiddenError):
            svc.load_dataset(parquet_grande_simulado)

    avisos = [e for e in eventos_completos if e["status"] == "aviso"]
    assert len(avisos) >= 1
    ev = avisos[0]
    assert "arquivo_grande" in ev["evento"]
    assert ev["contexto"]["threshold_mb"] == 512


# ---------------------------------------------------------------------------
# Testes: load_dataset chama log_parquet_open em arquivo pequeno
# ---------------------------------------------------------------------------


def test_load_dataset_chama_log_parquet_open(parquet_pequeno: Path, tmp_path: Path):
    """load_dataset deve chamar log_parquet_open com method='load_dataset'."""
    import interface_grafica.services.parquet_service as mod

    chamadas: list[dict] = []

    def fake_log(path, method, rows, cols, elapsed_ms, rss_before=-1, rss_after=-1):
        chamadas.append(
            {"path": path, "method": method, "rows": rows, "cols": cols, "elapsed_ms": elapsed_ms}
        )

    with patch.object(mod, "log_parquet_open", fake_log):
        svc = mod.ParquetService(root=tmp_path)
        df = svc.load_dataset(parquet_pequeno)

    assert len(chamadas) == 1
    c = chamadas[0]
    assert c["method"] == "load_dataset"
    assert c["rows"] == df.height
    assert c["cols"] == df.width
    assert c["elapsed_ms"] > 0


def test_obter_data_entrega_reg0000_chama_log_parquet_open(tmp_path: Path):
    """Leitura direta do pipeline GUI deve usar utilitarios.perf_monitor.log_parquet_open."""
    import interface_grafica.services.pipeline_funcoes_service as mod

    cnpj = "12345678901234"
    pasta = tmp_path / cnpj / "arquivos_parquet"
    pasta.mkdir(parents=True)
    arquivo = pasta / f"reg_0000_{cnpj}.parquet"
    pl.DataFrame({"data_entrega": ["2026-05-07"]}).write_parquet(arquivo)

    chamadas: list[dict] = []

    def fake_log(path, method, rows, cols, elapsed_ms, rss_before=-1, rss_after=-1):
        chamadas.append(
            {"path": path, "method": method, "rows": rows, "cols": cols, "elapsed_ms": elapsed_ms}
        )

    with patch.object(mod, "log_parquet_open", fake_log):
        svc = mod.ServicoExtracao(cnpj_root=tmp_path)
        assert svc.obter_data_entrega_reg0000(cnpj) == "2026-05-07"

    assert chamadas == [
        {
            "path": arquivo,
            "method": "ServicoExtracao.obter_data_entrega_reg0000",
            "rows": 1,
            "cols": 1,
            "elapsed_ms": chamadas[0]["elapsed_ms"],
        }
    ]
    assert chamadas[0]["elapsed_ms"] >= 0


def test_ler_parquet_colunas_chama_log_parquet_open(tmp_path: Path):
    """Leitura direta do servico de agregacao deve usar o helper canonico."""
    import interface_grafica.services.aggregation_service as mod

    arquivo = tmp_path / "dados.parquet"
    pl.DataFrame({"a": [1, 2], "b": ["x", "y"]}).write_parquet(arquivo)
    chamadas: list[dict] = []

    def fake_log(path, method, rows, cols, elapsed_ms, rss_before=-1, rss_after=-1):
        chamadas.append(
            {"path": path, "method": method, "rows": rows, "cols": cols, "elapsed_ms": elapsed_ms}
        )

    with patch.object(mod, "log_parquet_open", fake_log):
        df = mod.ServicoAgregacao._ler_parquet_colunas(arquivo, ["a"])

    assert df.shape == (2, 1)
    assert chamadas[0]["path"] == arquivo
    assert chamadas[0]["method"] == "ServicoAgregacao._ler_parquet_colunas"
    assert chamadas[0]["rows"] == 2
    assert chamadas[0]["cols"] == 1
    assert chamadas[0]["elapsed_ms"] >= 0
