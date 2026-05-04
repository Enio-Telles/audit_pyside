"""
Testes para PR 0.2 — guard rail: bloquear leitura integral de Parquet grande.

Verifica:
- LargeParquetForbiddenError e levantado quando arquivo > 512 MB e allow_full_load=False (default)
- allow_full_load=True permite a leitura e retorna DataFrame correto
- Arquivos pequenos continuam funcionando sem alteracao
- _carregar_dataset_ui retorna DataFrame vazio (nao propaga excecao)
- _carregar_dados_parquet_async retorna sem chamar callback para arquivo grande
"""
from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from interface_grafica.config import LARGE_PARQUET_THRESHOLD_MB
from interface_grafica.services.parquet_service import (
    LargeParquetForbiddenError,
    ParquetService,
)


# ---------------------------------------------------------------------------
# Fixtures compartilhadas
# ---------------------------------------------------------------------------


@pytest.fixture()
def parquet_pequeno(tmp_path: Path) -> Path:
    """Fixture: Parquet sintetico pequeno."""
    path = tmp_path / "pequeno.parquet"
    pl.DataFrame({"a": list(range(100)), "b": ["x"] * 100}).write_parquet(path)
    return path


@pytest.fixture()
def parquet_grande_simulado(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Fixture: Parquet sintetico pequeno com stat.st_size fingido acima do threshold."""
    path = tmp_path / "grande.parquet"
    pl.DataFrame({"a": list(range(10)), "b": ["y"] * 10}).write_parquet(path)

    _threshold_bytes = (LARGE_PARQUET_THRESHOLD_MB + 1) * 1024 * 1024
    original_stat = Path.stat

    def fake_stat(self: Path, *args: Any, **kwargs: Any):  # type: ignore[override]
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
# Testes: LargeParquetForbiddenError
# ---------------------------------------------------------------------------


def test_large_parquet_forbidden_error_eh_exception():
    """LargeParquetForbiddenError deve ser subclasse de Exception."""
    assert issubclass(LargeParquetForbiddenError, Exception)


def test_large_parquet_forbidden_error_mensagem():
    """LargeParquetForbiddenError deve conter path, size_mb e threshold_mb na mensagem."""
    path = Path("/dados/grande.parquet")
    exc = LargeParquetForbiddenError(path, size_mb=600.5, threshold_mb=512)
    msg = str(exc)
    assert "600" in msg
    assert "512" in msg


def test_large_parquet_forbidden_error_atributos():
    """LargeParquetForbiddenError deve expor atributos parquet_path, size_mb, threshold_mb."""
    path = Path("/dados/grande.parquet")
    exc = LargeParquetForbiddenError(path, size_mb=700.0, threshold_mb=512)
    assert exc.parquet_path == path
    assert exc.size_mb == 700.0
    assert exc.threshold_mb == 512


# ---------------------------------------------------------------------------
# Testes: load_dataset — bloqueio e allow_full_load
# ---------------------------------------------------------------------------


def test_load_dataset_bloqueia_parquet_grande(
    parquet_grande_simulado: Path, tmp_path: Path
) -> None:
    """load_dataset deve levantar LargeParquetForbiddenError para arquivo grande (default)."""
    service = ParquetService(root=tmp_path)
    with pytest.raises(LargeParquetForbiddenError):
        service.load_dataset(parquet_grande_simulado)


def test_load_dataset_allow_full_load_permite_leitura(
    parquet_grande_simulado: Path, tmp_path: Path
) -> None:
    """allow_full_load=True deve permitir a leitura e retornar DataFrame correto."""
    service = ParquetService(root=tmp_path)
    df = service.load_dataset(parquet_grande_simulado, allow_full_load=True)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 10
    assert "a" in df.columns


def test_load_dataset_nao_bloqueia_parquet_pequeno(
    parquet_pequeno: Path, tmp_path: Path
) -> None:
    """Arquivo pequeno nao deve levantar LargeParquetForbiddenError."""
    service = ParquetService(root=tmp_path)
    df = service.load_dataset(parquet_pequeno)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 100


def test_load_dataset_bloqueia_antes_de_ler_disco(
    parquet_grande_simulado: Path, tmp_path: Path
) -> None:
    """load_dataset deve bloquear sem executar scan do disco para arquivo grande."""
    service = ParquetService(root=tmp_path)
    with patch.object(service, "build_lazyframe") as mock_lf:
        with pytest.raises(LargeParquetForbiddenError):
            service.load_dataset(parquet_grande_simulado)
        mock_lf.assert_not_called()


# ---------------------------------------------------------------------------
# Testes: allow_full_load=False — parametro keyword-only
# ---------------------------------------------------------------------------


def test_load_dataset_allow_full_load_e_keyword_only(
    parquet_pequeno: Path, tmp_path: Path
) -> None:
    """allow_full_load deve ser passado apenas como keyword argument."""
    service = ParquetService(root=tmp_path)
    # Deve funcionar com keyword
    df = service.load_dataset(parquet_pequeno, allow_full_load=False)
    assert isinstance(df, pl.DataFrame)


# ---------------------------------------------------------------------------
# Testes: _carregar_dataset_ui — retorna DataFrame vazio e nao propaga excecao
# ---------------------------------------------------------------------------


def _criar_mixin_fake(parquet_service: ParquetService) -> Any:
    """Cria uma instancia minima do mixin com parquet_service mockado."""
    from interface_grafica.windows.main_window_loading import MainWindowLoadingMixin

    class FakeWindow(MainWindowLoadingMixin):
        def __init__(self):
            self.parquet_service = parquet_service
            self.status = MagicMock()

        def show_error(self, *args: Any, **kwargs: Any) -> None:
            pass

    return FakeWindow()


def test_carregar_dataset_ui_retorna_vazio_para_parquet_grande(
    parquet_grande_simulado: Path, tmp_path: Path
) -> None:
    """_carregar_dataset_ui deve retornar DataFrame vazio e exibir mensagem de status."""
    service = ParquetService(root=tmp_path)
    win = _criar_mixin_fake(service)
    result = win._carregar_dataset_ui(parquet_grande_simulado)
    assert isinstance(result, pl.DataFrame)
    assert result.is_empty()
    win.status.showMessage.assert_called_once()
    msg = win.status.showMessage.call_args[0][0]
    assert "grande" in msg.lower() or "MB" in msg


def test_carregar_dataset_ui_funciona_para_parquet_pequeno(
    parquet_pequeno: Path, tmp_path: Path
) -> None:
    """_carregar_dataset_ui deve retornar dados para arquivo pequeno sem erros."""
    service = ParquetService(root=tmp_path)
    win = _criar_mixin_fake(service)
    result = win._carregar_dataset_ui(parquet_pequeno)
    assert isinstance(result, pl.DataFrame)
    assert result.height == 100


# ---------------------------------------------------------------------------
# Testes: _carregar_dados_parquet_async — nao chama callback para arquivo grande
# ---------------------------------------------------------------------------


def test_carregar_dados_parquet_async_nao_chama_callback_para_grande(
    parquet_grande_simulado: Path, tmp_path: Path
) -> None:
    """_carregar_dados_parquet_async deve retornar sem chamar callback para arquivo grande."""
    service = ParquetService(root=tmp_path)
    win = _criar_mixin_fake(service)
    callback = MagicMock()
    win._carregar_dados_parquet_async(parquet_grande_simulado, callback)
    callback.assert_not_called()
    win.status.showMessage.assert_called_once()


def test_carregar_dados_parquet_async_exibe_mensagem_para_grande(
    parquet_grande_simulado: Path, tmp_path: Path
) -> None:
    """_carregar_dados_parquet_async deve exibir mensagem de arquivo grande."""
    service = ParquetService(root=tmp_path)
    win = _criar_mixin_fake(service)
    win._carregar_dados_parquet_async(parquet_grande_simulado, MagicMock())
    msg = win.status.showMessage.call_args[0][0]
    assert "grande" in msg.lower() or "MB" in msg
