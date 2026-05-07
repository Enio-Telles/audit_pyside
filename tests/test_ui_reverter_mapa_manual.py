import os
import sys
from pathlib import Path

import pytest
pytest.importorskip("PySide6")

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

pytestmark = pytest.mark.gui

# Skip on Windows CI due to DLL instability (0xc0000139 STATUS_ENTRYPOINT_NOT_FOUND).
if sys.platform == "win32" and os.getenv("GITHUB_ACTIONS"):
    pytest.skip("Skipping GUI tests on Windows CI due to DLL instability", allow_module_level=True)

from PySide6.QtWidgets import QApplication, QMessageBox

from interface_grafica.ui.main_window import MainWindow


def ensure_qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_click_reverter_mapa_manual_calls_service(monkeypatch, tmp_path):
    """Verifica que clicar no botão chama ServicoAgregacao.reverter_mapa_manual."""
    ensure_qapp()

    # Instancia janela
    window = MainWindow()
    # Define CNPJ selecionado
    window.state.current_cnpj = "12345678901234"

    # Prepara lista de snapshots retornada pelo serviço
    snapshot_path = (
        tmp_path / "snapshots" / "mapa_agrupamento_manual_123_20260420.parquet"
    )
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text("stub")
    snapshots = [str(snapshot_path)]

    # Monkeypatch no serviço para listar snapshots
    monkeypatch.setattr(
        window.servico_agregacao, "listar_snapshots_mapa_manual", lambda cnpj: snapshots
    )

    # Monkeypatch no diálogo para selecionar o item (retorna nome do arquivo e True)
    monkeypatch.setattr(
        "interface_grafica.controllers.agregacao_controller.QInputDialog.getItem",
        lambda *args, **kwargs: (Path(snapshots[0]).name, True),
    )

    # Força confirmação positiva
    monkeypatch.setattr(
        "interface_grafica.controllers.agregacao_controller.QMessageBox.question",
        lambda *args, **kwargs: QMessageBox.StandardButton.Yes,
    )
    monkeypatch.setattr(
        "interface_grafica.controllers.agregacao_controller.QMessageBox.information",
        lambda *args, **kwargs: QMessageBox.StandardButton.Ok,
    )

    called = {}

    def fake_reverter_mapa_manual(cnpj, snapshot_name=None):
        called["cnpj"] = cnpj
        called["snapshot_name"] = snapshot_name
        return True

    monkeypatch.setattr(
        window.servico_agregacao, "reverter_mapa_manual", fake_reverter_mapa_manual
    )

    # Evita execução de threads; simula execução sincrona do reprocessamento
    def fake_exec(
        func, *args, mensagem_inicial=None, on_success=None, on_failure=None, **kwargs
    ):
        if on_success:
            on_success(True)
        return True

    monkeypatch.setattr(window, "_executar_em_worker", fake_exec)

    # Simula clique do usuário
    window.btn_reverter_mapa_manual.click()

    assert called, "ServicoAgregacao.reverter_mapa_manual nao foi chamado"
    assert called["cnpj"] == "12345678901234"
    assert called["snapshot_name"] == Path(snapshots[0]).name
    window.close()
