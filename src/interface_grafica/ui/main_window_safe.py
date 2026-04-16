from __future__ import annotations

from typing import Iterable

from PySide6.QtCore import QThread, QTimer

from interface_grafica.ui.main_window import MainWindow as BaseMainWindow


class SafeMainWindow(BaseMainWindow):
    _WORKER_ATTRS = (
        "pipeline_worker",
        "query_worker",
        "service_worker",
        "_oracle_test_worker_1",
        "_oracle_test_worker_2",
        "_oracle_verify_worker_1",
        "_oracle_verify_worker_2",
    )

    def __init__(self) -> None:
        super().__init__()
        self._workers_monitorados_no_fechamento: set[int] = set()

    def _iter_workers_registrados(self) -> list[QThread]:
        encontrados: list[QThread] = []
        vistos: set[int] = set()

        def _append(worker: object) -> None:
            if not isinstance(worker, QThread):
                return
            ident = id(worker)
            if ident in vistos:
                return
            vistos.add(ident)
            encontrados.append(worker)

        for attr_name in self._WORKER_ATTRS:
            _append(getattr(self, attr_name, None))

        active_load_workers = getattr(self, "_active_load_workers", None)
        if isinstance(active_load_workers, set):
            for worker in list(active_load_workers):
                _append(worker)

        return encontrados

    def _workers_ativos(self) -> list[QThread]:
        return [worker for worker in self._iter_workers_registrados() if worker.isRunning()]

    def _solicitar_interrupcao_worker(self, worker: QThread) -> None:
        cancelar = getattr(worker, "cancelar", None)
        if callable(cancelar):
            try:
                cancelar()
            except Exception:
                pass

        request_interruption = getattr(worker, "requestInterruption", None)
        if callable(request_interruption):
            try:
                request_interruption()
            except Exception:
                pass

    def _solicitar_interrupcao_workers_ativos(self, workers: Iterable[QThread]) -> None:
        for worker in workers:
            self._solicitar_interrupcao_worker(worker)

    def _tentar_fechar_apos_workers(self) -> None:
        if self._workers_ativos():
            return
        self._closing_after_workers = False
        self._workers_monitorados_no_fechamento.clear()
        self.setEnabled(True)
        QTimer.singleShot(0, self.close)

    def closeEvent(self, event) -> None:
        ativos = self._workers_ativos()
        if not ativos:
            self._workers_monitorados_no_fechamento.clear()
            super().closeEvent(event)
            return

        if not self._closing_after_workers:
            self._closing_after_workers = True
            self.status.showMessage("Encerrando aplicação: interrompendo operações em execução...")
            self.setEnabled(False)
            self._solicitar_interrupcao_workers_ativos(ativos)
            for worker in ativos:
                ident = id(worker)
                if ident in self._workers_monitorados_no_fechamento:
                    continue
                self._workers_monitorados_no_fechamento.add(ident)
                worker.finished.connect(self._tentar_fechar_apos_workers)
            QTimer.singleShot(150, self._tentar_fechar_apos_workers)

        event.ignore()
