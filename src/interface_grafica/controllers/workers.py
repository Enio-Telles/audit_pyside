from __future__ import annotations

import inspect
from pathlib import Path
from typing import Callable

from PySide6.QtCore import QThread, Signal

from interface_grafica.services.pipeline_funcoes_service import ServicoPipelineCompleto


class PipelineWorker(QThread):
    finished_ok = Signal(object)
    failed = Signal(str)
    progress = Signal(str)

    def __init__(
        self,
        service: ServicoPipelineCompleto,
        cnpj: str,
        consultas: list[str | Path],
        tabelas: list[str],
        data_limite: str | None = None,
    ) -> None:
        super().__init__()
        self.service = service
        self.cnpj = cnpj
        self.consultas = consultas
        self.tabelas = tabelas
        self.data_limite = data_limite

    def run(self) -> None:
        try:
            result = self.service.executar_completo(
                self.cnpj,
                self.consultas,
                self.tabelas,
                self.data_limite,
                progresso=self.progress.emit,
            )
        except Exception as exc:  # pragma: no cover - UI
            from utilitarios.perf_monitor import registrar_evento_performance

            registrar_evento_performance(
                "pipeline_worker.erro",
                contexto={"cnpj": self.cnpj, "erro": str(exc)},
                status="error",
            )
            self.failed.emit("Ocorreu um erro no pipeline. Verifique os logs internos.")
            return

        if result.ok:
            self.finished_ok.emit(result)
        else:
            message = "\n".join(result.erros) if result.erros else "Falha nao pipeline."
            self.failed.emit(message or "Falha sem detalhes.")


class ServiceTaskWorker(QThread):
    finished_ok = Signal(object)
    failed = Signal(str)
    progress = Signal(str)

    def __init__(self, func: Callable, *args, **kwargs) -> None:
        super().__init__()
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def run(self) -> None:
        try:
            call_kwargs = dict(self.kwargs)
            try:
                assinatura = inspect.signature(self.func)
                if (
                    "progresso" in assinatura.parameters
                    and "progresso" not in call_kwargs
                ):
                    call_kwargs["progresso"] = self.progress.emit
            except Exception:
                pass
            resultado = self.func(*self.args, **call_kwargs)
        except Exception as exc:
            from utilitarios.perf_monitor import registrar_evento_performance

            registrar_evento_performance(
                "service_task_worker.erro",
                contexto={
                    "func": getattr(self.func, "__name__", str(self.func)),
                    "erro": str(exc),
                },
                status="error",
            )
            self.failed.emit(
                "Ocorreu um erro no processamento. Verifique os logs internos."
            )
            return
        self.finished_ok.emit(resultado)
