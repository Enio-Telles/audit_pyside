from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import polars as pl
from interface_grafica.config import LARGE_PARQUET_THRESHOLD_MB
from interface_grafica.services.parquet_service import (
    FilterCondition,
    LargeParquetForbiddenError,
    ParquetService,
)
from interface_grafica.services.query_worker import QueryWorker
from interface_grafica.controllers.workers import ServiceTaskWorker


class MainWindowLoadingMixin:
    def _marcar_recalculo_conversao_pendente(self, motivo: str | None = None) -> None:
        self._conversion_recalc_pending = True
        if hasattr(self, "btn_recalcular_fatores"):
            self.btn_recalcular_fatores.setEnabled(True)
        mensagem = "Alteracoes em fatores salvas. Recalculo pendente."
        if motivo:
            mensagem += f" {motivo}"
        self.status.showMessage(mensagem)
    def _limpar_recalculo_conversao_pendente(self) -> None:
        self._conversion_recalc_pending = False
        if hasattr(self, "btn_recalcular_fatores"):
            self.btn_recalcular_fatores.setEnabled(False)
    def _on_main_tab_changed(self, current_index: int) -> None:
        if not hasattr(self, "tab_conversao"):
            return

        # Lazy Loading: Ao trocar de aba, carrega os dados referentes ao CNPJ atual
        self._carregar_aba_atual()

        idx_conversao = self.tabs.indexOf(self.tab_conversao)
        if idx_conversao < 0:
            return
        if current_index != idx_conversao and self._conversion_recalc_pending:
            self.recalcular_derivados_conversao(show_popup=False)
    def _carregar_dataset_ui(
        self,
        path: Path,
        conditions: list[FilterCondition] | None = None,
        columns: list[str] | None = None,
    ) -> pl.DataFrame:
        colunas_solicitadas = columns
        if columns is not None:
            schema = set(self.parquet_service.get_schema(path))
            colunas_solicitadas = [coluna for coluna in columns if coluna in schema]
            if not colunas_solicitadas:
                return pl.DataFrame()
        try:
            return self.parquet_service.load_dataset(
                path, conditions or [], colunas_solicitadas
            )
        except LargeParquetForbiddenError as exc:
            if hasattr(self, "status"):
                self.status.showMessage(
                    f"Arquivo grande ({exc.size_mb:.0f} MB): use filtros ou paginacao.",
                    12000,
                )
            return pl.DataFrame()
    def _carregar_dados_parquet_async(
        self,
        path: Path,
        callback: Callable,
        status_msg: str = "",
        unique_cols: list[str] | None = None,
    ) -> None:
        """
        Carrega um arquivo Parquet em background.
        Se unique_cols for fornecido, extrai valores unicos dessas colunas no background.
        O callback sera chamado como callback(df) ou callback(df, uniques_dict).
        Arquivos acima de LARGE_PARQUET_THRESHOLD_MB sao bloqueados (exibem mensagem e retornam).
        """
        if ParquetService.is_large_parquet(path):
            try:
                size_mb = path.stat().st_size / (1024 * 1024)
            except OSError:
                size_mb = float(LARGE_PARQUET_THRESHOLD_MB) + 1
            if hasattr(self, "status"):
                self.status.showMessage(
                    f"Arquivo grande ({size_mb:.0f} MB): use filtros ou paginacao.",
                    12000,
                )
            return
        if status_msg:
            self.status.showMessage(f"Carregando {status_msg}...")

        parquet_service = self.parquet_service

        def _worker_load():
            if not path.exists():
                return None
            df = parquet_service.load_dataset(path, allow_full_load=False)
            if not unique_cols:
                return df

            uniques = {}
            for col in unique_cols:
                if col in df.columns:
                    # Extração pesada feita no worker (background thread)
                    uniques[col] = (
                        df.get_column(col)
                        .cast(pl.Utf8, strict=False)
                        .drop_nulls()
                        .unique()
                        .sort()
                        .to_list()
                    )
            return {"df": df, "uniques": uniques}

        worker = ServiceTaskWorker(_worker_load)

        def _on_success(result):
            if status_msg:
                self.status.showMessage(
                    f"✔ {status_msg.replace('Carregando', 'Feito')}", 3000
                )

            if isinstance(result, dict) and "df" in result:
                callback(result["df"], result.get("uniques"))
            else:
                callback(result)

        def _on_failed(err: str):
            self.show_error(
                "Erro de Carregamento", f"Falha ao carregar {path.name}: {err}"
            )

        worker.finished_ok.connect(_on_success)
        worker.failed.connect(_on_failed)

        if not hasattr(self, "_active_load_workers") or not isinstance(
            self._active_load_workers, set
        ):
            self._active_load_workers = set()

        self._active_load_workers.add(worker)
        worker.finished.connect(lambda: self._active_load_workers.discard(worker))
        worker.finished.connect(worker.deleteLater)

        worker.start()
    def _limpar_aba_resumo_estoque(self, contexto: str, mensagem: str) -> None:
        if contexto == "aba_mensal":
            self.aba_mensal_model.set_dataframe(pl.DataFrame())
            self._aba_mensal_df = pl.DataFrame()
            self.lbl_aba_mensal_status.setText(mensagem)
            self.lbl_aba_mensal_filtros.setText("Filtros ativos: nenhum")
            self._atualizar_titulo_aba_mensal()
            return
        if contexto == "aba_anual":
            self.aba_anual_model.set_dataframe(pl.DataFrame())
            self._aba_anual_df = pl.DataFrame()
            self.lbl_aba_anual_status.setText(mensagem)
            self.lbl_aba_anual_filtros.setText("Filtros ativos: nenhum")
            self._atualizar_titulo_aba_anual()
    def _garantir_resumos_estoque_atualizados(self, cnpj: str) -> bool:
        artefatos_defasados = self.servico_agregacao.artefatos_estoque_defasados(cnpj)
        if not artefatos_defasados:
            return True

        if self._sync_resumos_estoque_cnpj == cnpj:
            return False

        if self.service_worker is not None and self.service_worker.isRunning():
            self.status.showMessage(
                "Aguardando o processamento atual para sincronizar as tabelas mensal/anual."
            )
            return False

        self._sync_resumos_estoque_cnpj = cnpj
        nomes = {
            "calculos_mensais": "mensal",
            "calculos_anuais": "anual",
        }
        descricoes = ", ".join(nomes.get(item, item) for item in artefatos_defasados)

        def _on_success(ok) -> None:
            self._sync_resumos_estoque_cnpj = None
            if ok:
                self.refresh_file_tree(cnpj)
                self.atualizar_aba_mensal()
                self.atualizar_aba_anual()
                self.atualizar_aba_produtos_selecionados()
                self.atualizar_aba_resumo_global()
                self.status.showMessage(
                    f"Tabelas {descricoes} sincronizadas com a mov_estoque."
                )
            else:
                self.status.showMessage(
                    "Falha ao sincronizar as tabelas mensal/anual com a mov_estoque."
                )
                self.show_error(
                    "Falha na sincronizacao",
                    "Nao foi possivel atualizar as tabelas mensal/anual.",
                )

        def _on_failure(mensagem: str) -> None:
            self._sync_resumos_estoque_cnpj = None
            self.status.showMessage("Erro ao sincronizar as tabelas mensal/anual.")
            self.show_error("Falha na sincronizacao", mensagem)

        iniciado = self._executar_em_worker(
            self.servico_agregacao.recalcular_resumos_estoque,
            cnpj,
            mensagem_inicial=f"Sincronizando tabelas {descricoes} com a mov_estoque...",
            on_success=_on_success,
            on_failure=_on_failure,
        )
        if not iniciado:
            self._sync_resumos_estoque_cnpj = None
            return False
        return False
