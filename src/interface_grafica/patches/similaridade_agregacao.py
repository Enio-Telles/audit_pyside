from __future__ import annotations

from PySide6.QtWidgets import QCheckBox, QComboBox, QLabel, QPushButton


def apply_similarity_patch() -> None:
    """Aplica patch incremental para ordenacao por similaridade na aba Agregacao.

    O patch evita alterar a construcao grande da janela, mas instala controles,
    conexao de sinal e metodo de controller de forma idempotente. A acao apenas
    reordena a tabela e adiciona indicadores; nao executa agrupamento automatico.
    """
    from interface_grafica.controllers.agregacao_controller import AgregacaoControllerMixin
    from interface_grafica.windows.aba_agregacao import AgregacaoWindowMixin
    from interface_grafica.windows.main_window_signal_wiring_core import (
        MainWindowSignalWiringCoreMixin,
    )

    if getattr(AgregacaoWindowMixin, "_similarity_patch_applied", False):
        return

    original_build_tab_agregacao = AgregacaoWindowMixin._build_tab_agregacao
    original_connect_consulta_agregacao = (
        MainWindowSignalWiringCoreMixin._connect_consulta_agregacao_signals
    )

    def _build_tab_agregacao_com_similaridade(self):
        tab = original_build_tab_agregacao(self)

        if not hasattr(self, "btn_ordenar_similaridade_desc"):
            self.btn_ordenar_similaridade_desc = QPushButton("Ordenar por similaridade")
            self.btn_ordenar_similaridade_desc.setToolTip(
                "Ordena a tabela em blocos visuais. Nao executa agrupamento."
            )

            self.cmb_metodo_similaridade = QComboBox()
            self.cmb_metodo_similaridade.addItems([
                "Multiatributo (desc+NCM+CEST+GTIN+codigo)",
                "Composto legacy",
                "Particionamento fiscal",
                "Apenas descricao",
            ])
            self.cmb_metodo_similaridade.setToolTip(
                "Multiatributo: descricao, NCM, CEST, GTIN e cod_item/prod_cprod.\n"
                "Composto legacy: metodo classico baseado em score ponderado.\n"
                "Particionamento: agrupa por GTIN/NCM/CEST/UNIDADE primeiro.\n"
                "Apenas descricao: ignora identificadores, so texto."
            )

            self.chk_incluir_desc_sem_ncm = QCheckBox(
                "Incluir descricao em itens sem NCM"
            )
            self.chk_incluir_desc_sem_ncm.setVisible(False)

            self.lbl_aviso_similaridade = QLabel("")
            self.lbl_aviso_similaridade.setStyleSheet("color: #c47900;")
            self.lbl_aviso_similaridade.setVisible(False)

            def _on_metodo_changed(idx: int) -> None:
                is_particionamento = idx == 2
                is_so_descricao = idx == 3
                self.chk_incluir_desc_sem_ncm.setVisible(is_particionamento)
                if is_so_descricao:
                    self.lbl_aviso_similaridade.setText(
                        "Identificadores fiscais ignorados. Revise manualmente."
                    )
                    self.lbl_aviso_similaridade.setVisible(True)
                else:
                    self.lbl_aviso_similaridade.setVisible(False)

            self.cmb_metodo_similaridade.currentIndexChanged.connect(_on_metodo_changed)

            parent = self.btn_reprocessar_agregacao.parentWidget()
            layout = parent.layout() if parent is not None else None
            inserted = False
            if layout is not None:
                for idx in range(layout.count()):
                    item = layout.itemAt(idx)
                    if item is not None and item.widget() is self.btn_reprocessar_agregacao:
                        layout.insertWidget(idx + 1, self.btn_ordenar_similaridade_desc)
                        layout.insertWidget(idx + 2, self.cmb_metodo_similaridade)
                        layout.insertWidget(idx + 3, self.chk_incluir_desc_sem_ncm)
                        layout.insertWidget(idx + 4, self.lbl_aviso_similaridade)
                        inserted = True
                        break
            if not inserted and layout is not None:
                layout.addWidget(self.btn_ordenar_similaridade_desc)
                layout.addWidget(self.cmb_metodo_similaridade)
                layout.addWidget(self.chk_incluir_desc_sem_ncm)
                layout.addWidget(self.lbl_aviso_similaridade)

        return tab

    def ordenar_agregacao_por_similaridade(self) -> None:
        df = self.aggregation_table_model.get_dataframe()
        if df.is_empty():
            self.status.showMessage("Nenhuma linha para ordenar por similaridade.")
            return

        metodo_idx = 0
        if hasattr(self, "cmb_metodo_similaridade"):
            metodo_idx = self.cmb_metodo_similaridade.currentIndex()

        try:
            if metodo_idx == 0:
                from interface_grafica.services.similaridade_multiatributo_service import (
                    ordenar_blocos_similaridade_multiatributo,
                )
                df_ordenado = ordenar_blocos_similaridade_multiatributo(
                    df, janela=4, limite_bloco=82, usar_ncm_cest=True,
                )
                mensagem = (
                    "Tabela ordenada por similaridade multiatributo "
                    "(descricao, NCM, CEST, GTIN e codigo fiscal). "
                    "Nenhum agrupamento foi executado."
                )
            elif metodo_idx == 1:
                from interface_grafica.services.descricao_similarity_service import (
                    ordenar_blocos_similaridade_descricao,
                )
                df_ordenado = ordenar_blocos_similaridade_descricao(
                    df, janela=4, limite_bloco=82, usar_ncm_cest=True,
                )
                mensagem = (
                    "Tabela ordenada (metodo composto legacy). "
                    "Nenhum agrupamento foi executado."
                )
            elif metodo_idx == 2:
                from interface_grafica.services.particionamento_fiscal import (
                    ordenar_blocos_por_particionamento_fiscal,
                )
                incluir_desc = (
                    self.chk_incluir_desc_sem_ncm.isChecked()
                    if hasattr(self, "chk_incluir_desc_sem_ncm") else False
                )
                df_ordenado = ordenar_blocos_por_particionamento_fiscal(
                    df, incluir_camada_so_descricao=incluir_desc,
                )
                mensagem = (
                    f"Tabela ordenada (particionamento fiscal). "
                    f"{'Camada de descricao ATIVA. ' if incluir_desc else ''}"
                    "Nenhum agrupamento foi executado."
                )
            else:
                from interface_grafica.services.inverted_index_descricao import (
                    ordenar_blocos_apenas_por_descricao,
                )
                df_ordenado = ordenar_blocos_apenas_por_descricao(df, threshold=0.5)
                mensagem = (
                    "Tabela ordenada (apenas descricao). "
                    "Identificadores fiscais ignorados. "
                    "Revise os agrupamentos manualmente."
                )

            self.aggregation_table_model.set_dataframe(df_ordenado)
            self._resize_table_once(self.aggregation_table_view, "agregacao_top")
            self.status.showMessage(mensagem)
        except Exception as exc:
            self.show_error("Erro ao ordenar por similaridade", str(exc))

    def _connect_consulta_agregacao_signals_com_similaridade(self) -> None:
        original_connect_consulta_agregacao(self)
        if hasattr(self, "btn_ordenar_similaridade_desc"):
            try:
                self.btn_ordenar_similaridade_desc.clicked.disconnect(
                    self.ordenar_agregacao_por_similaridade
                )
            except Exception:
                pass
            self.btn_ordenar_similaridade_desc.clicked.connect(
                self.ordenar_agregacao_por_similaridade
            )

    AgregacaoWindowMixin._build_tab_agregacao = _build_tab_agregacao_com_similaridade
    AgregacaoControllerMixin.ordenar_agregacao_por_similaridade = ordenar_agregacao_por_similaridade
    MainWindowSignalWiringCoreMixin._connect_consulta_agregacao_signals = (
        _connect_consulta_agregacao_signals_com_similaridade
    )
    AgregacaoWindowMixin._similarity_patch_applied = True
