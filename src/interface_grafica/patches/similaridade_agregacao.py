from __future__ import annotations

from PySide6.QtWidgets import QCheckBox, QPushButton


def apply_similarity_patch() -> None:
    """Aplica patch incremental para ordenacao por similaridade na aba Agregacao."""
    from interface_grafica.controllers.agregacao_controller import AgregacaoControllerMixin
    from interface_grafica.windows.aba_agregacao import AgregacaoWindowMixin

    if getattr(AgregacaoWindowMixin, "_similarity_patch_applied", False):
        return

    original_build_tab_agregacao = AgregacaoWindowMixin._build_tab_agregacao

    def _build_tab_agregacao_com_similaridade(self):
        tab = original_build_tab_agregacao(self)

        if not hasattr(self, "btn_ordenar_similaridade_desc"):
            self.btn_ordenar_similaridade_desc = QPushButton("Ordenar por similaridade")
            self.btn_ordenar_similaridade_desc.setToolTip(
                "Ordena a tabela por blocos de similaridade de descricao, NCM, CEST e GTIN. "
                "Nao executa agrupamento automatico."
            )
            self.chk_similarity_ncm_cest = QCheckBox("Priorizar NCM/CEST")
            self.chk_similarity_ncm_cest.setChecked(True)
            self.chk_similarity_ncm_cest.setToolTip(
                "Quando marcado, NCM e CEST aproximam os blocos antes da comparacao de descricao."
            )

            layout = None
            try:
                layout = self.btn_reprocessar_agregacao.parentWidget().layout()
            except Exception:
                layout = None
            if layout is not None:
                stretch_index = -1
                for i in range(layout.count()):
                    item = layout.itemAt(i)
                    if item is not None and item.spacerItem() is not None:
                        stretch_index = i
                        break
                insert_at = stretch_index if stretch_index >= 0 else layout.count()
                layout.insertWidget(insert_at, self.btn_ordenar_similaridade_desc)
                layout.insertWidget(insert_at + 1, self.chk_similarity_ncm_cest)

            self.btn_ordenar_similaridade_desc.clicked.connect(
                self.ordenar_agregacao_por_similaridade
            )

        return tab

    def ordenar_agregacao_por_similaridade(self) -> None:
        from interface_grafica.services.descricao_similarity_service import (
            ordenar_blocos_similaridade_descricao,
        )

        df = self.aggregation_table_model.get_dataframe()
        if df.is_empty():
            self.status.showMessage("Nenhuma linha para ordenar por similaridade.")
            return

        usar_ncm_cest = True
        if hasattr(self, "chk_similarity_ncm_cest"):
            usar_ncm_cest = self.chk_similarity_ncm_cest.isChecked()

        try:
            df_ordenado = ordenar_blocos_similaridade_descricao(
                df,
                janela=4,
                limite_bloco=82,
                usar_ncm_cest=usar_ncm_cest,
            )
            self.aggregation_table_model.set_dataframe(df_ordenado)
            self._resize_table_once(self.aggregation_table_view, "agregacao_top")
            self.status.showMessage(
                "Tabela ordenada por similaridade de descricao, NCM, CEST e GTIN. "
                "Nenhum agrupamento foi executado."
            )
        except Exception as exc:
            self.show_error("Erro ao ordenar por similaridade", str(exc))

    AgregacaoWindowMixin._build_tab_agregacao = _build_tab_agregacao_com_similaridade
    AgregacaoControllerMixin.ordenar_agregacao_por_similaridade = ordenar_agregacao_por_similaridade
    AgregacaoWindowMixin._similarity_patch_applied = True
