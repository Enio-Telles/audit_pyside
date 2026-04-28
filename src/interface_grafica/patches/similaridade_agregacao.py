from __future__ import annotations

from PySide6.QtWidgets import QCheckBox, QPushButton


def apply_similarity_patch() -> None:
    """Aplica patch incremental para ordenacao por similaridade na aba Agregacao.

    O patch evita alterar a construcao grande da janela, mas instala controles e
    metodo de controller de forma idempotente. A acao apenas reordena a tabela e
    adiciona indicadores; nao executa agrupamento automatico.
    """
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
                "Quando marcado, NCM e CEST entram na chave de aproximacao dos blocos. "
                "GTIN continua sendo considerado no score quando existir."
            )

            # Insere logo apos o botao Reprocessar no mesmo layout horizontal.
            parent = self.btn_reprocessar_agregacao.parentWidget()
            layout = parent.layout() if parent is not None else None
            inserted = False
            if layout is not None:
                for idx in range(layout.count()):
                    item = layout.itemAt(idx)
                    if item is not None and item.widget() is self.btn_reprocessar_agregacao:
                        layout.insertWidget(idx + 1, self.btn_ordenar_similaridade_desc)
                        layout.insertWidget(idx + 2, self.chk_similarity_ncm_cest)
                        inserted = True
                        break
            if not inserted and layout is not None:
                layout.addWidget(self.btn_ordenar_similaridade_desc)
                layout.addWidget(self.chk_similarity_ncm_cest)

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
