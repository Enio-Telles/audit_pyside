from __future__ import annotations

from PySide6.QtWidgets import QCheckBox, QHBoxLayout, QPushButton

from interface_grafica.services.descricao_similarity_service import (
    ordenar_blocos_similaridade_descricao,
)
from interface_grafica.utils.safe_slot import safe_slot


class SimilaridadeDescricaoControllerMixin:
    """Adiciona ordenacao visual por similaridade na aba Agregacao.

    Este mixin nao executa agrupamento, nao salva Parquet e nao chama o servico
    de agregacao. Ele apenas reorganiza o DataFrame exibido na tabela superior.
    """

    def _install_similarity_controls(self) -> None:
        if hasattr(self, "btn_ordenar_similaridade_desc"):
            return
        if not hasattr(self, "btn_reprocessar_agregacao"):
            return

        self.btn_ordenar_similaridade_desc = QPushButton("Ordenar por similaridade")
        self.btn_ordenar_similaridade_desc.setToolTip(
            "Ordena a tabela por blocos de descricoes similares sem agrupar automaticamente."
        )
        self.chk_similarity_ncm_cest = QCheckBox("Separar por NCM/CEST")
        self.chk_similarity_ncm_cest.setChecked(True)
        self.chk_similarity_ncm_cest.setToolTip(
            "Quando marcado, compara descricoes dentro do mesmo NCM/CEST quando essas colunas existirem."
        )

        parent = self.btn_reprocessar_agregacao.parentWidget()
        layout = parent.layout() if parent is not None else None
        if layout is None:
            return

        toolbar_layout = None
        for idx in range(layout.count()):
            item = layout.itemAt(idx)
            child_layout = item.layout() if item is not None else None
            if isinstance(child_layout, QHBoxLayout):
                toolbar_layout = child_layout
                break

        if toolbar_layout is None:
            return

        toolbar_layout.insertWidget(3, self.btn_ordenar_similaridade_desc)
        toolbar_layout.insertWidget(4, self.chk_similarity_ncm_cest)
        self.btn_ordenar_similaridade_desc.clicked.connect(
            self.ordenar_agregacao_por_similaridade
        )

    @safe_slot
    def ordenar_agregacao_por_similaridade(self) -> None:
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
        except Exception as exc:
            self.show_error("Erro ao ordenar por similaridade", str(exc))
            return

        self.aggregation_table_model.set_dataframe(df_ordenado)
        self._resize_table_once(self.aggregation_table_view, "agregacao_top")
        self.status.showMessage(
            "Tabela ordenada por blocos de similaridade. Nenhum agrupamento foi executado."
        )
