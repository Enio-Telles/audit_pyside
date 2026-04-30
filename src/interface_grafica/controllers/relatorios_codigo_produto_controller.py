from __future__ import annotations

import polars as pl
from PySide6.QtWidgets import QMessageBox

from interface_grafica.config import CNPJ_ROOT


ESTOQUE_CODIGO_TABELAS = {
    "Resumo Global": "aba_resumo_global_codigo_produto_{cnpj}.parquet",
    "Mensal": "aba_mensal_codigo_produto_{cnpj}.parquet",
    "Anual": "aba_anual_codigo_produto_{cnpj}.parquet",
    "Periodos": "aba_periodos_codigo_produto_{cnpj}.parquet",
    "Mov. Estoque": "mov_estoque_codigo_produto_{cnpj}.parquet",
}


class RelatoriosCodigoProdutoControllerMixin:
    def _nome_tabela_estoque_codigo(self) -> str:
        combo = getattr(self, "cmb_estoque_codigo_tabela", None)
        if combo is None:
            return "Resumo Global"
        texto = combo.currentText()
        return texto if texto in ESTOQUE_CODIGO_TABELAS else "Resumo Global"

    def _path_tabela_estoque_codigo(self, cnpj: str, tabela: str):
        nome = ESTOQUE_CODIGO_TABELAS[tabela].format(cnpj=cnpj)
        return CNPJ_ROOT / cnpj / "analises" / "produtos" / nome

    def atualizar_aba_estoque_codigo_produto(self) -> None:
        cnpj = self.state.current_cnpj
        if not cnpj:
            self._estoque_codigo_df = pl.DataFrame()
            self.estoque_codigo_model.set_dataframe(pl.DataFrame())
            self.lbl_estoque_codigo_status.setText("Selecione um CNPJ para carregar a aba.")
            return

        tabela = self._nome_tabela_estoque_codigo()
        path = self._path_tabela_estoque_codigo(cnpj, tabela)
        if not path.exists():
            self._estoque_codigo_df = pl.DataFrame()
            self.estoque_codigo_model.set_dataframe(pl.DataFrame())
            self.lbl_estoque_codigo_status.setText(
                "Artefato por codigo ainda nao gerado. Use 'Gerar por codigo'."
            )
            return

        self.lbl_estoque_codigo_status.setText(f"Carregando {tabela} por codigo...")

        def _finalizar(df: pl.DataFrame | None, uniques: dict | None = None) -> None:
            if df is None:
                self._estoque_codigo_df = pl.DataFrame()
                self.estoque_codigo_model.set_dataframe(pl.DataFrame())
                self.lbl_estoque_codigo_status.setText(
                    f"Nao foi possivel carregar {tabela} por codigo."
                )
                return
            self._estoque_codigo_df = df
            self.estoque_codigo_model.set_dataframe(df)
            self._reset_table_resize_flag("estoque_codigo_produto")
            self._resize_table_once(self.estoque_codigo_table, "estoque_codigo_produto")
            self.lbl_estoque_codigo_status.setText(
                f"{tabela} por codigo carregado: {df.height} linhas."
            )

        self._carregar_dados_parquet_async(path, _finalizar, f"Carregando {tabela} por codigo")

    def calcular_estoque_codigo_produto(self, recalcular_base: bool = False) -> None:
        cnpj = self._obter_cnpj_valido()
        if not cnpj:
            return

        self.lbl_estoque_codigo_status.setText(
            "Recalculando base e gerando artefatos por codigo..."
            if recalcular_base
            else "Gerando artefatos por codigo a partir do mov_estoque atual..."
        )

        def _ok(resultado) -> None:
            if resultado:
                self.lbl_estoque_codigo_status.setText(
                    "Artefatos por codigo gerados com sucesso."
                )
                self.atualizar_aba_estoque_codigo_produto()
            else:
                self.lbl_estoque_codigo_status.setText(
                    "Nao foi possivel gerar os artefatos por codigo. Verifique os logs."
                )

        def _erro(mensagem: str) -> None:
            self.lbl_estoque_codigo_status.setText(
                "Erro ao gerar artefatos por codigo. Verifique os logs."
            )
            self.show_error("Erro no estoque por codigo", mensagem)

        self._executar_em_worker(
            self.servico_agregacao.calcular_estoque_codigo_produto,
            cnpj,
            mensagem_inicial="Gerando estoque por codigo do produto...",
            on_success=_ok,
            on_failure=_erro,
            recalcular_base=recalcular_base,
        )

    def exportar_estoque_codigo_produto_excel(self) -> None:
        if self._estoque_codigo_df is None or self._estoque_codigo_df.is_empty():
            QMessageBox.information(self, "Exportacao", "Nao ha dados por codigo para exportar.")
            return
        tabela = self._nome_tabela_estoque_codigo()
        target = self._save_dialog(f"Exportar {tabela} por codigo", "Excel (*.xlsx)")
        if not target:
            return
        try:
            wb = self._criar_workbook_exportacao(tabela, self._estoque_codigo_df)
            target.parent.mkdir(parents=True, exist_ok=True)
            wb.save(target)
            self.show_info("Exportacao concluida", f"Arquivo gerado em:\n{target}")
        except Exception as e:
            self.show_error("Erro de exportacao", str(e))

    def _criar_workbook_exportacao(self, titulo: str, df: pl.DataFrame):
        from openpyxl import Workbook

        wb = Workbook()
        ws = wb.active
        ws.title = titulo[:31]
        self._escrever_planilha_openpyxl(ws, df)
        return wb
