from __future__ import annotations

import base64

import polars as pl
from interface_grafica.models.table_model import PolarsTableModel
from interface_grafica.services.profile_utils import ordenar_colunas_visiveis
from interface_grafica.ui.dialogs import ColumnSelectorDialog
from PySide6.QtCore import QByteArray, Qt
from PySide6.QtWidgets import QComboBox, QInputDialog, QTableView


class MainWindowPreferencesMixin:
    def _preferencia_tabela_key(self, aba: str, scope: str | None = None) -> str:
        escopo = scope or (self.state.current_cnpj or "__global__")
        return f"preferencias_tabela::{aba}::{escopo}"
    def _consulta_scope(self) -> str:
        arquivo = (
            self.state.current_file.name
            if self.state.current_file
            else "__sem_arquivo__"
        )
        cnpj = self.state.current_cnpj or "__global__"
        return f"{cnpj}::{arquivo}"
    def _carregar_preferencias_tabela(self, aba: str, scope: str | None = None) -> dict:
        prefs = self.selection_service.get_value(
            self._preferencia_tabela_key(aba, scope), {}
        )
        return prefs if isinstance(prefs, dict) else {}
    def _capturar_estado_tabela(
        self, table: QTableView, model: PolarsTableModel
    ) -> dict:
        offset = 1 if getattr(model, "_checkable", False) else 0
        colunas = model.dataframe.columns
        header = table.horizontalHeader()
        visiveis = [
            nome
            for _visual, nome in sorted(
                (
                    (header.visualIndex(idx + offset), nome)
                    for idx, nome in enumerate(colunas)
                    if not table.isColumnHidden(idx + offset)
                ),
                key=lambda item: item[0],
            )
        ]
        estado = {
            "visible_columns": visiveis,
            "column_order": visiveis,
            "header_state": self._serializar_estado_header(table),
        }
        if getattr(model, "_last_sort_column", None):
            estado["sort_column"] = model._last_sort_column
            estado["sort_order"] = (
                "desc" if model._last_sort_order == Qt.DescendingOrder else "asc"
            )
        return estado
    def _aplicar_estado_tabela(
        self, table: QTableView, model: PolarsTableModel, prefs: dict
    ) -> bool:
        if not prefs or model.dataframe.is_empty():
            return False

        aplicado = False
        visiveis = prefs.get("visible_columns")
        if isinstance(visiveis, list) and visiveis:
            self._aplicar_preset_colunas(
                table, model.dataframe.columns, [str(v) for v in visiveis]
            )
            aplicado = True

        sort_column = prefs.get("sort_column")
        sort_order = (
            Qt.DescendingOrder
            if prefs.get("sort_order") == "desc"
            else Qt.AscendingOrder
        )
        if isinstance(sort_column, str) and sort_column in model.dataframe.columns:
            idx = model.dataframe.columns.index(sort_column) + (
                1 if getattr(model, "_checkable", False) else 0
            )
            model.sort(idx, sort_order)
            table.sortByColumn(idx, sort_order)
            aplicado = True

        header_state = prefs.get("header_state")
        if isinstance(header_state, str) and header_state:
            aplicado = self._restaurar_estado_header(table, header_state) or aplicado
        return aplicado
    def _colunas_estado_perfil(
        self, prefs: dict, model: PolarsTableModel
    ) -> list[str] | None:
        if not isinstance(prefs, dict) or model.dataframe.is_empty():
            return None

        raw = prefs.get("visible_columns")
        if not isinstance(raw, list) or not raw:
            return None

        visiveis = ordenar_colunas_perfil(
            list(model.dataframe.columns),
            raw,
            (
                prefs.get("column_order")
                if isinstance(prefs.get("column_order"), list)
                else None
            ),
        )
        if not visiveis:
            return None

        header_state = prefs.get("header_state")
        if not isinstance(header_state, str) or not header_state:
            return visiveis

        probe = QTableView()
        try:
            probe.setModel(model)
            if not self._restaurar_estado_header(probe, header_state):
                return visiveis

            offset = 1 if getattr(model, "_checkable", False) else 0
            ordem = [
                nome
                for _visual, nome in sorted(
                    (
                        (probe.horizontalHeader().visualIndex(idx + offset), nome)
                        for idx, nome in enumerate(model.dataframe.columns)
                        if nome in visiveis
                    ),
                    key=lambda item: item[0],
                )
            ]
            return ordenar_colunas_perfil(
                list(model.dataframe.columns), visiveis, ordem
            )
        finally:
            probe.setModel(None)
    def _nomes_perfis_nomeados_tabela(
        self, aba: str, scope: str | None = None
    ) -> list[str]:
        prefs = self._carregar_preferencias_tabela(aba, scope)
        perfis = prefs.get("named_profiles", {})
        if not isinstance(perfis, dict):
            return []
        return sorted(
            [str(nome) for nome in perfis.keys() if str(nome).strip()],
            key=lambda v: v.lower(),
        )
    def _obter_estado_perfil_nomeado(
        self, aba: str, perfil: str, scope: str | None = None
    ) -> dict | None:
        prefs = self._carregar_preferencias_tabela(aba, scope)
        perfis = prefs.get("named_profiles", {})
        if not isinstance(perfis, dict):
            return None
        estado = perfis.get(perfil)
        return estado if isinstance(estado, dict) else None
    def _atualizar_combo_perfis_tabela(
        self,
        combo: QComboBox,
        aba: str,
        presets: list[str],
        scope: str | None = None,
    ) -> None:
        atual = combo.currentText().strip()
        nomes = presets + [
            n
            for n in self._nomes_perfis_nomeados_tabela(aba, scope)
            if n not in presets
        ]
        combo.blockSignals(True)
        combo.clear()
        combo.addItems(nomes)
        if atual and atual in nomes:
            combo.setCurrentText(atual)
        elif nomes:
            combo.setCurrentIndex(0)
        combo.blockSignals(False)
    def _salvar_perfil_nomeado_tabela(
        self,
        aba: str,
        table: QTableView,
        model: PolarsTableModel,
        nome: str,
        scope: str | None = None,
    ) -> None:
        if model.dataframe.is_empty():
            return
        nome_limpo = nome.strip()
        if not nome_limpo:
            return
        prefs = self._carregar_preferencias_tabela(aba, scope)
        perfis = prefs.get("named_profiles", {})
        if not isinstance(perfis, dict):
            perfis = {}
        perfis[nome_limpo] = self._capturar_estado_tabela(table, model)
        prefs["named_profiles"] = perfis
        self.selection_service.set_value(
            self._preferencia_tabela_key(aba, scope), prefs
        )
    def _serializar_estado_header(self, table: QTableView) -> str:
        estado = bytes(table.horizontalHeader().saveState())
        return base64.b64encode(estado).decode("ascii")
    def _restaurar_estado_header(self, table: QTableView, valor: str) -> bool:
        try:
            bruto = base64.b64decode(valor.encode("ascii"))
            return bool(table.horizontalHeader().restoreState(QByteArray(bruto)))
        except Exception:
            return False
    def _salvar_preferencias_tabela(
        self,
        aba: str,
        table: QTableView,
        model: PolarsTableModel,
        scope: str | None = None,
    ) -> None:
        if model.dataframe.is_empty():
            return
        prefs = self._carregar_preferencias_tabela(aba, scope)
        prefs.update(self._capturar_estado_tabela(table, model))
        self.selection_service.set_value(
            self._preferencia_tabela_key(aba, scope), prefs
        )
    def _aplicar_preferencias_tabela(
        self,
        aba: str,
        table: QTableView,
        model: PolarsTableModel,
        scope: str | None = None,
    ) -> bool:
        prefs = self._carregar_preferencias_tabela(aba, scope)
        return self._aplicar_estado_tabela(table, model, prefs)
    def _abrir_menu_colunas_tabela(
        self, aba: str, table: QTableView, pos=None, scope: str | None = None
    ) -> None:
        model = table.model()
        if not isinstance(model, PolarsTableModel) or model.dataframe.is_empty():
            return
        offset = 1 if getattr(model, "_checkable", False) else 0
        header = table.horizontalHeader()
        colunas = [
            nome
            for _visual, nome in sorted(
                (
                    (header.visualIndex(idx + offset), nome)
                    for idx, nome in enumerate(model.dataframe.columns)
                ),
                key=lambda item: item[0],
            )
        ]
        visiveis = [
            nome
            for nome in colunas
            if nome in model.dataframe.columns
            and not table.isColumnHidden(model.dataframe.columns.index(nome) + offset)
        ]
        dialog = ColumnSelectorDialog(colunas, visiveis, self)
        if not dialog.exec():
            return
        selecionadas = dialog.selected_columns()
        if not selecionadas:
            self.show_error(
                "Selecao invalida", "Pelo menos uma coluna deve permanecer visivel."
            )
            return
        self._aplicar_ordem_colunas(table, dialog.column_order())
        self._aplicar_preset_colunas(table, colunas, selecionadas)
        self._salvar_preferencias_tabela(aba, table, model, scope)
    def _aplicar_perfil_tabela(
        self,
        aba: str,
        table: QTableView,
        model: PolarsTableModel,
        perfil: str,
        contexto: str,
        scope: str | None = None,
    ) -> None:
        if model.dataframe.is_empty():
            return
        perfil_salvo = self._obter_estado_perfil_nomeado(aba, perfil, scope)
        if perfil_salvo is not None:
            self._aplicar_estado_tabela(table, model, perfil_salvo)
            self._salvar_preferencias_tabela(aba, table, model, scope)
            return
        visiveis = self._obter_colunas_preset_perfil(
            perfil, model.dataframe.columns, contexto
        )
        self._aplicar_preset_colunas(table, model.dataframe.columns, visiveis)
        self._aplicar_layout_padrao_agregacao(contexto, table, model, perfil)
        self._salvar_preferencias_tabela(aba, table, model, scope)
    def _salvar_perfil_tabela_com_dialogo(
        self,
        aba: str,
        table: QTableView,
        model: PolarsTableModel,
        combo: QComboBox,
        presets: list[str],
        scope: str | None = None,
    ) -> None:
        if model.dataframe.is_empty():
            self.show_info(
                "Salvar perfil", "Nao ha dados carregados para salvar um perfil."
            )
            return
        nome, ok = QInputDialog.getText(self, "Salvar perfil", "Nome do perfil:")
        nome = (nome or "").strip()
        if not ok or not nome:
            return
        if nome.lower() in {p.lower() for p in presets} and nome.lower() != "exportar":
            self.show_error(
                "Nome invalido", "Escolha um nome diferente dos perfis padrao."
            )
            return
        self._salvar_perfil_nomeado_tabela(aba, table, model, nome, scope)
        self._atualizar_combo_perfis_tabela(combo, aba, presets, scope)
        combo.setCurrentText(nome)
    def _aplicar_ordenacao_padrao(
        self,
        table: QTableView,
        model: PolarsTableModel,
        colunas_prioritarias: list[str],
        order: Qt.SortOrder = Qt.AscendingOrder,
    ) -> None:
        if model.dataframe.is_empty():
            return

        colunas = model.dataframe.columns
        deslocamento = 1 if getattr(model, "_checkable", False) else 0
        for nome in colunas_prioritarias:
            if nome not in colunas:
                continue
            idx = colunas.index(nome) + deslocamento
            model.sort(idx, order)
            table.sortByColumn(idx, order)
            return
    def _aplicar_preset_colunas(
        self, table: QTableView, colunas: list[str], visiveis: list[str]
    ) -> None:
        visiveis_set = set(visiveis)
        model = table.model()
        if not isinstance(model, PolarsTableModel):
            return
        offset = 1 if getattr(model, "_checkable", False) else 0
        colunas_modelo = list(model.dataframe.columns)
        for idx, nome in enumerate(colunas_modelo):
            table.setColumnHidden(idx + offset, nome not in visiveis_set)
    def _aplicar_ordem_colunas(
        self, table: QTableView, ordem_colunas: list[str]
    ) -> None:
        model = table.model()
        if not isinstance(model, PolarsTableModel) or model.dataframe.is_empty():
            return
        header = table.horizontalHeader()
        offset = 1 if getattr(model, "_checkable", False) else 0
        for idx, nome in enumerate(ordem_colunas):
            if nome not in model.dataframe.columns:
                continue
            logical_index = model.dataframe.columns.index(nome) + offset
            visual_atual = header.visualIndex(logical_index)
            visual_destino = idx + offset
            if visual_atual != visual_destino:
                header.moveSection(visual_atual, visual_destino)
    def _dataframe_colunas_visiveis(
        self, table: QTableView, model: PolarsTableModel, df: pl.DataFrame | None = None
    ) -> pl.DataFrame:
        base_df = df if df is not None else model.dataframe
        if base_df.is_empty():
            return base_df
        offset = 1 if getattr(model, "_checkable", False) else 0
        colunas_modelo = list(model.dataframe.columns)
        header = table.horizontalHeader()
        visiveis = [
            nome
            for idx, nome in enumerate(colunas_modelo)
            if not table.isColumnHidden(idx + offset)
        ]
        ordem_visual = [
            nome
            for _visual, nome in sorted(
                (
                    (header.visualIndex(idx + offset), nome)
                    for idx, nome in enumerate(colunas_modelo)
                ),
                key=lambda item: item[0],
            )
        ]
        visiveis = ordenar_colunas_visiveis(
            list(base_df.columns), visiveis, ordem_visual
        )
        return base_df.select(visiveis) if visiveis else base_df
    def _dataframe_colunas_perfil(
        self,
        aba: str,
        contexto: str,
        model: PolarsTableModel,
        df: pl.DataFrame | None = None,
        perfil: str = "Exportar",
        scope: str | None = None,
    ) -> pl.DataFrame:
        base_df = df if df is not None else model.dataframe
        if base_df.is_empty():
            return base_df

        estado_perfil = self._obter_estado_perfil_nomeado(aba, perfil, scope)
        visiveis = self._colunas_estado_perfil(estado_perfil, model)

        if not visiveis:
            visiveis = self._obter_colunas_preset_perfil(
                perfil, list(base_df.columns), contexto
            )
            visiveis = [col for col in visiveis if col in base_df.columns]

        return base_df.select(visiveis) if visiveis else base_df
    def _refresh_profile_combos(self) -> None:
        combos = [
            (
                self.consulta_profile,
                "consulta",
                ["Padrao", "Auditoria", "Estoque", "Custos"],
                self._consulta_scope(),
            ),
            (
                self.top_profile,
                "agregacao_top",
                ["Padrao", "Auditoria", "Estoque", "Custos"],
                None,
            ),
            (
                self.bottom_profile,
                "agregacao_bottom",
                ["Padrao", "Auditoria", "Estoque", "Custos"],
                None,
            ),
            (
                self.conversao_profile,
                "conversao",
                ["Padrao", "Auditoria", "Estoque", "Custos"],
                None,
            ),
            (
                self.mov_profile,
                "mov_estoque",
                [
                    "Exportar",
                    "Padrao",
                    "Contribuinte",
                    "Auditoria",
                    "Auditoria Fiscal",
                    "Estoque",
                    "Custos",
                ],
                None,
            ),
            (
                self.mensal_profile,
                "aba_mensal",
                ["Exportar", "Padrao", "Auditoria", "Estoque", "Custos"],
                None,
            ),
            (
                self.anual_profile,
                "aba_anual",
                ["Exportar", "Padrao", "Auditoria", "Estoque", "Custos"],
                None,
            ),
            (
                self.produtos_sel_profile,
                "produtos_selecionados",
                ["Padrao", "Auditoria", "Estoque", "Custos"],
                None,
            ),
            (
                self.nfe_entrada_profile,
                "nfe_entrada",
                ["Padrao", "Auditoria", "Estoque", "Custos"],
                None,
            ),
            (
                self.id_agrupados_profile,
                "id_agrupados",
                ["Padrao", "Auditoria", "Estoque", "Custos"],
                None,
            ),
        ]
        for combo, aba, presets, scope in combos:
            if combo is not None:
                self._atualizar_combo_perfis_tabela(combo, aba, presets, scope)
    def _aplicar_perfil_consulta(self) -> None:
        if self.table_model.dataframe.is_empty():
            return
        self._aplicar_perfil_tabela(
            "consulta",
            self.table_view,
            self.table_model,
            self.consulta_profile.currentText(),
            "consulta",
            self._consulta_scope(),
        )
    def _aplicar_perfil_agregacao(
        self,
        aba: str,
        table: QTableView,
        model: PolarsTableModel,
        perfil: str,
    ) -> None:
        if model.dataframe.is_empty():
            return
        self._aplicar_perfil_tabela(aba, table, model, perfil, aba)
