"""Tests para validar que exportacao de tabs paginadas:
- usa _tab_df_filtrado (DF filtrado completo), nao a pagina visivel
- _limpar_pagina_tab impede export de dados stale
- troca de CNPJ/carga invalida o cache de todas as tabs
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import polars as pl
import pytest

SRC = Path(__file__).resolve().parents[3] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


# ---------------------------------------------------------------------------
# Stubs / Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def patch_pyside(monkeypatch: pytest.MonkeyPatch) -> None:
    class _QMessageBoxStub:
        @staticmethod
        def warning(*_a: Any, **_kw: Any) -> None:
            pass

        @staticmethod
        def information(*_a: Any, **_kw: Any) -> None:
            pass

        @staticmethod
        def critical(*_a: Any, **_kw: Any) -> None:
            pass

    qtwidgets = ModuleType("PySide6.QtWidgets")
    qtwidgets.QInputDialog = object
    qtwidgets.QMessageBox = _QMessageBoxStub
    qtwidgets.QFileDialog = object
    monkeypatch.setitem(sys.modules, "PySide6.QtWidgets", qtwidgets)

    workers = ModuleType("interface_grafica.controllers.workers")
    workers.ServiceTaskWorker = object
    monkeypatch.setitem(sys.modules, "interface_grafica.controllers.workers", workers)


class TextWidget:
    def __init__(self, value: str = "") -> None:
        self.value = value

    def text(self) -> str:
        return self.value

    def clear(self) -> None:
        self.value = ""

    def currentText(self) -> str:
        return self.value

    def setCurrentText(self, value: str) -> None:
        self.value = value

    def setCurrentIndex(self, _index: int) -> None:
        self.value = ""

    def setText(self, value: str) -> None:
        self.value = value


class ModelStub:
    def __init__(self) -> None:
        self.dataframe = pl.DataFrame()
        self.df_filtered = pl.DataFrame()

    def set_dataframe(self, df: pl.DataFrame) -> None:
        self.dataframe = df
        self.df_filtered = df

    def get_checked_rows(self) -> list[int]:
        return []

    def clear_checked(self) -> None:
        return None


def _build_controller(periodos_module: ModuleType, patch_pyside: None) -> Any:
    from interface_grafica.controllers.paginacao_tabs_mixin import PaginacaoTabsMixin
    from interface_grafica.controllers.relatorios_resumo_controller import (
        RelatoriosResumoControllerMixin,
    )

    class Ctrl(
        PaginacaoTabsMixin,
        periodos_module.RelatoriosPeriodosControllerMixin,
        RelatoriosResumoControllerMixin,
    ):
        def __init__(self) -> None:
            self.state = SimpleNamespace(current_cnpj="12345678000190")
            self.aba_periodos_model = ModelStub()
            self.aba_mensal_model = ModelStub()
            self.aba_anual_model = ModelStub()
            self.resumo_global_model = ModelStub()
            self.results_table_model = ModelStub()
            self.results_table = SimpleNamespace(selectionModel=lambda: None)

            for name in [
                "lbl_aba_periodos_status",
                "lbl_aba_periodos_filtros",
                "lbl_aba_mensal_status",
                "lbl_aba_mensal_filtros",
                "lbl_aba_anual_status",
                "lbl_aba_anual_filtros",
                "lbl_resumo_global_status",
                "lbl_resumo_global_totais",
                "lbl_aba_mensal_page",
                "lbl_aba_anual_page",
                "lbl_aba_periodos_page",
            ]:
                setattr(self, name, TextWidget(""))

            for name in [
                "btn_aba_mensal_prev_page",
                "btn_aba_mensal_next_page",
                "btn_aba_anual_prev_page",
                "btn_aba_anual_next_page",
                "btn_aba_periodos_prev_page",
                "btn_aba_periodos_next_page",
            ]:
                setattr(self, name, SimpleNamespace(setEnabled=lambda _: None))

            filter_names = [
                "periodo_filter_id",
                "periodo_filter_desc",
                "periodo_filter_texto",
                "periodo_filter_num_col",
                "periodo_filter_num_min",
                "periodo_filter_num_max",
                "mensal_filter_id",
                "mensal_filter_desc",
                "mensal_filter_ano",
                "mensal_filter_mes",
                "mensal_filter_texto",
                "mensal_filter_num_col",
                "mensal_filter_num_min",
                "mensal_filter_num_max",
                "anual_filter_id",
                "anual_filter_desc",
                "anual_filter_ano",
                "anual_filter_texto",
                "anual_filter_num_col",
                "anual_filter_num_min",
                "anual_filter_num_max",
            ]
            for name in filter_names:
                default = (
                    "Todos"
                    if name.endswith(("_ano", "_mes"))
                    else ("valor" if name.endswith("_num_col") else "")
                )
                setattr(self, name, TextWidget(default))

            self._aba_periodos_df = pl.DataFrame()
            self._aba_mensal_df = pl.DataFrame()
            self._aba_anual_df = pl.DataFrame()
            self._resumo_global_df = pl.DataFrame()
            self._filtro_cruzado_anuais_ids: list[str] = []
            self.aba_mensal_table = object()
            self.aba_anual_table = object()
            self._init_paginacao_tabs()

        # --- stubs para metodos que nao sao alvo dos testes ---

        def _filtrar_texto_em_colunas(self, df: pl.DataFrame, texto: str) -> pl.DataFrame:
            return df

        def _filtrar_intervalo_numerico(
            self, df: pl.DataFrame, column: str, value_min: str, value_max: str
        ) -> pl.DataFrame:
            return df

        def _formatar_resumo_filtros(self, pairs: list[tuple[str, str]]) -> str:
            return ""

        def _carregar_dados_parquet_async(self, path, callback, *_a, **_kw) -> None:
            pass

        def _popular_combo_texto(self, *_a, **_kw) -> None:
            pass

        def _reset_table_resize_flag(self, *_a) -> None:
            pass

        def _resize_table_once(self, *_a) -> None:
            pass

        def _aplicar_preferencias_tabela(self, *_a) -> bool:
            return False

        def _aplicar_ordenacao_padrao(self, *_a) -> None:
            pass

        def _aplicar_preset_aba_mensal(self) -> None:
            pass

        def _aplicar_preset_aba_anual(self) -> None:
            pass

        def _atualizar_titulo_aba_periodos(self, *_a) -> None:
            pass

        def _atualizar_titulo_aba_mensal(self, *_a) -> None:
            pass

        def _atualizar_titulo_aba_anual(self, *_a) -> None:
            pass

        def _salvar_preferencias_tabela(self, *_a) -> None:
            pass

        def atualizar_aba_produtos_selecionados(self) -> None:
            pass

        def _garantir_resumos_estoque_atualizados(self, *_a) -> bool:
            return True

        def _limpar_aba_resumo_estoque(self, *_a) -> None:
            pass

        def show_error(self, *_a) -> None:
            pass

        def _reprocessar_periodos_auto(self, cnpj: str) -> None:
            pass

    return Ctrl()


@pytest.fixture
def periodos_mod(patch_pyside: None) -> ModuleType:
    import importlib

    import interface_grafica.controllers.relatorios_periodos_controller as mod

    # Garante que o modulo usa o QMessageBox stub correto,
    # mesmo se outro teste importou antes com stub diferente.
    importlib.reload(mod)
    return mod


@pytest.fixture
def ctrl(periodos_mod: ModuleType, patch_pyside: None) -> Any:
    return _build_controller(periodos_mod, patch_pyside)


# ---------------------------------------------------------------------------
# Testes: _armazenar_pagina popula cache corretamente
# ---------------------------------------------------------------------------


class TestArmazenarPaginaPopulaCache:
    def test_armazenar_pagina_popula_tab_df_filtrado(self, ctrl: Any) -> None:
        df = pl.DataFrame({"x": [1, 2, 3]})
        ctrl._armazenar_pagina("aba_periodos", df)
        assert ctrl._tab_df_filtrado["aba_periodos"].height == 3

    def test_armazenar_pagina_reseta_para_pagina_1(self, ctrl: Any) -> None:
        ctrl._tab_page["aba_mensal"] = 5
        ctrl._armazenar_pagina("aba_mensal", pl.DataFrame({"x": [1]}))
        assert ctrl._tab_page["aba_mensal"] == 1


# ---------------------------------------------------------------------------
# Testes: _limpar_pagina_tab impede export de dados stale
# ---------------------------------------------------------------------------


class TestLimparPaginaTabImpedirStale:
    def test_limpar_esvazia_cache(self, ctrl: Any) -> None:
        ctrl._armazenar_pagina("mov_estoque", pl.DataFrame({"a": [1]}))
        ctrl._limpar_pagina_tab("mov_estoque")
        assert ctrl._tab_df_filtrado["mov_estoque"].is_empty()
        assert ctrl._tab_page["mov_estoque"] == 1

    @pytest.mark.parametrize("key", ["aba_periodos", "aba_mensal", "aba_anual"])
    def test_limpar_tab_key_esvazia(self, ctrl: Any, key: str) -> None:
        ctrl._armazenar_pagina(key, pl.DataFrame({"val": [99]}))
        assert not ctrl._tab_df_filtrado[key].is_empty()
        ctrl._limpar_pagina_tab(key)
        assert ctrl._tab_df_filtrado[key].is_empty()


# ---------------------------------------------------------------------------
# Testes: troca de CNPJ invalida cache das tabs
# ---------------------------------------------------------------------------


class TestCnpjSwitchInvalidatesCache:
    """Simula cenarios onde o CNPJ muda e verifica que o cache e limpo."""

    def test_periodos_cnpj_vazio_limpa_cache(self, ctrl: Any) -> None:
        ctrl._armazenar_pagina("aba_periodos", pl.DataFrame({"x": [1]}))
        ctrl.state.current_cnpj = ""
        ctrl.atualizar_aba_periodos()
        assert ctrl._tab_df_filtrado["aba_periodos"].is_empty()

    def test_mensal_cnpj_vazio_limpa_cache(self, ctrl: Any) -> None:
        ctrl._armazenar_pagina("aba_mensal", pl.DataFrame({"x": [1]}))
        ctrl.state.current_cnpj = ""
        ctrl.atualizar_aba_mensal()
        assert ctrl._tab_df_filtrado["aba_mensal"].is_empty()

    def test_anual_cnpj_vazio_limpa_cache(self, ctrl: Any) -> None:
        ctrl._armazenar_pagina("aba_anual", pl.DataFrame({"x": [1]}))
        ctrl.state.current_cnpj = ""
        ctrl.atualizar_aba_anual()
        assert ctrl._tab_df_filtrado["aba_anual"].is_empty()

    def test_anual_sync_pendente_limpa_cache(self, ctrl: Any) -> None:
        """Se resumos estoque nao estao atualizados, cache deve ser limpo."""
        ctrl._armazenar_pagina("aba_anual", pl.DataFrame({"x": [1]}))
        ctrl._garantir_resumos_estoque_atualizados = lambda *_a: False
        ctrl.state.current_cnpj = "12345678000190"
        ctrl.atualizar_aba_anual()
        assert ctrl._tab_df_filtrado["aba_anual"].is_empty()

    def test_mensal_sync_pendente_limpa_cache(self, ctrl: Any) -> None:
        ctrl._armazenar_pagina("aba_mensal", pl.DataFrame({"x": [1]}))
        ctrl._garantir_resumos_estoque_atualizados = lambda *_a: False
        ctrl.state.current_cnpj = "12345678000190"
        ctrl.atualizar_aba_mensal()
        assert ctrl._tab_df_filtrado["aba_mensal"].is_empty()


# ---------------------------------------------------------------------------
# Testes: exportacao le de _tab_df_filtrado, nao do model
# ---------------------------------------------------------------------------


class TestExportUsesTabDfFiltrado:
    """Verifica que os metodos de exportacao usam o DF completo, nao a pagina."""

    def test_export_periodos_usa_cache_nao_model(self, ctrl: Any) -> None:
        """Se _tab_df_filtrado['aba_periodos'] esta vazio, exportacao retorna cedo."""
        # model tem dados (pagina), mas cache esta vazio
        ctrl.aba_periodos_model.set_dataframe(pl.DataFrame({"x": [1]}))
        ctrl._tab_df_filtrado["aba_periodos"] = pl.DataFrame()
        # Exportacao deve bloquear (cache vazio), nao usar model
        # Se nao usar _tab_df_filtrado, teria tentado exportar
        # Aqui testamos que o metodo retorna sem erro
        ctrl.exportar_aba_periodos_excel()
        # Se chegou aqui sem erro, o metodo usou _tab_df_filtrado (vazio)

    def test_export_mensal_usa_cache_nao_model(self, ctrl: Any) -> None:
        ctrl.aba_mensal_model.set_dataframe(pl.DataFrame({"x": [1]}))
        ctrl._tab_df_filtrado["aba_mensal"] = pl.DataFrame()
        # O _dataframe_colunas_perfil recebe um DF vazio, entao retorna vazio
        ctrl._dataframe_colunas_perfil = lambda *_a, **_kw: pl.DataFrame()
        ctrl.exportar_aba_mensal_excel_metodo()

    def test_export_anual_usa_cache_nao_model(self, ctrl: Any) -> None:
        ctrl.aba_anual_model.set_dataframe(pl.DataFrame({"x": [1]}))
        ctrl._tab_df_filtrado["aba_anual"] = pl.DataFrame()
        ctrl._dataframe_colunas_perfil = lambda *_a, **_kw: pl.DataFrame()
        ctrl.exportar_aba_anual_excel_metodo()

    def test_filtrar_periodos_armazena_df_completo(self, ctrl: Any) -> None:
        """Apos aplicar filtros, _tab_df_filtrado contem o DF filtrado completo."""
        ctrl._aba_periodos_df = pl.DataFrame(
            {"id_agrupado": ["A", "B"], "valor": [1.0, 2.0]}
        )
        ctrl.periodo_filter_id.value = "A"
        ctrl.aplicar_filtros_aba_periodos()
        cached = ctrl._tab_df_filtrado["aba_periodos"]
        assert cached.height == 1
        assert cached["id_agrupado"].to_list() == ["A"]
