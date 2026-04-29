from __future__ import annotations

import sys
from types import ModuleType, SimpleNamespace

import polars as pl
import pytest


class TextValue:
    def __init__(self, value: str) -> None:
        self.value = value

    def currentText(self) -> str:  # noqa: N802
        return self.value

    def text(self) -> str:
        return self.value


class ModelWithChecks:
    def __init__(self) -> None:
        self.dataframe = pl.DataFrame()
        self.checked_keys: set[tuple[str]] = set()

    def set_dataframe(self, df: pl.DataFrame) -> None:
        self.dataframe = df

    def get_checked_rows(self) -> list[dict[str, str]]:
        return []

    def set_checked_keys(self, keys: set[tuple[str]]) -> None:
        self.checked_keys = keys


class DateFilter:
    def date(self) -> None:
        return None


@pytest.fixture
def pyside_stubs(monkeypatch: pytest.MonkeyPatch) -> None:
    pyside = ModuleType("PySide6")
    pyside.__path__ = []
    qtcore = ModuleType("PySide6.QtCore")
    qtcore.QDate = object
    qtcore.QThread = object
    qtcore.Qt = object
    qtcore.Signal = lambda *args, **kwargs: object()
    qtgui = ModuleType("PySide6.QtGui")
    qtgui.QFont = type("QFont", (), {"setBold": lambda self, value: None})
    qtwidgets = ModuleType("PySide6.QtWidgets")
    for name in ("QMessageBox", "QDateEdit", "QLabel", "QLineEdit"):
        setattr(qtwidgets, name, object)
    monkeypatch.setitem(sys.modules, "PySide6", pyside)
    monkeypatch.setitem(sys.modules, "PySide6.QtCore", qtcore)
    monkeypatch.setitem(sys.modules, "PySide6.QtGui", qtgui)
    monkeypatch.setitem(sys.modules, "PySide6.QtWidgets", qtwidgets)


def test_style_helpers_map_operational_states(pyside_stubs: None) -> None:
    from interface_grafica.controllers.relatorios_style_controller import (
        RelatoriosStyleControllerMixin,
        _is_truthy,
    )

    mixin = RelatoriosStyleControllerMixin()
    cases = [
        (mixin._aba_mensal_background, {"mes": 2}, "#1f1f1f"),
        (mixin._aba_mensal_background, {"entradas_desacob": 1}, "#5b3a06"),
        (mixin._aba_mensal_foreground, {"ICMS_entr_desacob": 1}, "#fff7ed"),
        (mixin._aba_mensal_foreground, {}, "#f5f5f5"),
        (mixin._aba_anual_foreground, {"saidas_desacob": 1}, "#fff7ed"),
        (mixin._aba_anual_foreground, {}, "#f5f5f5"),
        (mixin._aba_anual_background, {"entradas_desacob": 1}, "#5b3a06"),
        (mixin._mov_estoque_foreground, {"excluir_estoque": "S"}, "#94a3b8"),
        (mixin._mov_estoque_foreground, {"Tipo_operacao": "ENTRADA"}, "#93c5fd"),
        (mixin._mov_estoque_foreground, {"Tipo_operacao": "SAIDA"}, "#fca5a5"),
        (mixin._mov_estoque_foreground, {"Tipo_operacao": "ESTOQUE INICIAL"}, "#bfdbfe"),
        (mixin._mov_estoque_foreground, {"Tipo_operacao": "ESTOQUE FINAL"}, "#fde68a"),
        (mixin._mov_estoque_foreground, {"entr_desac_anual": 1}, "#fdba74"),
        (mixin._mov_estoque_foreground, {}, None),
        (mixin._mov_estoque_background, {"entr_desac_anual": 1}, "#431407"),
        (mixin._mov_estoque_background, {"excluir_estoque": "S"}, "#1e293b"),
        (mixin._mov_estoque_background, {"mov_rep": "1"}, "#111827"),
        (mixin._mov_estoque_background, {"Tipo_operacao": "ESTOQUE INICIAL"}, "#0f172a"),
        (mixin._mov_estoque_background, {"Tipo_operacao": "ENTRADA"}, "#10213f"),
        (mixin._mov_estoque_background, {"Tipo_operacao": "ESTOQUE FINAL"}, "#3f2f10"),
        (mixin._mov_estoque_background, {"Tipo_operacao": "SAIDA"}, "#3b1212"),
        (mixin._mov_estoque_background, {}, None),
    ]
    assert _is_truthy("sim")
    assert all(func(row, "") == expected for func, row, expected in cases)
    assert mixin._aba_anual_background({"id_agregado": "A"}, "") in {"#1f1f1f", "#262626"}
    assert mixin._mov_estoque_font({"entr_desac_anual": 1}, "") is not None
    assert mixin._mov_estoque_font({}, "") is None
    assert mixin._formatar_resumo_filtros([]) == "Filtros ativos: nenhum"


def test_sql_param_validation_normalizes_cnpj_and_dates(pyside_stubs: None) -> None:
    from interface_grafica.controllers.sql_query_controller import SqlQueryControllerMixin

    mixin = SqlQueryControllerMixin()
    values = {"cnpj": "11.222.333/0001-81", "data_ini": "2025-01-01", "data_fim": "31/12/2025"}
    mixin._validate_sql_param_values(values)
    assert values["cnpj"] == "11222333000181"
    assert mixin._sql_date_param_role("dt_ini_periodo") == ("start", ("periodo",))
    assert mixin._sql_date_param_role("") == (None, ())
    with pytest.raises(ValueError):
        mixin._validate_sql_param_values({"inicio": "2025-02-01", "fim": "2025-01-01"})


def test_produtos_helpers_filter_ids_years_and_checked_rows(pyside_stubs: None) -> None:
    from interface_grafica.controllers.relatorios_produtos_controller import (
        RelatoriosProdutosControllerMixin,
    )

    mixin = RelatoriosProdutosControllerMixin()
    mixin._aba_mensal_df = pl.DataFrame({"id_agregado": ["B"], "descr_padrao": ["Beta"], "ano": [2024]})
    mixin._aba_anual_df = pl.DataFrame({"id_agregado": ["A"], "descr_padrao": ["Alfa"], "ano": [2025]})
    mixin._mov_estoque_df = pl.DataFrame({"id_agrupado": ["C"], "descr_padrao": ["Gama"]})
    mixin.produtos_sel_filter_ano_ini = TextValue("2025")
    mixin.produtos_sel_filter_ano_fim = TextValue("2024")
    mixin.produtos_selecionados_model = SimpleNamespace(get_checked_rows=lambda: [{"id_agregado": "B"}, {"id_agregado": "A"}])
    base = mixin._coletar_base_produtos_selecionados()
    assert mixin._filtrar_dataframe_por_ids(base, ["A", "C"])["id_agregado"].to_list() == ["A", "C"]
    assert mixin._filtrar_dataframe_por_ano(mixin._aba_anual_df, 2025, None).height == 1
    assert mixin._anos_disponiveis_produtos_selecionados() == [2024, 2025]
    assert mixin._intervalo_anos_produtos_selecionados() == (2024, 2025)
    assert mixin._ids_produtos_selecionados_para_exportacao() == ["A", "B"]


def test_id_agrupados_legacy_parquet_recebe_qtd_descricoes(
    pyside_stubs: None,
) -> None:
    from interface_grafica.controllers.id_agrupados_controller import (
        _adicionar_qtd_descricoes,
    )

    df = pl.DataFrame(
        {
            "id_agrupado": ["A", "B"],
            "lista_descricoes": [["Produto A", "Produto A var"], ["Produto B"]],
        }
    )

    enriched = _adicionar_qtd_descricoes(df)

    assert enriched["qtd_descricoes"].to_list() == [2, 1]


def test_produtos_selecionados_consolida_totais_periodo(pyside_stubs: None) -> None:
    from interface_grafica.controllers.relatorios_produtos_controller import (
        RelatoriosProdutosControllerMixin,
    )

    class Controller(RelatoriosProdutosControllerMixin):
        def __init__(self) -> None:
            self.state = SimpleNamespace(current_cnpj="12345678000190")
            self.produtos_selecionados_model = ModelWithChecks()
            self.produtos_sel_table = object()
            self.lbl_produtos_sel_status = SimpleNamespace(setText=lambda _value: None)
            self.lbl_produtos_sel_resumo = SimpleNamespace(setText=lambda _value: None)
            self.lbl_produtos_sel_filtros = SimpleNamespace(setText=lambda _value: None)
            self.produtos_sel_filter_id = TextValue("")
            self.produtos_sel_filter_desc = TextValue("")
            self.produtos_sel_filter_texto = TextValue("")
            self.produtos_sel_filter_ano_ini = TextValue("Todos")
            self.produtos_sel_filter_ano_fim = TextValue("Todos")
            self.produtos_sel_filter_data_ini = DateFilter()
            self.produtos_sel_filter_data_fim = DateFilter()
            self._produtos_sel_preselecionado_cnpj = None
            self._mov_estoque_df = pl.DataFrame()
            self._aba_mensal_df = pl.DataFrame(
                {
                    "id_agregado": ["A", "A"],
                    "descr_padrao": ["Produto A", "Produto A"],
                    "ano": [2025, 2025],
                    "mes": [1, 2],
                    "ICMS_entr_desacob": [10.0, 20.0],
                    "ICMS_entr_desacob_periodo": [1.5, 2.5],
                }
            )
            self._aba_anual_df = pl.DataFrame(
                {
                    "id_agregado": ["A"],
                    "descr_padrao": ["Produto A"],
                    "ano": [2025],
                    "ICMS_saidas_desac": [7.0],
                    "ICMS_estoque_desac": [3.0],
                }
            )
            self._aba_periodos_df = pl.DataFrame(
                {
                    "id_agregado": ["A"],
                    "descr_padrao": ["Produto A"],
                    "ICMS_saidas_desac_periodo": [4.0],
                    "ICMS_estoque_desac_periodo": [6.0],
                }
            )

        def _valor_qdate_ativo(self, _value):
            return None

        def _filtrar_intervalo_data(self, df, *_args):
            return df

        def _filtrar_texto_em_colunas(self, df, _texto):
            return df

        def _formatar_resumo_filtros(self, _items):
            return "Filtros ativos: nenhum"

        def _atualizar_titulo_aba_produtos_selecionados(self, *_args):
            return None

        def _resize_table_once(self, *_args):
            return None

        def _aplicar_preferencias_tabela(self, *_args):
            return True

        def _salvar_preferencias_tabela(self, *_args):
            return None

    controller = Controller()
    controller.aplicar_filtros_produtos_selecionados()

    row = controller.produtos_selecionados_model.dataframe.row(0, named=True)
    assert row["total_ICMS_entr_desacob"] == 30.0
    assert row["total_ICMS_saidas_desac"] == 7.0
    assert row["total_ICMS_estoque_desac"] == 3.0
    assert row["total_ICMS_total"] == 40.0
    assert row["total_ICMS_entr_desacob_periodo"] == 4.0
    assert row["total_ICMS_saidas_desac_periodo"] == 4.0
    assert row["total_ICMS_estoque_desac_periodo"] == 6.0
    assert row["total_ICMS_total_periodo"] == 14.0
