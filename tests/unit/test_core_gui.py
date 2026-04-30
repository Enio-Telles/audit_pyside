from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import polars as pl
import pytest
from openpyxl import Workbook
from datetime import date


SRC = Path(__file__).resolve().parents[2] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


@pytest.fixture
def patch_pyside(monkeypatch: pytest.MonkeyPatch) -> None:
    qtwidgets = ModuleType("PySide6.QtWidgets")
    qtwidgets.QInputDialog = object
    qtwidgets.QMessageBox = object
    qtwidgets.QFileDialog = object
    monkeypatch.setitem(sys.modules, "PySide6.QtWidgets", qtwidgets)

    workers = ModuleType("interface_grafica.controllers.workers")
    workers.ServiceTaskWorker = object
    monkeypatch.setitem(sys.modules, "interface_grafica.controllers.workers", workers)


@pytest.fixture
def gui_modules(
    patch_pyside: None,
) -> tuple[type[Any], ModuleType, type[Any]]:
    import interface_grafica.controllers.agregacao_controller as agg_mod
    from interface_grafica.controllers import relatorios_periodos_controller as per_mod
    from interface_grafica.controllers.relatorios_resumo_controller import (
        RelatoriosResumoControllerMixin,
    )

    return agg_mod.AgregacaoControllerMixin, per_mod, RelatoriosResumoControllerMixin


class TextWidget:
    value: str

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
    dataframe: pl.DataFrame
    df_filtered: pl.DataFrame

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


def build_gui_controller(
    periodos_module: ModuleType,
    resumo_mixin: type[Any],
) -> Any:
    class Gui(periodos_module.RelatoriosPeriodosControllerMixin, resumo_mixin):
        state: SimpleNamespace
        results_table_model: ModelStub
        aba_periodos_model: ModelStub
        aba_mensal_model: ModelStub
        aba_anual_model: ModelStub
        resumo_global_model: ModelStub
        lbl_aba_periodos_status: TextWidget
        lbl_aba_periodos_filtros: TextWidget
        lbl_aba_mensal_status: TextWidget
        lbl_aba_mensal_filtros: TextWidget
        lbl_aba_anual_status: TextWidget
        lbl_aba_anual_filtros: TextWidget
        lbl_resumo_global_status: TextWidget
        lbl_resumo_global_totais: TextWidget
        _aba_periodos_df: pl.DataFrame
        _aba_mensal_df: pl.DataFrame
        _aba_anual_df: pl.DataFrame
        _resumo_global_df: pl.DataFrame
        _filtro_cruzado_anuais_ids: list[str]
        aba_mensal_table: Any
        aba_anual_table: Any

        def __init__(self) -> None:
            self.state = SimpleNamespace(current_cnpj="12345678000190")
            self.results_table_model = ModelStub()
            self.aba_periodos_model = ModelStub()
            self.aba_mensal_model = ModelStub()
            self.aba_anual_model = ModelStub()
            self.resumo_global_model = ModelStub()
            self.results_table = SimpleNamespace(selectionModel=lambda: None)
            self.lbl_aba_periodos_status = TextWidget("")
            self.lbl_aba_periodos_filtros = TextWidget("")
            self.lbl_aba_mensal_status = TextWidget("")
            self.lbl_aba_mensal_filtros = TextWidget("")
            self.lbl_aba_anual_status = TextWidget("")
            self.lbl_aba_anual_filtros = TextWidget("")
            self.lbl_resumo_global_status = TextWidget("")
            self.lbl_resumo_global_totais = TextWidget("")

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
            self._filtro_cruzado_anuais_ids = []
            self.aba_mensal_table = object()
            self.aba_anual_table = object()

        def _filtrar_texto_em_colunas(
            self,
            df: pl.DataFrame,
            texto: str,
        ) -> pl.DataFrame:
            expr = pl.lit(False)
            for column in [c for c in df.columns if df[c].dtype == pl.String]:
                expr |= (
                    pl.col(column)
                    .cast(pl.Utf8, strict=False)
                    .fill_null("")
                    .str.to_lowercase()
                    .str.contains(texto, literal=True)
                )
            return df.filter(expr)

        def _filtrar_intervalo_numerico(
            self,
            df: pl.DataFrame,
            column: str,
            value_min: str,
            value_max: str,
        ) -> pl.DataFrame:
            if column and value_min:
                df = df.filter(pl.col(column) >= float(value_min))
            if column and value_max:
                df = df.filter(pl.col(column) <= float(value_max))
            return df

        def _formatar_resumo_filtros(
            self,
            pairs: list[tuple[str, str]],
        ) -> str:
            return ", ".join(f"{key}={value}" for key, value in pairs if value) or "nenhum"

        def _carregar_dados_parquet_async(
            self,
            path: Path,
            callback: Any,
            *_args: Any,
            unique_cols: list[str] | None = None,
        ) -> None:
            df = pl.read_parquet(path)
            callback(
                df,
                {
                    col: df.get_column(col).drop_nulls().unique().to_list()
                    for col in unique_cols or []
                },
            )

        def _popular_combo_texto(
            self,
            combo: TextWidget,
            values: list[str],
            *_args: Any,
        ) -> None:
            combo.values = values

        def _reset_table_resize_flag(self, *_args: Any) -> None:
            return None

        def _resize_table_once(self, *_args: Any) -> None:
            return None

        def _aplicar_preferencias_tabela(self, *_args: Any) -> bool:
            return False

        def _aplicar_ordenacao_padrao(self, *_args: Any) -> None:
            return None

        def _aplicar_preset_aba_mensal(self) -> None:
            return None

        def _aplicar_preset_aba_anual(self) -> None:
            return None

        def _atualizar_titulo_aba_periodos(self, *_args: Any) -> None:
            return None

        def _atualizar_titulo_aba_mensal(self, *_args: Any) -> None:
            return None

        def _atualizar_titulo_aba_anual(self, *_args: Any) -> None:
            return None

        def _salvar_preferencias_tabela(self, *_args: Any) -> None:
            return None

        def atualizar_aba_produtos_selecionados(self) -> None:
            return None

        def _garantir_resumos_estoque_atualizados(self, *_args: Any) -> bool:
            return True

        def _limpar_aba_resumo_estoque(self, *_args: Any) -> None:
            return None

        def show_error(self, *_args: Any) -> None:
            return None

        def atualizar_aba_mensal(self) -> None:
            self._aba_mensal_df = pl.DataFrame(
                {"ano": [2025], "mes": [1], "ICMS_entr_desacob": [1.0]}
            )

        def atualizar_aba_anual(self) -> None:
            self._aba_anual_df = pl.DataFrame(
                {
                    "ano": [2025],
                    "ICMS_saidas_desac": [2.0],
                    "ICMS_estoque_desac": [3.0],
                }
            )

    return Gui()


def test_agregacao_helpers_cover_key_paths(gui_modules: tuple[type[Any], ModuleType, type[Any]]) -> None:
    agregacao_mixin, _periodos, _resumo = gui_modules

    class AggregationController(agregacao_mixin):
        pass

    controller = AggregationController()
    controller.results_table_model = SimpleNamespace(
        get_checked_rows=lambda: [
            {"id_agrupado": "A"},
            {"id_agrupado": "A"},
            {"id_agrupado": "B"},
        ],
        get_dataframe=lambda: pl.DataFrame(),
    )
    controller.results_table = SimpleNamespace(selectionModel=lambda: None)
    df = pl.DataFrame(
        {
            "ncm_padrao": ["1", "1", "2"],
            "cest_padrao": ["A", "A", "B"],
            "gtin_padrao": ["X", "X", "Z"],
            "descricao": ["p1", "p2", "p3"],
        }
    )

    assert controller._obter_ids_agrupados_para_reversao() == ["A", "B"]
    assert controller._resolver_coluna_agregacao(
        ["descricao"],
        ["descrição", "ncm"],
    ) == "descrição"
    assert controller._aplicar_modo_relacional_agregacao_df(
        df,
        "ncm_cest_gtin",
    )["descricao"].to_list() == ["p1", "p2"]


def test_periodos_mensal_anual_and_resumo(
    gui_modules: tuple[type[Any], ModuleType, type[Any]],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _agregacao, periodos_module, resumo_mixin = gui_modules
    controller = build_gui_controller(periodos_module, resumo_mixin)
    monkeypatch.setattr(periodos_module, "CNPJ_ROOT", tmp_path)
    monkeypatch.setattr(
        "interface_grafica.controllers.relatorios_resumo_controller.CNPJ_ROOT",
        tmp_path,
    )
    controller._reprocessar_periodos_auto = (
        lambda cnpj: setattr(controller, "reprocessado", cnpj)
    )
    controller.atualizar_aba_periodos()
    assert controller.reprocessado == "12345678000190"

    folder = tmp_path / controller.state.current_cnpj / "analises" / "produtos"
    folder.mkdir(parents=True)
    pl.DataFrame({"id_agrupado": ["A"], "valor": [1.0]}).write_parquet(
        folder / f"aba_periodos_{controller.state.current_cnpj}.parquet"
    )
    controller.atualizar_aba_periodos()
    assert controller.aba_periodos_model.dataframe["id_agrupado"].to_list() == ["A"]

    controller._aba_periodos_df = pl.DataFrame(
        {
            "id_agrupado": ["A", "B"],
            "descr_padrao": ["Produto bom", "Outro"],
            "texto": ["abc", "zzz"],
            "valor": [10.0, 1.0],
        }
    )
    controller.periodo_filter_id.value = "A"
    controller.periodo_filter_desc.value = "produto"
    controller.periodo_filter_texto.value = "abc"
    controller.periodo_filter_num_min.value = "5"
    controller.aplicar_filtros_aba_periodos()
    assert controller.aba_periodos_model.dataframe["id_agrupado"].to_list() == ["A"]

    controller._aba_mensal_df = pl.DataFrame(
        {
            "id_agregado": ["A", "B"],
            "descr_padrao": ["Produto bom", "Outro"],
            "ano": [2025, 2024],
            "mes": [1, 2],
            "texto": ["abc", "zzz"],
            "valor": [10.0, 1.0],
        }
    )
    controller.mensal_filter_id.value = "A"
    controller.mensal_filter_desc.value = "produto"
    controller.mensal_filter_ano.value = "2025"
    controller.mensal_filter_num_min.value = "5"
    controller.aplicar_filtros_aba_mensal()
    assert controller.aba_mensal_model.dataframe["id_agregado"].to_list() == ["A"]

    controller._aba_anual_df = pl.DataFrame(
        {
            "id_agregado": ["A", "B"],
            "descr_padrao": ["Produto bom", "Outro"],
            "ano": [2025, 2025],
            "texto": ["abc", "zzz"],
            "valor": [10.0, 1.0],
        }
    )
    controller._filtro_cruzado_anuais_ids = ["A"]
    controller.anual_filter_desc.value = "produto"
    controller.anual_filter_num_min.value = "5"
    controller.aplicar_filtros_aba_anual()
    assert controller.aba_anual_model.dataframe["id_agregado"].to_list() == ["A"]

    worksheet = Workbook().active
    controller._escrever_planilha_openpyxl(
        worksheet,
        pl.DataFrame(
            {
                "ano": [2025],
                "valor": [12.5],
                "Dt_doc": ["01/10/2021 00:00:00"],
                "Dt_e_s": ["01/10/2021 00:00:00"],
            }
        ),
    )
    assert worksheet["A2"].number_format == "0"
    assert worksheet["B2"].number_format == "#,##0.00"
    assert worksheet["C2"].value == date(2021, 10, 1)
    assert worksheet["D2"].value == date(2021, 10, 1)
    assert worksheet["C2"].number_format == "dd/mm/yyyy"
    assert worksheet["D2"].number_format == "dd/mm/yyyy"

    controller._aba_mensal_df = pl.DataFrame(
        {
            "ano": [2025],
            "mes": [1],
            "ICMS_entr_desacob": [10.0],
            "ICMS_entr_desacob_periodo": [2.0],
        }
    )
    controller._aba_anual_df = pl.DataFrame(
        {"ano": [2025], "ICMS_saidas_desac": [7.0], "ICMS_estoque_desac": [3.0]}
    )
    controller._aba_periodos_df = pl.DataFrame(
        {
            "cod_per": [202501],
            "ICMS_saidas_desac_periodo": [4.0],
            "ICMS_estoque_desac_periodo": [6.0],
        }
    )
    pl.DataFrame({"Ano/Mes": ["2025-01"], "Total": [1.0]}).write_parquet(
        folder / f"aba_resumo_global_{controller.state.current_cnpj}.parquet"
    )
    controller.atualizar_aba_resumo_global()
    row_janeiro = controller._resumo_global_df.filter(pl.col("Ano/Mes") == "2025-01").row(
        0, named=True
    )
    assert row_janeiro["ICMS_entr_desacob_periodo"] == 2.0
    assert row_janeiro["ICMS_saidas_desac_periodo"] == 4.0
    assert row_janeiro["ICMS_estoque_desac_periodo"] == 6.0
    assert row_janeiro["Total_periodo"] == 12.0


def test_resumo_global_helpers_cover_empty_and_consolidated(
    gui_modules: tuple[type[Any], ModuleType, type[Any]],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _agregacao, _periodos, resumo_mixin = gui_modules
    import interface_grafica.controllers.relatorios_periodos_controller as periodos_module

    controller = build_gui_controller(periodos_module, resumo_mixin)
    monkeypatch.setattr(
        "interface_grafica.controllers.relatorios_resumo_controller.CNPJ_ROOT",
        tmp_path,
    )
    empty = controller._gerar_resumo_global(pl.DataFrame(), pl.DataFrame(), [])
    assert empty.columns == [
        "Ano/Mes",
        "ICMS_entr_desacob",
        "ICMS_saidas_desac",
        "ICMS_estoque_desac",
        "Total",
        "ICMS_entr_desacob_periodo",
        "ICMS_saidas_desac_periodo",
        "ICMS_estoque_desac_periodo",
        "Total_periodo",
    ]
    assert empty.height == 0

    controller._aba_mensal_df = pl.DataFrame(
        {
            "ano": [2025, 2025],
            "mes": [1, 2],
            "ICMS_entr_desacob": [10.0, 5.0],
            "ICMS_entr_desacob_periodo": [2.0, 3.0],
        }
    )
    controller._aba_anual_df = pl.DataFrame(
        {"ano": [2025], "ICMS_saidas_desac": [7.0], "ICMS_estoque_desac": [3.0]}
    )
    controller._aba_periodos_df = pl.DataFrame(
        {
            "cod_per": [202501],
            "ICMS_saidas_desac_periodo": [4.0],
            "ICMS_estoque_desac_periodo": [6.0],
        }
    )
    summary = controller._gerar_resumo_global(
        controller._aba_mensal_df,
        controller._aba_anual_df,
        controller._aba_periodos_df,
        [2025],
    )
    assert summary.filter(pl.col("Ano/Mes") == "2025-12")["Total"].item() == 10.0
    assert (
        summary.filter(pl.col("Ano/Mes") == "2025-01")["ICMS_entr_desacob_periodo"].item()
        == 2.0
    )
    assert (
        summary.filter(pl.col("Ano/Mes") == "2025-01")[
            "ICMS_saidas_desac_periodo"
        ].item()
        == 4.0
    )
    assert (
        summary.filter(pl.col("Ano/Mes") == "2025-01")[
            "ICMS_estoque_desac_periodo"
        ].item()
        == 6.0
    )
    assert summary.filter(pl.col("Ano/Mes") == "2025-01")["Total_periodo"].item() == 12.0
    controller.atualizar_aba_resumo_global()
    assert (
        controller._resumo_global_df.filter(pl.col("Ano/Mes") == "2025-01")[
            "ICMS_entr_desacob"
        ].item()
        == 10.0
    )
    assert "ICMS entradas: 15,00" in controller.lbl_resumo_global_totais.value
    assert "ICMS saidas: 7,00" in controller.lbl_resumo_global_totais.value
    assert "ICMS estoque: 3,00" in controller.lbl_resumo_global_totais.value
    assert "Total: 25,00" in controller.lbl_resumo_global_totais.value
    assert "Total periodo: 15,00" in controller.lbl_resumo_global_totais.value


def test_resumo_global_cover_branches_sem_parquet(
    gui_modules: tuple[type[Any], ModuleType, type[Any]],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _agregacao, _periodos, resumo_mixin = gui_modules
    import interface_grafica.controllers.relatorios_periodos_controller as periodos_module

    controller = build_gui_controller(periodos_module, resumo_mixin)
    monkeypatch.setattr(
        "interface_grafica.controllers.relatorios_resumo_controller.CNPJ_ROOT",
        tmp_path,
    )
    controller.atualizar_aba_resumo_global()
    assert "Aguarde o processamento" in controller.lbl_resumo_global_status.value

    folder = tmp_path / controller.state.current_cnpj / "analises" / "produtos"
    folder.mkdir(parents=True)
    (folder / f"aba_mensal_{controller.state.current_cnpj}.parquet").write_text("x")
    (folder / f"aba_anual_{controller.state.current_cnpj}.parquet").write_text("x")
    controller._aba_mensal_df = pl.DataFrame()
    controller._aba_anual_df = pl.DataFrame()
    controller.atualizar_aba_resumo_global()
    assert "Carregando depend" in controller.lbl_resumo_global_status.value


def test_workers_and_query_worker_threads(
    patch_pyside: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class SignalStub:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            self.values: list[Any] = []

        def emit(self, value: Any = None) -> None:
            self.values.append(value)

    class ThreadStub:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            self._cancel = False

        def isInterruptionRequested(self) -> bool:
            return self._cancel

    qtcore = ModuleType("PySide6.QtCore")
    qtcore.QThread = ThreadStub
    qtcore.Signal = SignalStub
    monkeypatch.setitem(sys.modules, "PySide6.QtCore", qtcore)
    monkeypatch.delitem(
        sys.modules,
        "interface_grafica.controllers.workers",
        raising=False,
    )
    workers_module = importlib.import_module("interface_grafica.controllers.workers")

    ok_worker = workers_module.PipelineWorker(
        SimpleNamespace(
            executar_completo=lambda *args, **kwargs: SimpleNamespace(ok=True, erros=[])
        ),
        "123",
        [],
        [],
    )
    ok_worker.finished_ok = SignalStub()
    ok_worker.failed = SignalStub()
    ok_worker.progress = SignalStub()
    ok_worker.run()

    bad_worker = workers_module.PipelineWorker(
        SimpleNamespace(
            executar_completo=lambda *args, **kwargs: SimpleNamespace(
                ok=False,
                erros=["falha"],
            )
        ),
        "123",
        [],
        [],
    )
    bad_worker.finished_ok = SignalStub()
    bad_worker.failed = SignalStub()
    bad_worker.progress = SignalStub()
    bad_worker.run()

    task_worker = workers_module.ServiceTaskWorker(
        lambda progresso=None: (progresso("ok"), "feito")[1]
    )
    task_worker.finished_ok = SignalStub()
    task_worker.failed = SignalStub()
    task_worker.progress = SimpleNamespace(emit=lambda value: None)
    task_worker.run()

    error_worker = workers_module.ServiceTaskWorker(
        lambda: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    error_worker.finished_ok = SignalStub()
    error_worker.failed = SignalStub()
    error_worker.progress = SignalStub()
    error_worker.run()

    assert len(ok_worker.finished_ok.values) == 1
    assert bad_worker.failed.values == ["falha"]
    assert task_worker.finished_ok.values == ["feito"]
    assert error_worker.failed.values
