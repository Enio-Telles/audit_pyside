from pathlib import Path
from types import ModuleType, SimpleNamespace
import importlib
import sys
import types

import polars as pl
from openpyxl import Workbook

sys.path.insert(0, str(Path("src").resolve()))

qtwidgets = sys.modules.setdefault("PySide6.QtWidgets", types.ModuleType("PySide6.QtWidgets"))
qtwidgets.QInputDialog = getattr(qtwidgets, "QInputDialog", object)
qtwidgets.QMessageBox = getattr(qtwidgets, "QMessageBox", object)
qtwidgets.QFileDialog = getattr(qtwidgets, "QFileDialog", object)
placeholder = sys.modules.setdefault("interface_grafica.controllers.workers", ModuleType("interface_grafica.controllers.workers"))
placeholder.ServiceTaskWorker = getattr(placeholder, "ServiceTaskWorker", object)

from interface_grafica.controllers.agregacao_controller import AgregacaoControllerMixin
from interface_grafica.controllers import relatorios_periodos_controller as per
from interface_grafica.controllers.relatorios_resumo_controller import RelatoriosResumoControllerMixin


class T:
    def __init__(self, v=""): self.value = v
    def text(self): return self.value
    def clear(self): self.value = ""
    def currentText(self): return self.value
    def setCurrentText(self, v): self.value = v
    def setCurrentIndex(self, _): self.value = ""
    def setText(self, v): self.value = v


class M:
    def __init__(self): self.dataframe = self.df_filtered = pl.DataFrame()
    def set_dataframe(self, df): self.dataframe = self.df_filtered = df
    def get_checked_rows(self): return []
    def clear_checked(self): return None


class Agg(AgregacaoControllerMixin): pass


class Gui(per.RelatoriosPeriodosControllerMixin, RelatoriosResumoControllerMixin):
    def __init__(self):
        self.state = SimpleNamespace(current_cnpj="12345678000190")
        self.results_table_model = self.aba_periodos_model = self.aba_mensal_model = self.aba_anual_model = self.resumo_global_model = M()
        self.results_table = SimpleNamespace(selectionModel=lambda: None)
        self.lbl_aba_periodos_status = self.lbl_aba_periodos_filtros = self.lbl_aba_mensal_status = self.lbl_aba_mensal_filtros = self.lbl_aba_anual_status = self.lbl_aba_anual_filtros = self.lbl_resumo_global_status = T("")
        for name in ["periodo_filter_id","periodo_filter_desc","periodo_filter_texto","periodo_filter_num_col","periodo_filter_num_min","periodo_filter_num_max","mensal_filter_id","mensal_filter_desc","mensal_filter_ano","mensal_filter_mes","mensal_filter_texto","mensal_filter_num_col","mensal_filter_num_min","mensal_filter_num_max","anual_filter_id","anual_filter_desc","anual_filter_ano","anual_filter_texto","anual_filter_num_col","anual_filter_num_min","anual_filter_num_max"]:
            setattr(self, name, T("Todos" if name.endswith(("_ano","_mes")) else ("valor" if name.endswith("_num_col") else "")))
        self._aba_periodos_df = self._aba_mensal_df = self._aba_anual_df = self._resumo_global_df = pl.DataFrame()
        self._filtro_cruzado_anuais_ids = []
        self.aba_mensal_table = self.aba_anual_table = object()

    def _filtrar_texto_em_colunas(self, df, texto):
        expr = pl.lit(False)
        for c in [c for c in df.columns if df[c].dtype == pl.String]:
            expr |= pl.col(c).cast(pl.Utf8, strict=False).fill_null("").str.to_lowercase().str.contains(texto, literal=True)
        return df.filter(expr)
    def _filtrar_intervalo_numerico(self, df, col, vmin, vmax):
        if col and vmin: df = df.filter(pl.col(col) >= float(vmin))
        if col and vmax: df = df.filter(pl.col(col) <= float(vmax))
        return df
    def _formatar_resumo_filtros(self, pares): return ", ".join(f"{k}={v}" for k, v in pares if v) or "nenhum"
    def _carregar_dados_parquet_async(self, path, cb, *_a, unique_cols=None):
        df = pl.read_parquet(path); cb(df, {c: df.get_column(c).drop_nulls().unique().to_list() for c in unique_cols or []})
    def _popular_combo_texto(self, combo, values, *_): combo.values = values
    def _reset_table_resize_flag(self, *_): return None
    def _resize_table_once(self, *_): return None
    def _aplicar_preferencias_tabela(self, *_): return False
    def _aplicar_ordenacao_padrao(self, *_): return None
    def _aplicar_preset_aba_mensal(self): return None
    def _aplicar_preset_aba_anual(self): return None
    def _atualizar_titulo_aba_periodos(self, *_): return None
    def _atualizar_titulo_aba_mensal(self, *_): return None
    def _atualizar_titulo_aba_anual(self, *_): return None
    def _salvar_preferencias_tabela(self, *_): return None
    def atualizar_aba_produtos_selecionados(self): return None
    def _garantir_resumos_estoque_atualizados(self, *_): return True
    def _limpar_aba_resumo_estoque(self, *_): return None
    def show_error(self, *_): return None
    def atualizar_aba_mensal(self): self._aba_mensal_df = pl.DataFrame({"ano":[2025],"mes":[1],"ICMS_entr_desacob":[1.0]})
    def atualizar_aba_anual(self): self._aba_anual_df = pl.DataFrame({"ano":[2025],"ICMS_saidas_desac":[2.0],"ICMS_estoque_desac":[3.0]})


def test_agregacao_helpers_cover_key_paths():
    ctrl = Agg()
    ctrl.results_table_model = SimpleNamespace(get_checked_rows=lambda: [{"id_agrupado": "A"}, {"id_agrupado": "A"}, {"id_agrupado": "B"}], get_dataframe=lambda: pl.DataFrame())
    ctrl.results_table = SimpleNamespace(selectionModel=lambda: None)
    df = pl.DataFrame({"ncm_padrao": ["1", "1", "2"], "cest_padrao": ["A", "A", "B"], "gtin_padrao": ["X", "X", "Z"], "descricao": ["p1", "p2", "p3"]})
    assert ctrl._obter_ids_agrupados_para_reversao() == ["A", "B"]
    assert ctrl._resolver_coluna_agregacao(["descricao"], ["descrição", "ncm"]) == "descrição"
    assert ctrl._aplicar_modo_relacional_agregacao_df(df, "ncm_cest_gtin")["descricao"].to_list() == ["p1", "p2"]


def test_periodos_mensal_anual_and_resumo(monkeypatch, tmp_path: Path):
    ctrl = Gui(); monkeypatch.setattr(per, "CNPJ_ROOT", tmp_path); monkeypatch.setattr("interface_grafica.controllers.relatorios_resumo_controller.CNPJ_ROOT", tmp_path)
    ctrl._reprocessar_periodos_auto = lambda cnpj: setattr(ctrl, "reprocessado", cnpj)
    ctrl.atualizar_aba_periodos(); assert ctrl.reprocessado == "12345678000190"
    pasta = tmp_path / ctrl.state.current_cnpj / "analises" / "produtos"; pasta.mkdir(parents=True)
    pl.DataFrame({"id_agrupado": ["A"], "valor": [1.0]}).write_parquet(pasta / f"aba_periodos_{ctrl.state.current_cnpj}.parquet")
    ctrl.atualizar_aba_periodos(); assert ctrl.aba_periodos_model.dataframe["id_agrupado"].to_list() == ["A"]
    ctrl._aba_periodos_df = pl.DataFrame({"id_agrupado": ["A", "B"], "descr_padrao": ["Produto bom", "Outro"], "texto": ["abc", "zzz"], "valor": [10.0, 1.0]})
    ctrl.periodo_filter_id.value, ctrl.periodo_filter_desc.value, ctrl.periodo_filter_texto.value, ctrl.periodo_filter_num_min.value = "A", "produto", "abc", "5"
    ctrl.aplicar_filtros_aba_periodos(); assert ctrl.aba_periodos_model.dataframe["id_agrupado"].to_list() == ["A"]
    ctrl._aba_mensal_df = pl.DataFrame({"id_agregado": ["A", "B"], "descr_padrao": ["Produto bom", "Outro"], "ano": [2025, 2024], "mes": [1, 2], "texto": ["abc", "zzz"], "valor": [10.0, 1.0]})
    ctrl.mensal_filter_id.value, ctrl.mensal_filter_desc.value, ctrl.mensal_filter_ano.value, ctrl.mensal_filter_num_min.value = "A", "produto", "2025", "5"
    ctrl.aplicar_filtros_aba_mensal(); assert ctrl.aba_mensal_model.dataframe["id_agregado"].to_list() == ["A"]
    ctrl._aba_anual_df = pl.DataFrame({"id_agregado": ["A", "B"], "descr_padrao": ["Produto bom", "Outro"], "ano": [2025, 2025], "texto": ["abc", "zzz"], "valor": [10.0, 1.0]})
    ctrl._filtro_cruzado_anuais_ids, ctrl.anual_filter_desc.value, ctrl.anual_filter_num_min.value = ["A"], "produto", "5"
    ctrl.aplicar_filtros_aba_anual(); assert ctrl.aba_anual_model.dataframe["id_agregado"].to_list() == ["A"]
    ws = Workbook().active; ctrl._escrever_planilha_openpyxl(ws, pl.DataFrame({"ano":[2025],"valor":[12.5]})); assert ws["A2"].number_format == "0" and ws["B2"].number_format == "#,##0.00"
    pl.DataFrame({"Ano/Mes": ["2025-01"], "Total": [1.0]}).write_parquet(pasta / f"aba_resumo_global_{ctrl.state.current_cnpj}.parquet")
    ctrl.atualizar_aba_resumo_global(); assert ctrl._resumo_global_df["Ano/Mes"].to_list() == ["2025-01"]


def test_resumo_global_helpers_cover_empty_and_consolidated(monkeypatch, tmp_path: Path):
    ctrl = Gui(); monkeypatch.setattr("interface_grafica.controllers.relatorios_resumo_controller.CNPJ_ROOT", tmp_path)
    vazio = ctrl._gerar_resumo_global(pl.DataFrame(), pl.DataFrame(), [])
    assert vazio.columns == ["Ano/Mes", "ICMS_entr_desacob", "ICMS_saidas_desac", "ICMS_estoque_desac", "Total"] and vazio.height == 0
    ctrl._aba_mensal_df = pl.DataFrame({"ano":[2025, 2025], "mes":[1, 2], "ICMS_entr_desacob":[10.0, 5.0]})
    ctrl._aba_anual_df = pl.DataFrame({"ano":[2025], "ICMS_saidas_desac":[7.0], "ICMS_estoque_desac":[3.0]})
    resumo = ctrl._gerar_resumo_global(ctrl._aba_mensal_df, ctrl._aba_anual_df, [2025])
    assert resumo.filter(pl.col("Ano/Mes") == "2025-12")["Total"].item() == 10.0
    ctrl.atualizar_aba_resumo_global()
    assert ctrl._resumo_global_df.filter(pl.col("Ano/Mes") == "2025-01")["ICMS_entr_desacob"].item() == 10.0


def test_resumo_global_cover_branches_sem_parquet(monkeypatch, tmp_path: Path):
    ctrl = Gui(); monkeypatch.setattr("interface_grafica.controllers.relatorios_resumo_controller.CNPJ_ROOT", tmp_path)
    ctrl.atualizar_aba_resumo_global()
    assert "Aguarde o processamento" in ctrl.lbl_resumo_global_status.value
    pasta = tmp_path / ctrl.state.current_cnpj / "analises" / "produtos"; pasta.mkdir(parents=True)
    (pasta / f"aba_mensal_{ctrl.state.current_cnpj}.parquet").write_text("x")
    (pasta / f"aba_anual_{ctrl.state.current_cnpj}.parquet").write_text("x")
    ctrl._aba_mensal_df = ctrl._aba_anual_df = pl.DataFrame()
    ctrl.atualizar_aba_resumo_global()
    assert "Carregando depend" in ctrl.lbl_resumo_global_status.value


def test_workers_and_query_worker_threads():
    class Sig:
        def __init__(self, *_a, **_k): self.values = []
        def emit(self, v=None): self.values.append(v)
    class QThread:
        def __init__(self, *_a, **_k): self._cancel = False
        def isInterruptionRequested(self): return self._cancel
    qtcore = sys.modules.setdefault("PySide6.QtCore", ModuleType("PySide6.QtCore")); qtcore.QThread = QThread; qtcore.Signal = Sig
    sys.modules.pop("interface_grafica.controllers.workers", None); wmod = importlib.import_module("interface_grafica.controllers.workers")
    ok = wmod.PipelineWorker(SimpleNamespace(executar_completo=lambda *a, **k: SimpleNamespace(ok=True, erros=[])), "123", [], []); ok.finished_ok = Sig(); ok.failed = Sig(); ok.progress = Sig(); ok.run()
    bad = wmod.PipelineWorker(SimpleNamespace(executar_completo=lambda *a, **k: SimpleNamespace(ok=False, erros=["falha"])), "123", [], []); bad.finished_ok = Sig(); bad.failed = Sig(); bad.progress = Sig(); bad.run()
    task = wmod.ServiceTaskWorker(lambda progresso=None: (progresso("ok"), "feito")[1]); task.finished_ok = Sig(); task.failed = Sig(); task.progress = SimpleNamespace(emit=lambda v: None); task.run()
    err = wmod.ServiceTaskWorker(lambda: (_ for _ in ()).throw(RuntimeError("boom"))); err.finished_ok = Sig(); err.failed = Sig(); err.progress = Sig(); err.run()
    assert len(ok.finished_ok.values) == 1 and bad.failed.values == ["falha"] and task.finished_ok.values == ["feito"] and err.failed.values
