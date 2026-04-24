from datetime import date
from decimal import Decimal
from pathlib import Path
from types import ModuleType, SimpleNamespace
import importlib
import sys

import polars as pl
import pytest

sys.path.insert(0, str(Path("src").resolve()))

from interface_grafica import logging_setup
from interface_grafica.services import pipeline_funcoes_service as pipe
from interface_grafica.services.parquet_service import FilterCondition, ParquetService
from utilitarios import sql_catalog as sqlcat
from utilitarios import text as txt


class Cursor:
    def __init__(self, rows, fail=False):
        self.rows, self.fail, self.description = list(rows), fail, [("COL_A",), ("COL_B",)]

    def __enter__(self): return self
    def __exit__(self, *_): return False
    def execute(self, *_):
        if self.fail: raise RuntimeError("boom")
    def fetchmany(self, _): return self.rows.pop(0) if self.rows else []


class OracleConn:
    def __init__(self, rows, fail=False): self.rows, self.fail = rows, fail
    def __enter__(self): return self
    def __exit__(self, *_): return False
    def cursor(self): return Cursor(self.rows, self.fail)


def test_logging_setup_is_idempotent_and_json(monkeypatch):
    seen = []
    monkeypatch.setattr(logging_setup, "_configured", False)
    monkeypatch.setenv("AUDIT_LOG_JSON", "1")
    monkeypatch.setattr(logging_setup.structlog, "configure", lambda **kw: seen.append(kw))
    monkeypatch.setattr(logging_setup.logging, "basicConfig", lambda **kw: seen.append(kw))
    logging_setup.configure_structlog("warning")
    logging_setup.configure_structlog("error")
    assert logging_setup._configured and len(seen) == 2 and seen[0]["processors"][-1].__class__.__name__ == "JSONRenderer"


def test_parquet_service_end_to_end(tmp_path: Path):
    base = tmp_path / "12345678901"
    for p in [base / "arquivos_parquet", base / "analises" / "produtos", base / "produtos"]: p.mkdir(parents=True, exist_ok=True)
    for n in ["ok.parquet", "x_produtos_y.parquet"]: (base / "arquivos_parquet" / n).write_text("x")
    for n in ["tb_documentos_1.parquet", "arquivo_invalido.parquet"]: (base / "analises" / "produtos" / n).write_text("x")
    (base / "produtos" / "mov_estoque_1.parquet").write_text("x")
    path = tmp_path / "dados.parquet"
    pl.DataFrame({"nome": ["b", "a", None], "valor": [2, 1, 3], "lista": [["x"], ["y"], []]}).write_parquet(path)
    svc = ParquetService(root=tmp_path)
    assert sorted(p.name for p in svc.list_parquet_files("12345678901")) == ["mov_estoque_1.parquet", "ok.parquet", "tb_documentos_1.parquet"]
    assert ParquetService._normalize_operator("contÃ©m") == "contem"
    assert svc.apply_filters(pl.DataFrame({"nome": ["A", "B"], "valor": [1, 2]}).lazy(), [FilterCondition("faltante", "igual", "x"), FilterCondition("nome", "igual", "")], {"nome": pl.String, "valor": pl.Int64}).collect().shape == (2, 2)
    page1 = svc.get_page(path, [FilterCondition("nome", "não é nulo", "")], ["nome"], 1, 2, "valor")
    assert page1.total_rows == 2 and page1.df_visible["nome"].to_list() == ["a", "b"] and svc.get_page(path, [FilterCondition("nome", "não é nulo", "")], ["nome"], 1, 2, "valor") is page1
    svc.save_dataset(path, pl.DataFrame({"id": [2], "nome": ["B"]}))
    assert svc.load_dataset(path, [FilterCondition("nome", "igual", "B")], ["id"])["id"].to_list() == [2]


def test_pipeline_funcoes_service_paths_binds_and_execucao(monkeypatch, tmp_path: Path):
    sql_path = tmp_path / "consulta.sql"; sql_path.write_text("select :CNPJ from dual")
    monkeypatch.setattr(pipe, "CNPJ_ROOT", tmp_path)
    monkeypatch.setattr(pipe, "resolve_sql_path", lambda item: sql_path if item == "consulta" else (_ for _ in ()).throw(FileNotFoundError()))
    monkeypatch.setattr(pipe, "ler_sql", lambda path: "select :cnpj from dual" if Path(path).suffix == ".sql" else None)
    monkeypatch.setattr(pipe, "obter_conexao_oracle", lambda: OracleConn([[("x", 1)], []]))
    svc = pipe.ServicoExtracao(cnpj_root=tmp_path, consultas_dir=tmp_path)
    assert pipe.ServicoExtracao.sanitizar_cnpj("12.345.678/0001-90") == "12345678000190"
    with pytest.raises(ValueError): pipe.ServicoExtracao.sanitizar_cnpj("123")
    assert pipe.ServicoExtracao.montar_binds("select :CNPJ, :data_limite_processamento, :X from dual", {"cnpj": "1", "DATA_LIMITE_PROCESSAMENTO": "2026-01-01"}) == {"CNPJ": "1", "data_limite_processamento": "2026-01-01", "X": None}
    assert svc.pasta_parquets("123").exists() and svc.pasta_produtos("123").exists()
    reg = tmp_path / "123" / "arquivos_parquet"; reg.mkdir(parents=True, exist_ok=True); pl.DataFrame({"data_entrega": [date(2026, 1, 5), date(2026, 3, 7)]}).write_parquet(reg / "reg_0000_123.parquet")
    assert svc.obter_data_entrega_reg0000("123") == "07/03/2026"
    analises, brutos = tmp_path / "123" / "analises" / "produtos", tmp_path / "123" / "arquivos_parquet"
    analises.mkdir(parents=True, exist_ok=True); brutos.mkdir(parents=True, exist_ok=True)
    for n in ["produtos_123.parquet", "produtos_final_123.parquet", "produtos_agrupados_123.parquet"]: (analises / n).write_text("x")
    for n in ["abc_produtos_123.parquet", "ok_123.parquet"]: (brutos / n).write_text("x")
    pipe.ServicoTabelas.limpar_arquivos_legados("123")
    assert sorted(p.name for p in analises.glob("*.parquet")) == ["produtos_agrupados_123.parquet", "produtos_final_123.parquet"]
    msgs = []; arquivos = svc.executar_consultas("12.345.678/0001-90", ["consulta"], progresso=msgs.append)
    assert len(arquivos) == 1 and Path(arquivos[0]).exists() and any("linhas lidas" in m for m in msgs)


def test_pipeline_funcoes_service_tabelas_and_pipeline(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(pipe, "CNPJ_ROOT", tmp_path)
    monkeypatch.setattr(pipe, "_importar_funcao_tabela", lambda *_: (lambda cnpj, pasta: True))
    msgs = []; ok = pipe.ServicoTabelas.gerar_tabelas("12.345.678/0001-90", ["tb_documentos", "itens"], msgs.append)
    monkeypatch.setattr(pipe, "_importar_funcao_tabela", lambda *_: (lambda cnpj, pasta: False))
    bad = pipe.ServicoTabelas.gerar_tabelas("12345678000190", ["tb_documentos"])
    full = pipe.ServicoPipelineCompleto()
    full.servico_extracao = SimpleNamespace(executar_consultas=lambda *args: ["a.parquet"])
    full.servico_tabelas = SimpleNamespace(gerar_tabelas=lambda *args: pipe.ResultadoGeracaoTabelas(ok=False, geradas=["itens"], erros=["falhou"], tempos={"itens": 1.5}))
    result = full.executar_completo("12.345.678/0001-90", ["x.sql"], ["itens"])
    assert ok.ok and ok.geradas == ["tb_documentos", "itens"] and any("OK" in m for m in msgs)
    assert not bad.ok and "retornou False" in bad.erros[0]
    assert not result.ok and result.cnpj == "12345678000190" and result.erros == ["falhou"]


def test_sql_catalog_and_text_helpers(tmp_path: Path, monkeypatch):
    sql_root, archive, ref, atom = tmp_path / "sql", tmp_path / "sql" / "archive", tmp_path / "sql" / "referencia", tmp_path / "sql" / "arquivos_parquet"
    for p in [archive, ref, atom, sql_root / "grupo"]: p.mkdir(parents=True, exist_ok=True)
    for p in [sql_root / "grupo" / "b.sql", sql_root / "grupo" / "a.sql", archive / "old.sql", ref / "ref.sql", atom / "x.sql"]: p.write_text("select 1")
    monkeypatch.setattr(sqlcat, "SQL_ROOT", sql_root); monkeypatch.setattr(sqlcat, "SQL_ARCHIVE_ROOT", archive); monkeypatch.setattr(sqlcat, "_SQL_ARQUIVOS_PARQUET_ROOT", atom); sqlcat._index_entries.cache_clear()
    entry = sqlcat.SqlCatalogEntry("grupo/a.sql", sql_root / "grupo" / "a.sql")
    assert [i.sql_id for i in sqlcat.list_sql_entries()] == ["grupo/a.sql", "grupo/b.sql"] and entry.display_name == "a" and entry.source_label == "grupo"
    assert sqlcat.resolve_sql_path("a.sql").name == "a.sql" and sqlcat.migrate_sql_id_list(["a.sql", "grupo/a.sql", "missing.sql"]) == ["grupo/a.sql"]
    with pytest.raises(FileNotFoundError): sqlcat.resolve_sql_path("inexistente.sql")
    assert txt.remove_accents("ação") == "acao" and txt.normalize_text("Produto de Açúcar e Mel") == "PRODUTO ACUCAR MEL" and txt.normalize_desc("  prod.-a/ç  ") == "PROD. A C"
    assert sorted(["item10", "item2", "item1"], key=txt.natural_sort_key) == ["item1", "item2", "item10"] and txt.is_year_column_name("ano_base") and not txt.is_year_column_name("mes")
    assert txt.display_cell(True) == "true" and txt.display_cell(date(2025, 1, 2)) == "02/01/2025" and txt.display_cell(Decimal("12.50")) == "12,50" and txt.display_cell(float("inf")) == "" and txt.display_cell(["a", None, "b"]) == "a, b"
    assert txt.display_cell("2025", column_name="ano") == "2025" and txt.display_cell("2025-01-02T03:04") == "02/01/2025 03:04:00" and txt._parse_data_iso("") is None


def test_query_worker_success_cancel_and_error(monkeypatch):
    class Sig:
        def __init__(self, *_a, **_k): self.values = []
        def emit(self, v=None): self.values.append(v)
    class QThread:
        def __init__(self, *_a, **_k): self._cancel = False
        def isInterruptionRequested(self): return self._cancel
    class QCursor(Cursor): pass
    class QConn:
        def __init__(self, rows, fail=False): self.rows, self.fail, self.cancelled = rows, fail, False
        def cursor(self): return QCursor(self.rows, self.fail)
        def cancel(self): self.cancelled = True
        def close(self): return None
    qtcore = sys.modules.setdefault("PySide6.QtCore", ModuleType("PySide6.QtCore")); qtcore.QThread = QThread; qtcore.Signal = Sig
    aux = sys.modules.setdefault("transformacao.auxiliares.logs", ModuleType("transformacao.auxiliares.logs")); aux.log_exception = lambda exc: None
    sys.modules.pop("interface_grafica.services.query_worker", None)
    qmod = importlib.import_module("interface_grafica.services.query_worker")
    monkeypatch.setattr(qmod, "registrar_evento_performance", lambda *a, **k: None)
    for conn, cancelled, expect_ok in [(QConn([[("x", 1)], []]), False, True), (QConn([[("x", 1)], []]), True, False), (QConn([], True), False, False)]:
        monkeypatch.setattr(qmod, "conectar_oracle", lambda conn=conn: conn)
        worker = qmod.QueryWorker("select 1", {"a": 1}, fetch_size=2); worker.progress = Sig(); worker.finished_ok = Sig(); worker.failed = Sig(); worker._cancel = cancelled; worker.run()
        if expect_ok:
            assert worker.finished_ok.values[0].shape == (1, 2)
        else:
            assert worker.failed.values
