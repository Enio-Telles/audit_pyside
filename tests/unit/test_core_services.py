from __future__ import annotations

import importlib
import sys
from collections.abc import Sequence
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import polars as pl
import pytest


SRC = Path(__file__).resolve().parents[2] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from interface_grafica import logging_setup
from interface_grafica.services import pipeline_funcoes_service as pipe
from interface_grafica.services.parquet_service import FilterCondition, ParquetService
from utilitarios import sql_catalog as sqlcat
from utilitarios import text as txt


class Cursor:
    rows: list[Sequence[Any]]
    fail: bool
    description: list[tuple[str]]

    def __init__(self, rows: Sequence[Sequence[Any]], fail: bool = False) -> None:
        self.rows = list(rows)
        self.fail = fail
        self.description = [("COL_A",), ("COL_B",)]

    def __enter__(self) -> Cursor:
        return self

    def __exit__(self, *_args: Any) -> bool:
        return False

    def execute(self, *_args: Any) -> None:
        if self.fail:
            raise RuntimeError("boom")

    def fetchmany(self, _size: int) -> Sequence[Any]:
        return self.rows.pop(0) if self.rows else []


class OracleConn:
    rows: Sequence[Sequence[Any]]
    fail: bool

    def __init__(self, rows: Sequence[Sequence[Any]], fail: bool = False) -> None:
        self.rows = rows
        self.fail = fail

    def __enter__(self) -> OracleConn:
        return self

    def __exit__(self, *_args: Any) -> bool:
        return False

    def cursor(self) -> Cursor:
        return Cursor(self.rows, self.fail)


def test_logging_setup_is_idempotent_and_json(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[dict[str, Any]] = []
    monkeypatch.setattr(logging_setup, "_configured", False)
    monkeypatch.setenv("AUDIT_LOG_JSON", "1")
    monkeypatch.setattr(
        logging_setup.structlog,
        "configure",
        lambda **kwargs: seen.append(kwargs),
    )
    monkeypatch.setattr(
        logging_setup.logging,
        "basicConfig",
        lambda **kwargs: seen.append(kwargs),
    )
    logging_setup.configure_structlog("warning")
    logging_setup.configure_structlog("error")

    assert logging_setup._configured
    assert len(seen) == 2
    assert seen[0]["processors"][-1].__class__.__name__ == "JSONRenderer"


def test_parquet_service_end_to_end(tmp_path: Path) -> None:
    base = tmp_path / "12345678901"
    for path in [
        base / "arquivos_parquet",
        base / "analises" / "produtos",
        base / "produtos",
    ]:
        path.mkdir(parents=True, exist_ok=True)

    for name in ["ok.parquet", "x_produtos_y.parquet"]:
        (base / "arquivos_parquet" / name).write_text("x")
    for name in ["tb_documentos_1.parquet", "arquivo_invalido.parquet"]:
        (base / "analises" / "produtos" / name).write_text("x")
    (base / "produtos" / "mov_estoque_1.parquet").write_text("x")

    path = tmp_path / "dados.parquet"
    pl.DataFrame(
        {"nome": ["b", "a", None], "valor": [2, 1, 3], "lista": [["x"], ["y"], []]}
    ).write_parquet(path)
    service = ParquetService(root=tmp_path)

    assert sorted(
        parquet.name for parquet in service.list_parquet_files("12345678901")
    ) == ["mov_estoque_1.parquet", "ok.parquet", "tb_documentos_1.parquet"]
    assert ParquetService._normalize_operator("contÃƒÂ©m") == "contem"
    assert service.apply_filters(
        pl.DataFrame({"nome": ["A", "B"], "valor": [1, 2]}).lazy(),
        [
            FilterCondition("faltante", "igual", "x"),
            FilterCondition("nome", "igual", ""),
        ],
        {"nome": pl.String, "valor": pl.Int64},
    ).collect().shape == (2, 2)

    page1 = service.get_page(
        path,
        [FilterCondition("nome", "não é nulo", "")],
        ["nome"],
        1,
        2,
        "valor",
    )
    assert page1.total_rows == 2
    assert page1.df_visible["nome"].to_list() == ["a", "b"]
    assert (
        service.get_page(
            path,
            [FilterCondition("nome", "não é nulo", "")],
            ["nome"],
            1,
            2,
            "valor",
        )
        is page1
    )

    service.save_dataset(path, pl.DataFrame({"id": [2], "nome": ["B"]}))
    assert service.load_dataset(
        path,
        [FilterCondition("nome", "igual", "B")],
        ["id"],
    )["id"].to_list() == [2]


def test_pipeline_funcoes_service_paths_binds_and_execucao(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    sql_path = tmp_path / "consulta.sql"
    sql_path.write_text("select :CNPJ from dual")
    monkeypatch.setattr(pipe, "CNPJ_ROOT", tmp_path)
    monkeypatch.setattr(
        pipe,
        "resolve_sql_path",
        lambda item: (
            sql_path
            if item == "consulta"
            else (_ for _ in ()).throw(FileNotFoundError())
        ),
    )
    monkeypatch.setattr(
        pipe,
        "ler_sql",
        lambda path: "select :cnpj from dual" if Path(path).suffix == ".sql" else None,
    )
    monkeypatch.setattr(pipe, "obter_conexao_oracle", lambda: OracleConn([[("x", 1)], []]))

    service = pipe.ServicoExtracao(cnpj_root=tmp_path, consultas_dir=tmp_path)
    assert pipe.ServicoExtracao.sanitizar_cnpj("12.345.678/0001-90") == "12345678000190"
    with pytest.raises(ValueError):
        pipe.ServicoExtracao.sanitizar_cnpj("123")

    assert pipe.ServicoExtracao.montar_binds(
        "select :CNPJ, :data_limite_processamento, :X from dual",
        {"cnpj": "1", "DATA_LIMITE_PROCESSAMENTO": "2026-01-01"},
    ) == {"CNPJ": "1", "data_limite_processamento": "2026-01-01", "X": None}
    assert service.pasta_parquets("123").exists()
    assert service.pasta_produtos("123").exists()

    reg = tmp_path / "123" / "arquivos_parquet"
    reg.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {"data_entrega": [date(2026, 1, 5), date(2026, 3, 7)]}
    ).write_parquet(reg / "reg_0000_123.parquet")
    assert service.obter_data_entrega_reg0000("123") == "07/03/2026"

    analyses = tmp_path / "123" / "analises" / "produtos"
    raw = tmp_path / "123" / "arquivos_parquet"
    analyses.mkdir(parents=True, exist_ok=True)
    raw.mkdir(parents=True, exist_ok=True)
    for name in [
        "produtos_123.parquet",
        "produtos_final_123.parquet",
        "produtos_agrupados_123.parquet",
    ]:
        (analyses / name).write_text("x")
    for name in ["abc_produtos_123.parquet", "ok_123.parquet"]:
        (raw / name).write_text("x")
    pipe.ServicoTabelas.limpar_arquivos_legados("123")
    assert sorted(path.name for path in analyses.glob("*.parquet")) == [
        "produtos_agrupados_123.parquet",
        "produtos_final_123.parquet",
    ]

    messages: list[str] = []
    files = service.executar_consultas(
        "12.345.678/0001-90",
        ["consulta"],
        progresso=messages.append,
    )
    assert len(files) == 1
    assert Path(files[0]).exists()
    assert any("linhas lidas" in message for message in messages)


def test_pipeline_funcoes_service_tabelas_and_pipeline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(pipe, "CNPJ_ROOT", tmp_path)
    monkeypatch.setattr(pipe, "_importar_funcao_tabela", lambda *_args: (lambda cnpj, pasta: True))
    messages: list[str] = []
    ok = pipe.ServicoTabelas.gerar_tabelas(
        "12.345.678/0001-90",
        ["tb_documentos", "itens"],
        messages.append,
    )

    monkeypatch.setattr(
        pipe,
        "_importar_funcao_tabela",
        lambda *_args: (lambda cnpj, pasta: False),
    )
    bad = pipe.ServicoTabelas.gerar_tabelas("12345678000190", ["tb_documentos"])
    full = pipe.ServicoPipelineCompleto()
    full.servico_extracao = SimpleNamespace(executar_consultas=lambda *args: ["a.parquet"])
    full.servico_tabelas = SimpleNamespace(
        gerar_tabelas=lambda *args: pipe.ResultadoGeracaoTabelas(
            ok=False,
            geradas=["itens"],
            erros=["falhou"],
            tempos={"itens": 1.5},
        )
    )
    result = full.executar_completo("12.345.678/0001-90", ["x.sql"], ["itens"])

    assert ok.ok
    assert ok.geradas == ["tb_documentos", "itens"]
    assert any("OK" in message for message in messages)
    assert not bad.ok
    assert "retornou False" in bad.erros[0]
    assert not result.ok
    assert result.cnpj == "12345678000190"
    assert result.erros == ["falhou"]


def test_sql_catalog_and_text_helpers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sql_root = tmp_path / "sql"
    archive = sql_root / "archive"
    reference = sql_root / "referencia"
    atomized = sql_root / "arquivos_parquet"
    for path in [archive, reference, atomized, sql_root / "grupo"]:
        path.mkdir(parents=True, exist_ok=True)

    for path in [
        sql_root / "grupo" / "b.sql",
        sql_root / "grupo" / "a.sql",
        archive / "old.sql",
        reference / "ref.sql",
        atomized / "x.sql",
    ]:
        path.write_text("select 1")

    monkeypatch.setattr(sqlcat, "SQL_ROOT", sql_root)
    monkeypatch.setattr(sqlcat, "SQL_ARCHIVE_ROOT", archive)
    monkeypatch.setattr(sqlcat, "_SQL_ARQUIVOS_PARQUET_ROOT", atomized)
    sqlcat._index_entries.cache_clear()

    entry = sqlcat.SqlCatalogEntry("grupo/a.sql", sql_root / "grupo" / "a.sql")
    assert [item.sql_id for item in sqlcat.list_sql_entries()] == ["grupo/a.sql", "grupo/b.sql"]
    assert entry.display_name == "a"
    assert entry.source_label == "grupo"
    assert sqlcat.resolve_sql_path("a.sql").name == "a.sql"
    assert sqlcat.migrate_sql_id_list(["a.sql", "grupo/a.sql", "missing.sql"]) == [
        "grupo/a.sql"
    ]
    with pytest.raises(FileNotFoundError):
        sqlcat.resolve_sql_path("inexistente.sql")

    assert txt.remove_accents("ação") == "acao"
    assert txt.normalize_text("Produto de Açúcar e Mel") == "PRODUTO ACUCAR MEL"
    assert txt.normalize_desc("  prod.-a/ç  ") == "PROD. A C"
    assert sorted(
        ["item10", "item2", "item1"],
        key=txt.natural_sort_key,
    ) == ["item1", "item2", "item10"]
    assert txt.is_year_column_name("ano_base")
    assert not txt.is_year_column_name("mes")
    assert txt.display_cell(True) == "true"
    assert txt.display_cell(date(2025, 1, 2)) == "02/01/2025"
    assert txt.display_cell(Decimal("12.50")) == "12,50"
    assert txt.display_cell(float("inf")) == ""
    assert txt.display_cell(["a", None, "b"]) == "a, b"
    assert txt.display_cell("2025", column_name="ano") == "2025"
    assert txt.display_cell("2025-01-02T03:04") == "02/01/2025 03:04:00"
    assert txt._parse_data_iso("") is None


def test_query_worker_success_cancel_and_error(
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

    class QueryCursor(Cursor):
        pass

    class QueryConn:
        def __init__(self, rows: Sequence[Sequence[Any]], fail: bool = False) -> None:
            self.rows = rows
            self.fail = fail
            self.cancelled = False

        def cursor(self) -> QueryCursor:
            return QueryCursor(self.rows, self.fail)

        def cancel(self) -> None:
            self.cancelled = True

        def close(self) -> None:
            return None

    qtcore = ModuleType("PySide6.QtCore")
    qtcore.QThread = ThreadStub
    qtcore.Signal = SignalStub
    monkeypatch.setitem(sys.modules, "PySide6.QtCore", qtcore)

    aux = ModuleType("transformacao.auxiliares.logs")
    aux.log_exception = lambda exc: None
    monkeypatch.setitem(sys.modules, "transformacao.auxiliares.logs", aux)
    monkeypatch.delitem(
        sys.modules,
        "interface_grafica.services.query_worker",
        raising=False,
    )
    query_module = importlib.import_module("interface_grafica.services.query_worker")
    monkeypatch.setattr(
        query_module,
        "registrar_evento_performance",
        lambda *args, **kwargs: None,
    )

    cases = [
        (QueryConn([[("x", 1)], []]), False, True),
        (QueryConn([[("x", 1)], []]), True, False),
        (QueryConn([], True), False, False),
    ]
    for conn, cancelled, expect_ok in cases:
        monkeypatch.setattr(query_module, "conectar_oracle", lambda conn=conn: conn)
        worker = query_module.QueryWorker("select 1", {"a": 1}, fetch_size=2)
        worker.progress = SignalStub()
        worker.finished_ok = SignalStub()
        worker.failed = SignalStub()
        worker._cancel = cancelled
        worker.run()
        if expect_ok:
            assert worker.finished_ok.values[0].shape == (1, 2)
        else:
            assert worker.failed.values
