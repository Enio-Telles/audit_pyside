"""Testes unitários de utilidades diversas não cobertas por outros arquivos."""
from __future__ import annotations

import pathlib

import polars as pl
import pytest

from utilitarios.calculos_compartilhados import resolver_ref
from utilitarios.codigo_fonte import expr_gerar_codigo_fonte
from utilitarios.ler_sql import ler_sql
from utilitarios.validacao_schema import garantir_tipos_compativeis
from utilitarios.validar_cnpj import validar_cnpj
from utilitarios import sql_catalog


# ---------------------------------------------------------------------------
# validar_cnpj — branches ausentes
# ---------------------------------------------------------------------------


def test_validar_cnpj_tamanho_short() -> None:
    assert validar_cnpj("123") is False


def test_validar_cnpj_primeiro_digito_errado() -> None:
    # 11222333000191 → primeiro digito calculado seria 8, não 9
    assert validar_cnpj("11222333000191") is False


def test_validar_cnpj_segundo_digito_zero() -> None:
    # 04252011000110 → segundo dígito calculado é 0 (resultado < 2)
    assert validar_cnpj("04252011000110") is True


# ---------------------------------------------------------------------------
# calculos_compartilhados.resolver_ref — return None
# ---------------------------------------------------------------------------


def test_resolver_ref_arquivo_inexistente() -> None:
    assert resolver_ref("arquivo_que_nao_existe.parquet") is None


# ---------------------------------------------------------------------------
# validacao_schema.garantir_tipos_compativeis — return df sem modificação
# ---------------------------------------------------------------------------


def test_garantir_tipos_ja_correto() -> None:
    df = pl.DataFrame({"val": [1.0, 2.0]})
    result = garantir_tipos_compativeis(df, {"val": pl.Float64})
    assert result.schema["val"] == pl.Float64


# ---------------------------------------------------------------------------
# codigo_fonte.expr_gerar_codigo_fonte — corpo da função
# ---------------------------------------------------------------------------


def test_expr_gerar_codigo_fonte_aplica() -> None:
    expr = expr_gerar_codigo_fonte("cnpj", "codigo")
    df = pl.DataFrame({"cnpj": ["12345678000190", "", None], "codigo": ["X1", "Y2", "Z3"]})
    result = df.with_columns(expr)
    assert "codigo_fonte" in result.columns
    assert result["codigo_fonte"][0] == "12345678000190|X1"
    assert result["codigo_fonte"][1] == "Y2"


# ---------------------------------------------------------------------------
# ler_sql — except Exception genérico + raise fatal
# ---------------------------------------------------------------------------


def test_ler_sql_erro_generico_levanta_excecao_fatal(tmp_path, monkeypatch) -> None:
    sql_file = tmp_path / "consulta.sql"
    sql_file.write_text("SELECT 1")

    def _sempre_falha(self, *args, **kwargs):  # noqa: ANN001
        raise IOError("falha simulada")

    monkeypatch.setattr(pathlib.Path, "read_text", _sempre_falha)
    with pytest.raises(Exception, match="ERRO FATAL"):
        ler_sql(sql_file)


# ---------------------------------------------------------------------------
# sql_catalog._iter_sql_paths — return [] quando SQL_ROOT inexiste
# ---------------------------------------------------------------------------


def test_iter_sql_paths_sem_sql_root(monkeypatch, tmp_path) -> None:
    fake_root = tmp_path / "nao_existe" / "sql"
    monkeypatch.setattr(sql_catalog, "SQL_ROOT", fake_root)
    result = list(sql_catalog._iter_sql_paths())
    assert result == []
