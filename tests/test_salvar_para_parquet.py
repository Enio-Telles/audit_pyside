import logging
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from utilitarios.salvar_para_parquet import salvar_para_parquet


@pytest.fixture
def dummy_df():
    """Retorna um DataFrame Polars simples para testes."""
    return pl.DataFrame(
        {"id": [1, 2, 3], "nome": ["A", "B", "C"], "valor": [10.5, 20.0, 30.5]}
    )


def test_salvar_df_caminho_completo(dummy_df, tmp_path):
    """Testa salvar um DataFrame passando o caminho completo do arquivo."""
    arquivo_saida = tmp_path / "teste_completo.parquet"
    resultado = salvar_para_parquet(df=dummy_df, caminho_saida=arquivo_saida)

    assert resultado is True
    assert arquivo_saida.exists()

    df_lido = pl.read_parquet(arquivo_saida)
    assert df_lido.shape == (3, 3)
    assert df_lido.columns == ["id", "nome", "valor"]


def test_salvar_df_com_nome_arquivo(dummy_df, tmp_path):
    """Testa salvar um DataFrame passando o diretório e o nome do arquivo."""
    resultado = salvar_para_parquet(
        df=dummy_df, caminho_saida=tmp_path, nome_arquivo="teste_nome"
    )

    assert resultado is True
    arquivo_esperado = tmp_path / "teste_nome.parquet"
    assert arquivo_esperado.exists()

    df_lido = pl.read_parquet(arquivo_esperado)
    assert df_lido.shape == (3, 3)


def test_salvar_lazyframe(dummy_df, tmp_path):
    """Testa salvar um LazyFrame (deve fazer collect automaticamente)."""
    lazy_df = dummy_df.lazy()
    arquivo_saida = tmp_path / "teste_lazy.parquet"
    resultado = salvar_para_parquet(df=lazy_df, caminho_saida=arquivo_saida)

    assert resultado is True
    assert arquivo_saida.exists()


def test_salvar_df_vazio(tmp_path, caplog):
    """Testa salvar um DataFrame vazio e verifica se o aviso é logado."""
    df_vazio = pl.DataFrame(schema={"id": pl.Int64, "nome": pl.Utf8})
    arquivo_saida = tmp_path / "teste_vazio.parquet"

    with caplog.at_level(logging.WARNING, logger="utilitarios.salvar_para_parquet"):
        resultado = salvar_para_parquet(df=df_vazio, caminho_saida=arquivo_saida)

    assert resultado is True
    assert arquivo_saida.exists()
    assert any("esta vazio" in msg for msg in caplog.messages)


def test_salvar_com_schema(dummy_df, tmp_path):
    """Testa salvar impondo um schema do PyArrow."""
    # Convertendo id para float via schema pyarrow
    schema_pa = pa.schema(
        [("id", pa.float64()), ("nome", pa.string()), ("valor", pa.float64())]
    )
    arquivo_saida = tmp_path / "teste_schema.parquet"

    resultado = salvar_para_parquet(
        df=dummy_df, caminho_saida=arquivo_saida, schema=schema_pa
    )

    assert resultado is True
    assert arquivo_saida.exists()

    # Lendo o arquivo com pyarrow para verificar o schema
    table_lida = pq.read_table(arquivo_saida)
    assert table_lida.schema.field("id").type == pa.float64()


def test_salvar_com_schema_incompativel(dummy_df, tmp_path, caplog):
    """Testa o comportamento ao passar um schema que falha no cast."""
    # Tentando converter string para inteiro (deve falhar e cair no except do schema)
    schema_pa_invalido = pa.schema(
        [
            ("id", pa.int64()),
            ("nome", pa.int64()),  # Incompatível com string 'A', 'B', 'C'
            ("valor", pa.float64()),
        ]
    )
    arquivo_saida = tmp_path / "teste_schema_invalido.parquet"

    with caplog.at_level(logging.WARNING, logger="utilitarios.salvar_para_parquet"):
        resultado = salvar_para_parquet(
            df=dummy_df, caminho_saida=arquivo_saida, schema=schema_pa_invalido
        )

    assert resultado is True
    assert arquivo_saida.exists()
    assert any("Falha ao impor schema estrito" in msg for msg in caplog.messages)


def test_salvar_com_metadata(dummy_df, tmp_path):
    """Testa salvar um DataFrame adicionando metadados nas colunas."""
    metadata_dict = {"id": "Identificador unico", "valor": "Valor monetario"}
    arquivo_saida = tmp_path / "teste_metadata.parquet"

    resultado = salvar_para_parquet(
        df=dummy_df, caminho_saida=arquivo_saida, metadata=metadata_dict
    )

    assert resultado is True
    assert arquivo_saida.exists()

    # Ler a tabela com pyarrow para verificar os metadados
    table_lida = pq.read_table(arquivo_saida)

    # Verifica metadata da coluna 'id'
    id_field = table_lida.schema.field("id")
    assert b"description" in id_field.metadata
    assert id_field.metadata[b"description"] == b"Identificador unico"
    assert id_field.metadata[b"comment"] == b"Identificador unico"

    # Verifica metadata da coluna 'nome' (não deve ter)
    nome_field = table_lida.schema.field("nome")
    assert nome_field.metadata is None or b"description" not in nome_field.metadata


def test_criacao_diretorio_pai(dummy_df, tmp_path):
    """Testa se o diretório pai é criado caso não exista."""
    dir_novo = tmp_path / "pasta_nova" / "subpasta"
    arquivo_saida = dir_novo / "teste_dir.parquet"

    assert not dir_novo.exists()

    resultado = salvar_para_parquet(df=dummy_df, caminho_saida=arquivo_saida)

    assert resultado is True
    assert dir_novo.exists()
    assert arquivo_saida.exists()


def test_excecao_geral_ao_salvar(dummy_df, tmp_path, mocker, caplog):
    """Testa o tratamento de exceção ao tentar salvar o arquivo (e.g. erro de permissão)."""
    arquivo_saida = tmp_path / "teste_excecao.parquet"

    # Forçar um erro no método write_parquet do DataFrame
    mocker.patch.object(
        pl.DataFrame, "write_parquet", side_effect=PermissionError("Acesso negado")
    )

    with caplog.at_level(logging.ERROR, logger="utilitarios.salvar_para_parquet"):
        resultado = salvar_para_parquet(df=dummy_df, caminho_saida=arquivo_saida)

    assert resultado is False
    assert any("Acesso negado" in msg for msg in caplog.messages)
