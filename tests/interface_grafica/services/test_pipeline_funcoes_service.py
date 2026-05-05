from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest
import polars as pl

from interface_grafica.services.pipeline_funcoes_service import (
    _parquet_valido_simples,
    ServicoExtracao,
    ServicoTabelas,
    ServicoPipelineCompleto,
    ResultadoPipeline,
    ResultadoGeracaoTabelas,
)

def test_parquet_valido_simples_nao_existe(tmp_path: Path):
    assert _parquet_valido_simples(tmp_path / "nao_existe.parquet") is False

def test_parquet_valido_simples_vazio(tmp_path: Path):
    vazio = tmp_path / "vazio.parquet"
    vazio.touch()
    assert _parquet_valido_simples(vazio) is False

def test_parquet_valido_simples_valido(tmp_path: Path):
    df = pl.DataFrame({"a": [1, 2, 3]})
    valido = tmp_path / "valido.parquet"
    df.write_parquet(valido)
    assert _parquet_valido_simples(valido) is True

def test_parquet_valido_simples_invalido(tmp_path: Path):
    invalido = tmp_path / "invalido.parquet"
    invalido.write_text("nao e um parquet")
    assert _parquet_valido_simples(invalido) is False

def test_sanitizar_cnpj():
    assert ServicoExtracao.sanitizar_cnpj("12.345.678/0001-90") == "12345678000190"
    assert ServicoExtracao.sanitizar_cnpj("12345678000190") == "12345678000190"

def test_montar_binds():
    servico = ServicoExtracao()
    sql_text = "SELECT * FROM tabela WHERE cnpj = :cnpj AND data = :data_limite_processamento AND outro = :nao_existe"
    valores = {"cnpj": "12345678000190", "data_limite_processamento": "2023"}
    binds = servico.montar_binds(sql_text, valores)
    assert binds == {"cnpj": "12345678000190", "data_limite_processamento": "2023", "nao_existe": None}

def test_servico_tabelas_listar_tabelas():
    tabelas = ServicoTabelas.listar_tabelas()
    assert isinstance(tabelas, list)
    assert len(tabelas) > 0
    assert all(isinstance(t, dict) and "id" in t and "nome" in t for t in tabelas)

@patch("interface_grafica.services.pipeline_funcoes_service.CNPJ_ROOT", new_callable=lambda: Path("/tmp/mock_root"))
def test_limpar_arquivos_legados(mock_root, tmp_path: Path):
    with patch("interface_grafica.services.pipeline_funcoes_service.CNPJ_ROOT", tmp_path):
        cnpj = "12345678000190"
        pasta_analises = tmp_path / cnpj / "analises" / "produtos"
        pasta_brutos = tmp_path / cnpj / "arquivos_parquet"
        pasta_analises.mkdir(parents=True, exist_ok=True)
        pasta_brutos.mkdir(parents=True, exist_ok=True)

        # Create some legacy and valid files
        (pasta_analises / "produtos_unidades_1.parquet").touch()
        (pasta_analises / "produtos_final_1.parquet").touch()
        (pasta_brutos / "vendas_produtos_1.parquet").touch()
        (pasta_brutos / "vendas_validas.parquet").touch()

        ServicoTabelas.limpar_arquivos_legados(cnpj)

        assert not (pasta_analises / "produtos_unidades_1.parquet").exists()
        assert (pasta_analises / "produtos_final_1.parquet").exists()
        assert not (pasta_brutos / "vendas_produtos_1.parquet").exists()
        assert (pasta_brutos / "vendas_validas.parquet").exists()

@patch("interface_grafica.services.pipeline_funcoes_service._importar_funcao_tabela")
def test_gerar_tabelas_sucesso(mock_importar, tmp_path: Path):
    mock_func = MagicMock(return_value=True)
    mock_importar.return_value = mock_func

    with patch("interface_grafica.services.pipeline_funcoes_service.CNPJ_ROOT", tmp_path):
        mensagens = []
        resultado = ServicoTabelas.gerar_tabelas(
            "12345678000190", ["tb_documentos", "itens"], lambda x: mensagens.append(x)
        )

        assert resultado.ok is True
        assert len(resultado.erros) == 0
        assert "tb_documentos" in resultado.geradas
        assert "itens" in resultado.geradas
        assert "item_unidades" not in resultado.geradas # Not requested
        assert len(mensagens) > 0

@patch("interface_grafica.services.pipeline_funcoes_service._importar_funcao_tabela")
def test_gerar_tabelas_falha_excecao(mock_importar, tmp_path: Path):
    mock_func = MagicMock(side_effect=Exception("Erro simulado"))
    mock_importar.return_value = mock_func

    with patch("interface_grafica.services.pipeline_funcoes_service.CNPJ_ROOT", tmp_path):
        resultado = ServicoTabelas.gerar_tabelas("12345678000190", ["tb_documentos"])

        assert resultado.ok is False
        assert len(resultado.erros) == 1
        assert "Erro simulado" in resultado.erros[0]
        assert len(resultado.geradas) == 0

@patch("interface_grafica.services.pipeline_funcoes_service._importar_funcao_tabela")
def test_gerar_tabelas_falha_retorno_false(mock_importar, tmp_path: Path):
    mock_func = MagicMock(return_value=False)
    mock_importar.return_value = mock_func

    with patch("interface_grafica.services.pipeline_funcoes_service.CNPJ_ROOT", tmp_path):
        resultado = ServicoTabelas.gerar_tabelas("12345678000190", ["tb_documentos"])

        assert resultado.ok is False
        assert len(resultado.erros) == 1
        assert "etapa retornou False" in resultado.erros[0]
        assert len(resultado.geradas) == 0

@patch("interface_grafica.services.pipeline_funcoes_service.ServicoExtracao")
@patch("interface_grafica.services.pipeline_funcoes_service.ServicoTabelas")
def test_pipeline_completo_sucesso(mock_tabelas_cls, mock_extracao_cls):
    # Mocking sanitizar_cnpj is necessary if ServicoExtracao is patched since it's a staticmethod
    mock_extracao_cls.sanitizar_cnpj = MagicMock(return_value="12345678000190")

    mock_extracao = mock_extracao_cls.return_value
    mock_extracao.executar_consultas.return_value = ["file1.parquet"]

    mock_tabelas = mock_tabelas_cls.return_value
    mock_tabelas.gerar_tabelas.return_value = ResultadoGeracaoTabelas(ok=True, geradas=["tab1"])

    pipeline = ServicoPipelineCompleto()
    mensagens = []

    resultado = pipeline.executar_completo(
        "123.456.789/0001-00",
        consultas=["sql1"],
        tabelas=["tab1"],
        progresso=lambda x: mensagens.append(x)
    )

    assert resultado.ok is True
    assert resultado.cnpj == "12345678000190"
    assert "file1.parquet" in resultado.arquivos_gerados
    assert "tab1" in resultado.arquivos_gerados
    assert len(resultado.erros) == 0
    assert len(mensagens) > 0

@patch("interface_grafica.services.pipeline_funcoes_service.ServicoExtracao")
@patch("interface_grafica.services.pipeline_funcoes_service.ServicoTabelas")
def test_pipeline_completo_falha_extracao(mock_tabelas_cls, mock_extracao_cls):
    mock_extracao_cls.sanitizar_cnpj = MagicMock(return_value="12345678000190")

    mock_extracao = mock_extracao_cls.return_value
    mock_extracao.executar_consultas.side_effect = Exception("Erro extracao")

    pipeline = ServicoPipelineCompleto()

    resultado = pipeline.executar_completo("12345678000190", consultas=["sql1"], tabelas=[])

    assert resultado.ok is False
    assert len(resultado.erros) == 1
    assert "Erro extracao" in resultado.erros[0]

@patch("interface_grafica.services.pipeline_funcoes_service.ServicoExtracao")
@patch("interface_grafica.services.pipeline_funcoes_service.ServicoTabelas")
def test_pipeline_completo_falha_tabelas(mock_tabelas_cls, mock_extracao_cls):
    mock_extracao_cls.sanitizar_cnpj = MagicMock(return_value="12345678000190")

    mock_extracao = mock_extracao_cls.return_value
    mock_extracao.executar_consultas.return_value = []

    mock_tabelas = mock_tabelas_cls.return_value
    mock_tabelas.gerar_tabelas.return_value = ResultadoGeracaoTabelas(ok=False, erros=["Erro tabela"])

    pipeline = ServicoPipelineCompleto()

    resultado = pipeline.executar_completo("12345678000190", consultas=[], tabelas=["tab1"])

    assert resultado.ok is False
    assert len(resultado.erros) == 1
    assert "Erro tabela" in resultado.erros[0]


def test_servico_extracao_init():
    servico = ServicoExtracao()
    assert servico.consultas_dir is not None

def test_servico_extracao_init_with_consultas_dir():
    # Test init with different types
    servico1 = ServicoExtracao(consultas_dir="test_dir")
    assert "test_dir" in [p.name for p in [servico1.consultas_dir]]

@patch("interface_grafica.services.pipeline_funcoes_service.obter_conexao_oracle")
@patch("interface_grafica.services.pipeline_funcoes_service.resolve_sql_path")
@patch("interface_grafica.services.pipeline_funcoes_service.ler_sql")
@patch("interface_grafica.services.pipeline_funcoes_service._gravar_cursor_em_parquet")
def test_executar_consultas_sucesso(mock_gravar, mock_ler_sql, mock_resolve, mock_conexao, tmp_path: Path):
    # Setup mocks
    mock_conn = MagicMock()
    mock_conexao.return_value.__enter__.return_value = mock_conn
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # Mocking SQL file and text
    sql_path = tmp_path / "consulta1.sql"
    sql_path.touch()
    mock_resolve.return_value = sql_path
    mock_ler_sql.return_value = "SELECT * FROM t"

    mock_gravar.return_value = 100 # linhas

    # Patch self.pasta_parquets to use tmp_path
    with patch.object(ServicoExtracao, 'pasta_parquets', return_value=tmp_path):
        servico = ServicoExtracao()

        mensagens = []
        arquivos = servico.executar_consultas(
            "12345678000190", ["consulta1"], data_limite="2023", progresso=lambda x: mensagens.append(x)
        )

        assert len(arquivos) == 1
        assert "consulta1_12345678000190.parquet" in arquivos[0]
        assert len(mensagens) > 0
        assert mock_conn.cursor.called
        assert mock_cursor.execute.called

@patch("interface_grafica.services.pipeline_funcoes_service.obter_conexao_oracle")
@patch("interface_grafica.services.pipeline_funcoes_service.resolve_sql_path")
@patch("interface_grafica.services.pipeline_funcoes_service.ler_sql")
def test_executar_consultas_falha(mock_ler_sql, mock_resolve, mock_conexao, tmp_path: Path):
    mock_conn = MagicMock()
    mock_conexao.return_value.__enter__.return_value = mock_conn
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    sql_path = tmp_path / "consulta_falha.sql"
    sql_path.touch()
    mock_resolve.return_value = sql_path
    mock_ler_sql.return_value = "SELECT * FROM t"

    # Simulate execution failure
    mock_cursor.execute.side_effect = Exception("Oracle Error")

    with patch.object(ServicoExtracao, 'pasta_parquets', return_value=tmp_path):
        servico = ServicoExtracao()

        mensagens = []
        arquivos = servico.executar_consultas(
            "12345678000190", ["consulta_falha"], progresso=lambda x: mensagens.append(x)
        )

        # O arquivo não entra na lista se der erro
        assert len(arquivos) == 0
        # A mensagem de erro deve estar nas msgs
        assert any("Erro em" in msg and "Oracle Error" in msg for msg in mensagens)

def test_executar_completo_fase_1_sem_tabelas():
    # Setup mocks
    with patch('interface_grafica.services.pipeline_funcoes_service.ServicoExtracao.executar_consultas') as mock_executar:
        mock_executar.return_value = ["file1.parquet"]

        pipeline = ServicoPipelineCompleto()

        # Test just the Phase 1 branch logic
        resultado = pipeline.executar_completo(
            cnpj="12345678000190",
            consultas=["sql1"],
            tabelas=[]
        )

        assert resultado.ok is True
        assert "file1.parquet" in resultado.arquivos_gerados
        assert mock_executar.called


def test_sanitizar_cnpj_erro():
    with pytest.raises(ValueError, match="Informe um CPF com 11 digitos ou um CNPJ com 14 digitos"):
        ServicoExtracao.sanitizar_cnpj("123")

def test_pasta_parquets():
    servico = ServicoExtracao()
    pasta = servico.pasta_parquets("12345678000190")
    assert pasta.name == "arquivos_parquet"
    assert "12345678000190" in str(pasta)

def test_executar_completo_fase_1_sem_tabelas_falha(mock_extracao_cls=None, mock_tabelas_cls=None):
    with patch('interface_grafica.services.pipeline_funcoes_service.ServicoExtracao.executar_consultas') as mock_executar:
        mock_executar.side_effect = Exception("Erro na fase 1")
        pipeline = ServicoPipelineCompleto()
        resultado = pipeline.executar_completo(
            cnpj="12345678000190",
            consultas=["sql1"],
            tabelas=[]
        )
        assert resultado.ok is False
        assert "Erro na fase 1" in str(resultado.erros[0])


@patch("interface_grafica.services.pipeline_funcoes_service.list_sql_entries")
def test_listar_consultas(mock_list):
    # Setup mock returns
    mock_entry = MagicMock()
    mock_entry.sql_id = "consulta_teste"
    mock_list.return_value = [mock_entry]

    servico = ServicoExtracao()
    consultas = servico.listar_consultas()

    assert len(consultas) == 1
    assert consultas[0] == "consulta_teste"


def test_executar_completo_fase_2_excecao(mock_extracao_cls=None, mock_tabelas_cls=None):
    with patch('interface_grafica.services.pipeline_funcoes_service.ServicoExtracao.executar_consultas') as mock_executar:
        mock_executar.return_value = []
        with patch('interface_grafica.services.pipeline_funcoes_service.ServicoTabelas.gerar_tabelas') as mock_gerar:
            mock_gerar.side_effect = Exception("Erro critico tabelas")

            pipeline = ServicoPipelineCompleto()
            resultado = pipeline.executar_completo(
                cnpj="12345678000190",
                consultas=[],
                tabelas=["tab1"]
            )

            assert resultado.ok is False
            assert "Erro critico tabelas" in str(resultado.erros[0])

def test_apagar_dados_processados(tmp_path: Path):
    with patch("interface_grafica.services.pipeline_funcoes_service.ServicoExtracao.pasta_cnpj") as mock_pasta_cnpj:
        mock_pasta_cnpj.return_value = tmp_path / "12345678000190"
        servico = ServicoExtracao()
        cnpj = "12345678000190"
        pasta = mock_pasta_cnpj.return_value

        # Test folder doesn't exist
        assert servico.apagar_dados_cnpj(cnpj) is False

        # Create folder structure
        pasta.mkdir()
        (pasta / "arquivos_parquet").mkdir()
        (pasta / "analises").mkdir()
        (pasta / "outra_pasta").mkdir()

        assert servico.apagar_dados_cnpj(cnpj) is True
        assert not (pasta / "arquivos_parquet").exists()
        assert not (pasta / "analises").exists()
        assert (pasta / "outra_pasta").exists()

def test_apagar_cnpj_total(tmp_path: Path):
    with patch("interface_grafica.services.pipeline_funcoes_service.ServicoExtracao.pasta_cnpj") as mock_pasta_cnpj:
        mock_pasta_cnpj.return_value = tmp_path / "12345678000190"
        servico = ServicoExtracao()
        cnpj = "12345678000190"
        pasta = mock_pasta_cnpj.return_value

        # Test folder doesn't exist
        assert servico.apagar_cnpj_total(cnpj) is False

        # Create folder
        pasta.mkdir()
        assert pasta.exists()

        assert servico.apagar_cnpj_total(cnpj) is True
        assert not pasta.exists()


@patch("interface_grafica.services.pipeline_funcoes_service.obter_conexao_oracle")
@patch("interface_grafica.services.pipeline_funcoes_service.resolve_sql_path")
@patch("interface_grafica.services.pipeline_funcoes_service.ler_sql")
@patch("interface_grafica.services.pipeline_funcoes_service._gravar_cursor_em_parquet")
def test_executar_consultas_fallback_path(mock_gravar, mock_ler_sql, mock_resolve, mock_conexao, tmp_path: Path):
    # Setup connection mock to avoid actual DB calls
    mock_conn = MagicMock()
    mock_conexao.return_value.__enter__.return_value = mock_conn
    mock_resolve.side_effect = Exception("Not found in catalog")

    with patch.object(ServicoExtracao, 'pasta_parquets', return_value=tmp_path):
        servico = ServicoExtracao(consultas_dir=tmp_path)

        # Test fallback 1: File directly exists
        sql_file = tmp_path / "consulta_fallback.sql"
        sql_file.touch()
        sql_file.write_text("SELECT 1")

        mensagens = []
        # This will fail at execution because mock_cursor is not fully set up to return data
        # But it should reach the fallback logic

        mock_ler_sql.return_value = "SELECT 1"
        servico.executar_consultas("12345678000190", ["consulta_fallback.sql"], progresso=lambda x: mensagens.append(x))

        assert any("Executando consulta_fallback.sql" in m for m in mensagens)

@patch("interface_grafica.services.pipeline_funcoes_service.obter_conexao_oracle")
@patch("interface_grafica.services.pipeline_funcoes_service.resolve_sql_path")
def test_executar_consultas_pular_existente(mock_resolve, mock_conexao, tmp_path: Path):
    mock_conn = MagicMock()
    mock_conexao.return_value.__enter__.return_value = mock_conn

    sql_path = tmp_path / "consulta_existente.sql"
    sql_path.touch()
    mock_resolve.return_value = sql_path

    with patch.object(ServicoExtracao, 'pasta_parquets', return_value=tmp_path):
        with patch("interface_grafica.services.pipeline_funcoes_service._parquet_valido_simples", return_value=True):
            servico = ServicoExtracao()

            mensagens = []
            arquivos = servico.executar_consultas(
                "12345678000190", ["consulta_existente"], progresso=lambda x: mensagens.append(x), pular_existente=True
            )

            assert len(arquivos) == 1
            assert any("Pulando" in m for m in mensagens)

@patch("interface_grafica.services.pipeline_funcoes_service.obter_conexao_oracle")
@patch("interface_grafica.services.pipeline_funcoes_service.resolve_sql_path")
def test_executar_consultas_remover_temporario(mock_resolve, mock_conexao, tmp_path: Path):
    mock_conn = MagicMock()
    mock_conexao.return_value.__enter__.return_value = mock_conn

    sql_path = tmp_path / "consulta_tmp.sql"
    sql_path.touch()
    mock_resolve.return_value = sql_path

    with patch.object(ServicoExtracao, 'pasta_parquets', return_value=tmp_path):
        # Create a temp file
        temp_file = tmp_path / "consulta_tmp_12345678000190.parquet.tmp"
        temp_file.touch()
        assert temp_file.exists()

        with patch("interface_grafica.services.pipeline_funcoes_service.ler_sql", return_value=None):
            servico = ServicoExtracao()

            mensagens = []
            servico.executar_consultas(
                "12345678000190", ["consulta_tmp"], progresso=lambda x: mensagens.append(x)
            )

            assert not temp_file.exists()
            assert any("Removido arquivo temporario" in m for m in mensagens)

@patch("interface_grafica.services.pipeline_funcoes_service.obter_conexao_oracle")
@patch("interface_grafica.services.pipeline_funcoes_service.resolve_sql_path")
def test_executar_consultas_ler_sql_falha(mock_resolve, mock_conexao, tmp_path: Path):
    mock_conn = MagicMock()
    mock_conexao.return_value.__enter__.return_value = mock_conn

    sql_path = tmp_path / "consulta_leitura_falha.sql"
    sql_path.touch()
    mock_resolve.return_value = sql_path

    with patch.object(ServicoExtracao, 'pasta_parquets', return_value=tmp_path):
        with patch("interface_grafica.services.pipeline_funcoes_service.ler_sql", return_value=None):
            servico = ServicoExtracao()

            mensagens = []
            arquivos = servico.executar_consultas(
                "12345678000190", ["consulta_leitura_falha"], progresso=lambda x: mensagens.append(x)
            )

            assert len(arquivos) == 0
            assert any("nao foi possivel ler" in m for m in mensagens)


def test_pasta_produtos():
    servico = ServicoExtracao()
    pasta = servico.pasta_produtos("12345678000190")
    assert pasta.name == "produtos"
    assert pasta.parent.name == "analises"
    assert "12345678000190" in str(pasta)

def test_obter_data_entrega_reg0000_vazio(tmp_path: Path):
    with patch.object(ServicoExtracao, 'pasta_parquets', return_value=tmp_path):
        servico = ServicoExtracao()

        df = pl.DataFrame({"data_entrega": []}, schema={"data_entrega": pl.Date})
        arquivo = tmp_path / "reg_0000_12345678000190.parquet"
        df.write_parquet(arquivo)

        assert servico.obter_data_entrega_reg0000("12345678000190") is None

def test_obter_data_entrega_reg0000_invalido(tmp_path: Path):
    with patch.object(ServicoExtracao, 'pasta_parquets', return_value=tmp_path):
        servico = ServicoExtracao()

        arquivo = tmp_path / "reg_0000_12345678000190.parquet"
        arquivo.write_text("invalido")

        assert servico.obter_data_entrega_reg0000("12345678000190") is None

def test_obter_data_entrega_reg0000_sem_strftime(tmp_path: Path):
    with patch.object(ServicoExtracao, 'pasta_parquets', return_value=tmp_path):
        servico = ServicoExtracao()

        df = pl.DataFrame({"data_entrega": ["2023-01-01"]})
        arquivo = tmp_path / "reg_0000_12345678000190.parquet"
        df.write_parquet(arquivo)

        assert servico.obter_data_entrega_reg0000("12345678000190") == "2023-01-01"

@patch("interface_grafica.services.pipeline_funcoes_service.obter_conexao_oracle")
@patch("interface_grafica.services.pipeline_funcoes_service.resolve_sql_path")
@patch("interface_grafica.services.pipeline_funcoes_service.ler_sql")
@patch("interface_grafica.services.pipeline_funcoes_service._gravar_cursor_em_parquet")
def test_executar_consultas_fallback_path_com_sql(mock_gravar, mock_ler_sql, mock_resolve, mock_conexao, tmp_path: Path):
    mock_conn = MagicMock()
    mock_conexao.return_value.__enter__.return_value = mock_conn
    mock_resolve.side_effect = Exception("Not found")

    with patch.object(ServicoExtracao, 'pasta_parquets', return_value=tmp_path):
        servico = ServicoExtracao(consultas_dir=tmp_path)

        sql_file = tmp_path / "consulta_fallback.sql"
        sql_file.touch()
        sql_file.write_text("SELECT 1")

        mensagens = []
        mock_ler_sql.return_value = "SELECT 1"
        if True:
            # Use specific name to hit the branch
            servico.executar_consultas("12345678000190", ["consulta_fallback"], progresso=lambda x: mensagens.append(x))

        assert any("Executando consulta_fallback" in m for m in mensagens)

def test_gerar_tabelas_ignora_nao_encontrada(tmp_path: Path):
    with patch("interface_grafica.services.pipeline_funcoes_service.CNPJ_ROOT", tmp_path):
        mensagens = []
        resultado = ServicoTabelas.gerar_tabelas(
            "123", ["tabela_nao_existe"], lambda x: mensagens.append(x)
        )
        assert resultado.ok is True # Not considered an error, just skipped
        assert len(resultado.geradas) == 0

from interface_grafica.services.pipeline_funcoes_service import _importar_funcao_tabela

def test_importar_funcao_tabela():
    # We can use an existing module to test the dynamic import
    # Like 'utilitarios' or any other known path, but we need one under 'transformacao'
    # For now, let's just mock importlib
    with patch("importlib.import_module") as mock_import:
        mock_module = MagicMock()
        mock_module.minha_funcao = "resultado_funcao"
        mock_import.return_value = mock_module

        funcao = _importar_funcao_tabela("meu_modulo", "minha_funcao")
        assert funcao == "resultado_funcao"
        mock_import.assert_called_with("transformacao.meu_modulo")


def test_servico_extracao_init_with_consultas_dir_list():
    servico = ServicoExtracao(consultas_dir=["/tmp/dir1", "/tmp/dir2"])
    assert len(servico.consultas_dirs) == 2
    assert servico.consultas_dirs[0].name == "dir1"

def test_limpar_arquivos_legados_excecao(tmp_path: Path):
    with patch("interface_grafica.services.pipeline_funcoes_service.CNPJ_ROOT", tmp_path):
        cnpj = "123"
        pasta_analises = tmp_path / cnpj / "analises" / "produtos"
        pasta_brutos = tmp_path / cnpj / "arquivos_parquet"
        pasta_analises.mkdir(parents=True, exist_ok=True)
        pasta_brutos.mkdir(parents=True, exist_ok=True)

        arquivo_analise = pasta_analises / "produtos_unidades_1.parquet"
        arquivo_analise.touch()
        arquivo_bruto = pasta_brutos / "vendas_produtos_1.parquet"
        arquivo_bruto.touch()

        with patch.object(Path, "unlink", side_effect=Exception("Erro unlink")):
            ServicoTabelas.limpar_arquivos_legados(cnpj)

        # Files should still exist since unlink failed and exception was caught
        assert arquivo_analise.exists()
        assert arquivo_bruto.exists()


def test_obter_data_entrega_reg0000_excecao_geral(tmp_path: Path):
    with patch.object(ServicoExtracao, 'pasta_parquets', return_value=tmp_path):
        servico = ServicoExtracao()

        df = pl.DataFrame({"data_entrega": ["2023-01-01"]})
        arquivo = tmp_path / "reg_0000_12345678000190.parquet"
        df.write_parquet(arquivo)

        with patch("polars.read_parquet", side_effect=Exception("Erro generico")):
            assert servico.obter_data_entrega_reg0000("12345678000190") is None

def test_gerar_tabelas_ignora_tabela_sem_info(tmp_path: Path):
    # tb_documentos is in TABELAS_DISPONIVEIS
    # we patch TABELAS_DISPONIVEIS to be empty so info is None
    with patch("interface_grafica.services.pipeline_funcoes_service.TABELAS_DISPONIVEIS", []):
        with patch("interface_grafica.services.pipeline_funcoes_service.CNPJ_ROOT", tmp_path):
            resultado = ServicoTabelas.gerar_tabelas(
                "123", ["tb_documentos"]
            )
            assert resultado.ok is True
            assert len(resultado.geradas) == 0


def test_obter_data_entrega_reg0000_falso_maior_data(tmp_path: Path):
    with patch.object(ServicoExtracao, 'pasta_parquets', return_value=tmp_path):
        servico = ServicoExtracao()

        # Insert a null value
        df = pl.DataFrame({"data_entrega": [None]}, schema={"data_entrega": pl.Date})
        arquivo = tmp_path / "reg_0000_12345678000190.parquet"
        df.write_parquet(arquivo)

        assert servico.obter_data_entrega_reg0000("12345678000190") is None
