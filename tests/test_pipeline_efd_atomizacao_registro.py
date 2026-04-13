from pathlib import Path
import sys


sys.path.insert(0, str(Path("src").resolve()))

from interface_grafica.services.pipeline_funcoes_service import TABELAS_DISPONIVEIS, ServicoTabelas
from orquestrador_pipeline import REGISTO_TABELAS


def test_registro_efd_atomizacao_no_catalogo_e_na_ordem():
    ids_catalogo = [item["id"] for item in TABELAS_DISPONIVEIS]
    assert "efd_atomizacao" in ids_catalogo
    assert ids_catalogo[0] == "efd_atomizacao"

    ids_listados = [item["id"] for item in ServicoTabelas.listar_tabelas()]
    assert ids_listados[0] == "efd_atomizacao"


def test_registro_efd_atomizacao_no_orquestrador():
    registro = REGISTO_TABELAS.get("efd_atomizacao")
    assert registro is not None
    assert registro.funcao_path == "transformacao.efd_atomizacao:gerar_efd_atomizacao"
    assert registro.deps == []
