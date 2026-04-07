from pathlib import Path
import sys


sys.path.insert(0, str(Path("src").resolve()))

from interface_grafica.services.pipeline_funcoes_service import (  # noqa: E402
    TABELAS_DISPONIVEIS,
    enriquecer_consultas_dependentes,
)
from orquestrador_pipeline import REGISTO_TABELAS  # noqa: E402


def test_registro_ressarcimento_st_no_catalogo_e_orquestrador():
    ids_catalogo = [item["id"] for item in TABELAS_DISPONIVEIS]
    assert "ressarcimento_st" in ids_catalogo

    registro = REGISTO_TABELAS.get("ressarcimento_st")
    assert registro is not None
    assert registro.funcao_path == "transformacao.ressarcimento_st:gerar_ressarcimento_st"
    assert registro.deps == ["efd_atomizacao", "c176_xml"]


def test_enriquecer_consultas_dependentes_adiciona_sqls_canonicas_do_ressarcimento():
    consultas = enriquecer_consultas_dependentes([], ["ressarcimento_st"])
    nomes = [Path(str(item)).name for item in consultas]

    assert "01_parametros_ultima_efd.sql" in nomes
    assert "07_sefin_vigencia.sql" in nomes
    assert "08_rateio_frete_cte.sql" in nomes
    assert "10_st_calc_ate_2022.sql" in nomes
    assert "11_fronteira_item_simples.sql" in nomes
    assert "12_fronteira_item_completo.sql" in nomes
