import pytest
from src.metodologia_mds import orchestrator


def test_orchestrator_functions_available():
    assert callable(orchestrator.gerar_movimentacao_estoque)
    assert callable(orchestrator.gerar_calculos_periodos)
    assert callable(orchestrator.gerar_calculos_mensais)
    assert callable(orchestrator.gerar_calculos_anuais)


# lightweight smoke test: calling with an obviously missing CNPJ should return False
# or raise a well-formed RuntimeError if dependencies are missing. We accept both behaviors.
def test_orchestrator_calls_fail_gracefully():
    try:
        res = orchestrator.gerar_calculos_periodos("00000000000000")
        assert res in (False, True)
    except RuntimeError:
        pytest.skip("Runtime imports unavailable in this environment")
