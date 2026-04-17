from pathlib import Path
import sys


sys.path.insert(0, str(Path("src").resolve()))

import interface_grafica.services.aggregation_service as aggregation_service_module


def test_recalcular_referencias_agr_inclui_calculos_periodos(monkeypatch):
    ordem = []
    servico = aggregation_service_module.ServicoAgregacao()

    monkeypatch.setattr(servico, "recalcular_produtos_final", lambda cnpj: ordem.append("produtos_final") or True)
    monkeypatch.setattr(servico, "refazer_tabelas_agr", lambda cnpj: ordem.append("fontes_agr") or True)
    monkeypatch.setattr(aggregation_service_module, "calcular_fatores_conversao", lambda cnpj: ordem.append("fatores_conversao") or True)
    monkeypatch.setattr(aggregation_service_module, "gerar_c170_xml", lambda cnpj: ordem.append("c170_xml") or True)
    monkeypatch.setattr(aggregation_service_module, "gerar_c176_xml", lambda cnpj: ordem.append("c176_xml") or True)
    monkeypatch.setattr(aggregation_service_module, "gerar_movimentacao_estoque", lambda cnpj: ordem.append("mov_estoque") or True)
    monkeypatch.setattr(aggregation_service_module, "gerar_calculos_mensais", lambda cnpj: ordem.append("calculos_mensais") or True)
    monkeypatch.setattr(aggregation_service_module, "gerar_calculos_anuais", lambda cnpj: ordem.append("calculos_anuais") or True)
    monkeypatch.setattr(aggregation_service_module, "gerar_calculos_periodos", lambda cnpj: ordem.append("calculos_periodos") or True)

    assert servico.recalcular_referencias_agr("12345678000190", reset_timings=False) is True
    assert ordem == [
        "produtos_final",
        "fontes_agr",
        "fatores_conversao",
        "c170_xml",
        "c176_xml",
        "mov_estoque",
        "calculos_mensais",
        "calculos_anuais",
        "calculos_periodos",
    ]


def test_recalcular_mov_estoque_inclui_calculos_periodos(monkeypatch):
    ordem = []
    servico = aggregation_service_module.ServicoAgregacao()

    monkeypatch.setattr(aggregation_service_module, "gerar_c176_xml", lambda cnpj: ordem.append("c176_xml") or True)
    monkeypatch.setattr(aggregation_service_module, "gerar_movimentacao_estoque", lambda cnpj: ordem.append("mov_estoque") or True)
    monkeypatch.setattr(aggregation_service_module, "gerar_calculos_mensais", lambda cnpj: ordem.append("calculos_mensais") or True)
    monkeypatch.setattr(aggregation_service_module, "gerar_calculos_anuais", lambda cnpj: ordem.append("calculos_anuais") or True)
    monkeypatch.setattr(aggregation_service_module, "gerar_calculos_periodos", lambda cnpj: ordem.append("calculos_periodos") or True)

    assert servico.recalcular_mov_estoque("12345678000190", reset_timings=False) is True
    assert ordem == [
        "c176_xml",
        "mov_estoque",
        "calculos_mensais",
        "calculos_anuais",
        "calculos_periodos",
    ]


def test_artefatos_estoque_defasados_inclui_calculos_periodos(tmp_path, monkeypatch):
    cnpj = "12345678000190"
    pasta_produtos = tmp_path / cnpj / "analises" / "produtos"
    pasta_produtos.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(aggregation_service_module, "CNPJ_ROOT", tmp_path)

    (pasta_produtos / f"aba_mensal_{cnpj}.parquet").touch()
    (pasta_produtos / f"aba_anual_{cnpj}.parquet").touch()
    (pasta_produtos / f"mov_estoque_{cnpj}.parquet").touch()

    servico = aggregation_service_module.ServicoAgregacao()
    artefatos = set(servico.artefatos_estoque_defasados(cnpj))

    assert artefatos == {"calculos_mensais", "calculos_anuais", "calculos_periodos"}
