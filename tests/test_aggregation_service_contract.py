from pathlib import Path
import sys

import polars as pl


sys.path.insert(0, str(Path("src").resolve()))

import interface_grafica.services.aggregation_service as aggregation_service_module


def test_salvar_mapa_manual_registra_log(tmp_path: Path, monkeypatch):
    cnpj = "99999999000106"
    pasta_prod = tmp_path / cnpj / "analises" / "produtos"
    pasta_prod.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(aggregation_service_module, "CNPJ_ROOT", tmp_path)

    servico = aggregation_service_module.ServicoAgregacao()

    df_manual = pl.DataFrame({"id_descricao": ["id1"], "id_agrupado": ["AGR_Z"]})
    ok = servico.salvar_mapa_manual(cnpj, df_manual, reprocessar=False)

    assert ok is True
    logs = servico.ler_linhas_log(cnpj)
    assert any(entry.get("tipo") == "mapa_manual_atualizado" for entry in logs)
