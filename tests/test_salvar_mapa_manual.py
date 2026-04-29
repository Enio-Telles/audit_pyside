from pathlib import Path
import sys

import polars as pl


sys.path.insert(0, str(Path("src").resolve()))

import interface_grafica.services.aggregation_service as aggregation_service_module


def test_salvar_mapa_manual_grava_arquivo(tmp_path: Path, monkeypatch):
    cnpj = "99999999000103"
    pasta_prod = tmp_path / cnpj / "analises" / "produtos"
    pasta_prod.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(aggregation_service_module, "CNPJ_ROOT", tmp_path)

    # criar base de descricao para permitir auditoria
    pl.DataFrame(
        {"id_descricao": ["id1"], "descricao_normalizada": ["PROD A"]}
    ).write_parquet(pasta_prod / f"descricao_produtos_{cnpj}.parquet")

    servico = aggregation_service_module.ServicoAgregacao()

    df_manual = pl.DataFrame({"id_descricao": ["id1"], "id_agrupado": ["AGR_X"]})

    ok = servico.salvar_mapa_manual(cnpj, df_manual, reprocessar=False)

    assert ok is True
    assert (pasta_prod / f"mapa_agrupamento_manual_{cnpj}.parquet").exists()


def test_salvar_mapa_manual_gera_auditoria_para_nao_matching(tmp_path: Path, monkeypatch):
    cnpj = "99999999000104"
    pasta_prod = tmp_path / cnpj / "analises" / "produtos"
    pasta_prod.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(aggregation_service_module, "CNPJ_ROOT", tmp_path)

    # criar base de descricao vazia
    pl.DataFrame({"id_descricao": ["id1"]}).write_parquet(pasta_prod / f"descricao_produtos_{cnpj}.parquet")

    servico = aggregation_service_module.ServicoAgregacao()

    df_manual = pl.DataFrame({"id_descricao": ["missing"], "id_agrupado": ["AGR_Y"]})

    ok = servico.salvar_mapa_manual(cnpj, df_manual, reprocessar=False)

    assert ok is True
    assert (pasta_prod / f"mapa_agrupamento_manual_{cnpj}.parquet").exists()
    assert (pasta_prod / f"auditoria_mapa_agrupamento_manual_sem_match_{cnpj}.parquet").exists()


def test_salvar_mapa_manual_snapshot_and_rollback(tmp_path: Path, monkeypatch):
    cnpj = "99999999000105"
    pasta_prod = tmp_path / cnpj / "analises" / "produtos"
    pasta_prod.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(aggregation_service_module, "CNPJ_ROOT", tmp_path)

    servico = aggregation_service_module.ServicoAgregacao()

    df_manual_1 = pl.DataFrame({"id_descricao": ["idA"], "id_agrupado": ["AGR_1"]})
    ok1 = servico.salvar_mapa_manual(cnpj, df_manual_1, reprocessar=False)
    assert ok1 is True

    # Second save should create a snapshot of the previous map
    df_manual_2 = pl.DataFrame({"id_descricao": ["idA"], "id_agrupado": ["AGR_2"]})
    ok2 = servico.salvar_mapa_manual(cnpj, df_manual_2, reprocessar=False)
    assert ok2 is True

    snapshots = list((pasta_prod / "snapshots").glob("mapa_agrupamento_manual_*.parquet"))
    assert len(snapshots) >= 1

    # Reverter para o snapshot mais recente
    ok_rev = servico.reverter_mapa_manual(cnpj)
    assert ok_rev is True

    df_restored = pl.read_parquet(pasta_prod / f"mapa_agrupamento_manual_{cnpj}.parquet")
    # After revert, restored mapping should equal the first mapping
    assert df_restored.to_dicts() == df_manual_1.select(["id_descricao", "id_agrupado"]).to_dicts()
