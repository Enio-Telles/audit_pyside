from pathlib import Path
import sys
import os
from datetime import datetime, timedelta
import time

import polars as pl


sys.path.insert(0, str(Path("src").resolve()))

import interface_grafica.services.aggregation_service as aggregation_service_module


def _create_snapshots(snapshots_dir: Path, cnpj: str, days_list: list[int]) -> list[Path]:
    created: list[Path] = []
    for d in days_list:
        ts = (datetime.now() - timedelta(days=d)).strftime("%Y%m%dT%H%M%S")
        fname = snapshots_dir / f"mapa_agrupamento_manual_{cnpj}_{ts}.parquet"
        pl.DataFrame({"id_descricao": [f"x{d}"]}).write_parquet(fname)
        old = time.time() - d * 86400
        os.utime(fname, (old, old))
        created.append(fname)
    return created


def test_keep_last_preserved(tmp_path: Path, monkeypatch):
    cnpj = "11111111000111"
    pasta_prod = tmp_path / cnpj / "analises" / "produtos"
    snapshots_dir = pasta_prod / "snapshots"
    snapshots_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(aggregation_service_module, "CNPJ_ROOT", tmp_path)

    servico = aggregation_service_module.ServicoAgregacao()

    created = _create_snapshots(snapshots_dir, cnpj, list(range(6)))

    removed = servico.limpar_snapshots_mapa_manual(cnpj, keep_last=2, keep_days=0)

    assert removed == len(created) - 2

    remaining = sorted(snapshots_dir.glob("*.parquet"), key=lambda p: p.stat().st_mtime, reverse=True)
    assert len(remaining) == 2
    assert remaining[0] == created[0]
    assert remaining[1] == created[1]


def test_keep_days_retention(tmp_path: Path, monkeypatch):
    cnpj = "22222222000122"
    pasta_prod = tmp_path / cnpj / "analises" / "produtos"
    snapshots_dir = pasta_prod / "snapshots"
    snapshots_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(aggregation_service_module, "CNPJ_ROOT", tmp_path)

    servico = aggregation_service_module.ServicoAgregacao()

    created = _create_snapshots(snapshots_dir, cnpj, [0, 1, 3])

    removed = servico.limpar_snapshots_mapa_manual(cnpj, keep_last=0, keep_days=2)

    assert removed == 1

    remaining = sorted(snapshots_dir.glob("*.parquet"), key=lambda p: p.stat().st_mtime, reverse=True)
    assert created[2] not in remaining
    assert created[0] in remaining and created[1] in remaining


def test_dry_run_does_not_delete(tmp_path: Path, monkeypatch):
    cnpj = "33333333000133"
    pasta_prod = tmp_path / cnpj / "analises" / "produtos"
    snapshots_dir = pasta_prod / "snapshots"
    snapshots_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(aggregation_service_module, "CNPJ_ROOT", tmp_path)

    servico = aggregation_service_module.ServicoAgregacao()

    created = _create_snapshots(snapshots_dir, cnpj, list(range(4)))

    removed = servico.limpar_snapshots_mapa_manual(cnpj, keep_last=1, keep_days=0, dry_run=True)

    assert removed == len(created) - 1

    # Ensure files still exist after dry-run
    remaining = sorted(snapshots_dir.glob("*.parquet"), key=lambda p: p.stat().st_mtime, reverse=True)
    assert set(remaining) == set(created)
