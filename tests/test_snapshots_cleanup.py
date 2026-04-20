from pathlib import Path
import sys
import os
from datetime import datetime, timedelta
import time

import polars as pl


sys.path.insert(0, str(Path("src").resolve()))

import interface_grafica.services.aggregation_service as aggregation_service_module


def test_limpar_snapshots_mapa_manual(tmp_path: Path, monkeypatch):
    cnpj = "99999999000107"
    pasta_prod = tmp_path / cnpj / "analises" / "produtos"
    snapshots_dir = pasta_prod / "snapshots"
    snapshots_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(aggregation_service_module, "CNPJ_ROOT", tmp_path)

    servico = aggregation_service_module.ServicoAgregacao()

    # create 12 snapshot files with decreasing ages
    for i in range(12):
        ts = (datetime.now() - timedelta(days=i)).strftime("%Y%m%dT%H%M%S")
        fname = snapshots_dir / f"mapa_agrupamento_manual_{cnpj}_{ts}.parquet"
        pl.DataFrame({"id_descricao": [f"x{i}"]}).write_parquet(fname)
        # set mtime to simulate older file
        old = time.time() - i * 86400
        os.utime(fname, (old, old))

    removed = servico.limpar_snapshots_mapa_manual(cnpj, keep_last=5, keep_days=1)

    assert removed >= 7
    remaining = list(snapshots_dir.glob("*.parquet"))
    assert len(remaining) <= 5
