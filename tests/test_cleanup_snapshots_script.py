from pathlib import Path
import sys
import os
import time
from datetime import datetime, timedelta

import polars as pl


sys.path.insert(0, str(Path("src").resolve()))

# load the script module directly from the repository scripts/ folder
import importlib.util
script_path = Path(__file__).resolve().parents[1] / "scripts" / "cleanup_snapshots.py"
spec = importlib.util.spec_from_file_location("cleanup_snapshots", script_path)
cleanup_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cleanup_mod)
cleanup_all_cnpjs = cleanup_mod.cleanup_all_cnpjs


def test_cleanup_script(tmp_path: Path):
    cnpj1 = "99999999000101"
    cnpj2 = "99999999000102"
    for cnpj in (cnpj1, cnpj2):
        snaps_dir = tmp_path / cnpj / "analises" / "produtos" / "snapshots"
        snaps_dir.mkdir(parents=True, exist_ok=True)
        for i in range(8):
            ts = (datetime.now() - timedelta(days=i + 1)).strftime("%Y%m%dT%H%M%S")
            fname = snaps_dir / f"mapa_agrupamento_manual_{cnpj}_{ts}.parquet"
            pl.DataFrame({"id_descricao": [f"x{i}"]}).write_parquet(fname)
            old = time.time() - (i + 1) * 86400
            os.utime(fname, (old, old))

    results = cleanup_all_cnpjs(base_dir=tmp_path, keep_last=3, keep_days=0, dry_run=False)

    assert cnpj1 in results and cnpj2 in results
    for val in results.values():
        assert val >= 5

    for cnpj in (cnpj1, cnpj2):
        snaps = list((tmp_path / cnpj / "analises" / "produtos" / "snapshots").glob("*.parquet"))
        assert len(snaps) <= 3
