#!/usr/bin/env python3
from pathlib import Path
import os
import time
from datetime import datetime, timedelta

def main():
    try:
        import polars as pl
        use_pl = True
    except Exception:
        use_pl = False

    cnpj = "84654326000394"
    snapshots_dir = Path("dados") / "CNPJ" / cnpj / "analises" / "produtos" / "snapshots"
    snapshots_dir.mkdir(parents=True, exist_ok=True)

    created = []
    for i in range(8):
        ts = (datetime.now() - timedelta(days=i)).strftime("%Y%m%dT%H%M%S")
        fname = snapshots_dir / f"mapa_agrupamento_manual_{cnpj}_{ts}.parquet"
        if use_pl:
            pl.DataFrame({"id_descricao": [f"x{i}"]}).write_parquet(fname)
        else:
            # create a placeholder file if polars is not available
            with open(fname, "wb") as fh:
                fh.write(b"example")

        # set mtime to simulate older files
        old = time.time() - i * 86400
        os.utime(fname, (old, old))
        created.append(str(fname))

    print(f"Created {len(created)} snapshot files for {cnpj}")
    for p in created:
        print(p)

if __name__ == '__main__':
    main()
