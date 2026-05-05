import os
import sys
import pytest
from pathlib import Path

# Add scripts directory to path to import benchmark generator
SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

try:
    from benchmark_gui import _gerar_parquet_sintetico
except ImportError:
    # Fallback to local stub if not found
    def _gerar_parquet_sintetico(target: Path, size_mb: int) -> Path:
        import polars as pl
        import numpy as np
        
        linhas_por_mb = 33_000
        n_linhas = linhas_por_mb * size_mb
        
        df = pl.DataFrame({
            "id_agrupado": [f"id_agrupado_auto_{i:012x}" for i in range(n_linhas)],
            "descricao": [f"PRODUTO {i:05d}" for i in range(n_linhas)],
            "vlr_contabil": np.random.rand(n_linhas) * 100,
        })
        p = target / f"bench_{size_mb}mb.parquet"
        df.write_parquet(p)
        return p

@pytest.fixture(scope="session")
def bench_large_parquet_64mb(tmp_path_factory) -> Path:
    """Fixture que gera um parquet sintetico de 64 MB para benchmarks reproduzíveis e rápidos na CI."""
    bench_dir = tmp_path_factory.mktemp("bench_large")
    path = _gerar_parquet_sintetico(bench_dir, 64)
    return path
