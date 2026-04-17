"""conftest.py raiz — garante que src/ e src/utilitarios/ estejam no sys.path."""
import sys
from pathlib import Path

ROOT = Path(__file__).parent
SRC = ROOT / "src"
UTILITARIOS = SRC / "utilitarios"

for p in (str(SRC), str(UTILITARIOS)):
    if p not in sys.path:
        sys.path.insert(0, p)
