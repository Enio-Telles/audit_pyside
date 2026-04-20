"""Legacy proxy for the canonical produtos_final implementation."""

from __future__ import annotations

import sys
from pathlib import Path

CURRENT_FILE = Path(__file__).resolve()
ROOT_DIR = CURRENT_FILE.parent.parent.parent
SRC_DIR = ROOT_DIR / "src"

for path in (ROOT_DIR, SRC_DIR):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from transformacao.produtos_final_v2 import gerar_produtos_final, produtos_agrupados

__all__ = ["gerar_produtos_final", "produtos_agrupados"]


if __name__ == "__main__":
    if len(sys.argv) > 1:
        gerar_produtos_final(sys.argv[1])
    else:
        gerar_produtos_final(input("CNPJ: "))
