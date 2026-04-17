"""
Lançador seguro do Fiscal Parquet Analyzer.

Usa a SafeMainWindow, que solicita cancelamento/interrupção dos workers
ativos ao fechar a aplicação, reduzindo risco de threads zumbis.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).parent
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

UTILITARIOS_DIR = SRC_DIR / "utilitarios"
if str(UTILITARIOS_DIR) not in sys.path:
    sys.path.insert(0, str(UTILITARIOS_DIR))

from PySide6.QtWidgets import QApplication
from interface_grafica.ui.main_window_safe import SafeMainWindow


def main() -> int:
    app = QApplication(sys.argv)
    app.setApplicationName("Fiscal Parquet Analyzer (Safe)")
    window = SafeMainWindow()
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
