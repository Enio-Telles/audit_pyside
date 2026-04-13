"""
Lançador principal do Fiscal Parquet Analyzer.

Executa a interface gráfica a partir da raiz do projeto C:\\Sistema_pysisde,
configurando o sys.path para encontrar os pacotes dentro de src/.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# Adiciona o diretório src ao sys.path para permitir pacotes como interface_grafica, extracao, etc.
ROOT_DIR = Path(__file__).parent
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Garante que os utilitários também estejam acessíveis
UTILITARIOS_DIR = SRC_DIR / "utilitarios"
if str(UTILITARIOS_DIR) not in sys.path:
    sys.path.insert(0, str(UTILITARIOS_DIR))

from PySide6.QtWidgets import QApplication
from interface_grafica.ui.main_window import MainWindow

def main() -> int:
    app = QApplication(sys.argv)
    app.setApplicationName("Fiscal Parquet Analyzer (Refatorado)")
    window = MainWindow()
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())

