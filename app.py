"""
Lançador principal do Fiscal Parquet Analyzer.

Executa a interface gráfica a partir da raiz do projeto C:\\Sistema_pysisde,
configurando o sys.path para encontrar os pacotes dentro de src/.
"""

from __future__ import annotations

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

# Bundle smoke-test mode: verify imports without launching the GUI.
# Used by .github/workflows/bundle-smoke.yml — exits 0 if the bundle is healthy.
if __name__ == "__main__" and "--smoke" in sys.argv:
    from utilitarios.project_paths import PROJECT_ROOT  # noqa: F401

    print("smoke-ok", flush=True)
    sys.exit(0)


QApplication = None
MainWindow = None

def main() -> int:
    global QApplication, MainWindow
    if QApplication is None:
        from PySide6.QtWidgets import QApplication as _QApp
        QApplication = _QApp
    if MainWindow is None:
        from interface_grafica.windows.main_window import MainWindow as _MainWindow
        MainWindow = _MainWindow
    try:
        from interface_grafica.logging_setup import configure_structlog, install_fallback_hooks
    except Exception:
        def configure_structlog(*a, **kw):
            return None
        def install_fallback_hooks(*a, **kw):
            return None
    try:
        from interface_grafica.patches.similaridade_agregacao import apply_similarity_patch
    except Exception:
        def apply_similarity_patch(*a, **kw):
            return None
    configure_structlog()
    install_fallback_hooks()
    apply_similarity_patch()
    app = QApplication(sys.argv)
    app.setApplicationName("Fiscal Parquet Analyzer (Refatorado)")
    window = MainWindow()
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
