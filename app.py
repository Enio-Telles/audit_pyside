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

from PySide6.QtWidgets import QApplication  # noqa: E402
from interface_grafica.logging_setup import configure_structlog, install_fallback_hooks  # noqa: E402
from interface_grafica.patches.similaridade_agregacao import apply_similarity_patch  # noqa: E402

from interface_grafica.widgets.splash_screen import ModernSplashScreen  # noqa: E402


def main() -> int:
    configure_structlog()
    install_fallback_hooks()
    app = QApplication(sys.argv)
    app.setApplicationName("Fiscal Parquet Analyzer (Refatorado)")

    splash = ModernSplashScreen()
    splash.show()
    splash.set_progress(10, "Configurando ambiente...")

    app.processEvents()

    splash.set_progress(30, "Carregando patches...")
    apply_similarity_patch()

    splash.set_progress(60, "Inicializando interface...")
    from interface_grafica.windows.main_window import MainWindow  # noqa: E402
    window = MainWindow()

    splash.set_progress(90, "Finalizando...")
    window.show()
    splash.finish(window)

    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
