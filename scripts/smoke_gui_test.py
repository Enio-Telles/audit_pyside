import sys
from pathlib import Path

def main():
    # Ensure the repository root is on sys.path so `src` is importable
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root))
    # Lightweight smoke test: import the extracted module and instantiate
    # AbaRelatorios with a minimal dummy MainWindow that provides the
    # attributes/methods AbaRelatorios expects.
    try:
        from PySide6.QtWidgets import QApplication, QWidget, QPushButton
    except Exception as e:
        print("PySide6 import failed:", e)
        raise

    app = QApplication.instance() or QApplication(sys.argv)

    class DummyMain:
        def _build_tab_mov_estoque(self):
            return QWidget()

        def _build_tab_aba_anual(self):
            return QWidget()

        def _build_tab_aba_periodos(self):
            return QWidget()

        def _build_tab_produtos_selecionados(self):
            return QWidget()

        def _build_tab_id_agrupados(self):
            return QWidget()

        def _criar_botao_destacar(self):
            return QPushButton("destacar")

        resumo_global_model = None
        aba_mensal_model = None

    dummy = DummyMain()

    try:
        from src.interface_grafica.windows.aba_relatorios import AbaRelatorios
    except Exception as e:
        print("Failed to import AbaRelatorios:", e)
        raise

    try:
        aba = AbaRelatorios(dummy)
        print("AbaRelatorios instantiated ok -> root type:", type(aba.root))
    except Exception as e:
        print("Failed to instantiate AbaRelatorios:", e)
        raise


if __name__ == "__main__":
    main()
