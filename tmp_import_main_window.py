import sys

sys.path.insert(0, "src")
try:
    from interface_grafica.ui.main_window import MainWindow

    print("Imported MainWindow")
except Exception as e:
    import traceback

    traceback.print_exc()
    print("FAILED", e)
