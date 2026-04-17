import sys

sys.path.insert(0, "src")
try:

    print("Imported MainWindow")
except Exception as e:
    import traceback

    traceback.print_exc()
    print("FAILED", e)
