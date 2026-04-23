import os
import sys
import traceback

import pytest
from PySide6.QtWidgets import QApplication, QTabWidget
from PySide6.QtCore import QThreadPool


def _collect_tabwidgets(win):
    return [w for w in win.findChildren(QTabWidget)]


@pytest.mark.gui_smoke
def test_main_window_smoke(qtbot):
    """Headless smoke test for MainWindow shim and canonical implementation.

    - Ensures windows can be constructed offscreen
    - Iterates each QTabWidget tabs and processes events
    - Asserts window visible and no uncaught exceptions
    """
    # Ensure offscreen platform for CI / headless
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

    exceptions = []

    old_excepthook = sys.excepthook

    def _excepthook(exc_type, exc, tb):
        exceptions.append((exc_type, exc, traceback.format_tb(tb)))

    sys.excepthook = _excepthook

    try:
        # Import shim
        from interface_grafica.ui.main_window import MainWindow as MainWindowShim

        # Try canonical import used in different repo states; fall back if missing
        try:
            from interface_grafica.windows.main_window import MainWindow as MainWindowCanonical
        except Exception:
            try:
                from interface_grafica.ui.main_window_impl import MainWindow as MainWindowCanonical
            except Exception:
                MainWindowCanonical = None

        for MW in (MainWindowShim, MainWindowCanonical):
            if MW is None:
                continue
            win = MW()
            qtbot.addWidget(win)
            win.show()
            qtbot.wait(100)

            tab_widgets = _collect_tabwidgets(win)
            assert tab_widgets, "No QTabWidget found in MainWindow"

            for tab in tab_widgets:
                for i in range(tab.count()):
                    tab.setCurrentIndex(i)
                    qtbot.wait(50)
                    QApplication.processEvents()
                    assert win.isVisible()

            win.close()
            # wait for any running QThreadPool tasks
            try:
                QThreadPool.globalInstance().waitForDone(2000)
            except Exception:
                # best-effort; some Qt bindings may not accept timeout
                pass

        assert not exceptions, f"Exceptions were captured during GUI run: {exceptions}"
    finally:
        sys.excepthook = old_excepthook
