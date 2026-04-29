from __future__ import annotations

from typing import TYPE_CHECKING

from PySide6.QtWidgets import QInputDialog, QMessageBox

__all__ = ["MainWindow", "QInputDialog", "QMessageBox"]


if TYPE_CHECKING:
	# Allow static type checkers to see the real class without importing it at runtime
	from interface_grafica.windows.main_window import MainWindow as _RealMainWindow  # type: ignore


def _load_real_main_window():
	from interface_grafica.windows.main_window import MainWindow as _RealMainWindow

	return _RealMainWindow


class MainWindow:
	"""
	Lazy-constructor shim for the canonical `MainWindow`.

	Importing this module is intentionally cheap: the real implementation from
	`interface_grafica.windows.main_window` is imported only when an instance
	is created. This avoids pulling heavy runtime dependencies (Polars, Oracle
	client, services) at test-collection time.
	"""

	def __new__(cls, *args, **kwargs):
		Real = _load_real_main_window()
		return Real(*args, **kwargs)
