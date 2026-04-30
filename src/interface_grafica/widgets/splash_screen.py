from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor, QFont, QLinearGradient, QPalette
from PySide6.QtWidgets import QProgressBar, QSplashScreen, QVBoxLayout, QWidget, QLabel


class ModernSplashScreen(QSplashScreen):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowFlags(Qt.WindowStaysOnTopHint | Qt.FramelessWindowHint)
        self.setFixedSize(500, 300)

        # Container widget for layout
        self.container = QWidget(self)
        self.container.setFixedSize(500, 300)
        
        layout = QVBoxLayout(self.container)
        layout.setContentsMargins(30, 40, 30, 40)
        layout.setSpacing(10)

        # Title
        self.title_label = QLabel("Fiscal Parquet Analyzer")
        self.title_label.setStyleSheet("font-size: 28px; font-weight: bold; color: #ffffff;")
        self.title_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.title_label)

        # Subtitle
        self.subtitle_label = QLabel("Iniciando serviços...")
        self.subtitle_label.setStyleSheet("font-size: 14px; color: #94a3b8;")
        self.subtitle_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.subtitle_label)

        layout.addStretch()

        # Progress bar
        self.progress = QProgressBar()
        self.progress.setFixedHeight(6)
        self.progress.setTextVisible(False)
        self.progress.setStyleSheet("""
            QProgressBar {
                background-color: #1e293b;
                border-radius: 3px;
                border: none;
            }
            QProgressBar::chunk {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0, stop:0 #3b82f6, stop:1 #2563eb);
                border-radius: 3px;
            }
        """)
        layout.addWidget(self.progress)

        # Status text
        self.status_label = QLabel("Carregando módulos...")
        self.status_label.setStyleSheet("font-size: 11px; color: #64748b;")
        layout.addWidget(self.status_label)

        # Apply gradient background to container
        self.container.setStyleSheet("""
            QWidget {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #0f172a, stop:1 #1e293b);
                border: 1px solid #334155;
                border-radius: 12px;
            }
            QLabel {
                background: transparent;
                border: none;
            }
        """)

    def set_progress(self, value: int, message: str | None = None) -> None:
        self.progress.setValue(value)
        if message:
            self.status_label.setText(message)
        self.repaint()
