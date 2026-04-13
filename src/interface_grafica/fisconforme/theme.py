from __future__ import annotations


COLORS = {
    "primary": "#12355B",
    "secondary": "#2E86AB",
    "accent": "#F18F01",
    "success": "#2D9C5A",
    "warning": "#C87D12",
    "danger": "#D64545",
    "bg": "#F4F7FB",
    "card": "#FFFFFF",
    "surface": "#EAF0F6",
    "border": "#D5DEE8",
    "text": "#22313F",
    "muted": "#5F7183",
    "sidebar": "#11263D",
    "sidebar_active": "#1C4E78",
    "hover": "#E6F1FA",
    "log_bg": "#17212B",
    "log_text": "#D8E7F5",
}

SPACING = {
    "xs": 4,
    "sm": 8,
    "md": 12,
    "lg": 18,
    "xl": 24,
}

WINDOW_MIN_WIDTH = 1120
WINDOW_MIN_HEIGHT = 660
SIDEBAR_WIDTH = 248
ADAPTIVE_TWO_COLUMN_BREAKPOINT = 860


def build_stylesheet() -> str:
    return f"""
    QMainWindow {{
        background-color: {COLORS['bg']};
    }}
    QWidget {{
        background-color: transparent;
        color: {COLORS['text']};
        font-family: 'Segoe UI', 'Arial', sans-serif;
        font-size: 12px;
    }}
    QLabel#PageTitle {{
        font-size: 21px;
        font-weight: 700;
        color: {COLORS['primary']};
    }}
    QLabel#PageSubtitle {{
        font-size: 12px;
        color: {COLORS['muted']};
    }}
    QWidget#Sidebar {{
        background-color: {COLORS['sidebar']};
        min-width: {SIDEBAR_WIDTH}px;
        max-width: {SIDEBAR_WIDTH}px;
    }}
    QLabel#SidebarTitle {{
        color: #FFFFFF;
        font-size: 15px;
        font-weight: 700;
    }}
    QLabel#SidebarSubtitle {{
        color: #A8BCD0;
        font-size: 10px;
    }}
    QPushButton[step="true"] {{
        text-align: left;
        padding: 10px 12px;
        border-radius: 8px;
        border: none;
        color: #A8BCD0;
        font-weight: 600;
    }}
    QPushButton[step="true"]:hover {{
        background-color: rgba(255,255,255,0.06);
    }}
    QPushButton[stepState="active"] {{
        background-color: {COLORS['sidebar_active']};
        color: #FFFFFF;
    }}
    QPushButton[stepState="done"] {{
        background-color: rgba(45,156,90,0.15);
        color: #90D2AA;
    }}
    QFrame#SidebarSummaryCard {{
        background-color: rgba(255,255,255,0.06);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 14px;
    }}
    QLabel#SidebarSummaryLabel {{
        color: #8EA4B9;
        font-size: 10px;
        font-weight: 600;
    }}
    QLabel#SidebarSummaryValue {{
        color: #FFFFFF;
        font-size: 12px;
        font-weight: 700;
    }}
    QWidget#ContentFrame {{
        background-color: {COLORS['bg']};
    }}
    QFrame#ContentHeader,
    QFrame#FooterBar {{
        background-color: {COLORS['card']};
        border-bottom: 1px solid {COLORS['border']};
    }}
    QFrame#FooterBar {{
        border-bottom: none;
        border-top: 1px solid {COLORS['border']};
    }}
    QScrollArea {{
        border: none;
    }}
    QGroupBox {{
        background-color: {COLORS['card']};
        border: 1px solid {COLORS['border']};
        border-radius: 12px;
        margin-top: 8px;
        padding: 16px;
        font-size: 12px;
        font-weight: 700;
        color: {COLORS['primary']};
    }}
    QGroupBox::title {{
        subcontrol-origin: margin;
        left: 14px;
        padding: 0 8px;
        background-color: {COLORS['card']};
    }}
    QPushButton {{
        background-color: {COLORS['card']};
        border: 1px solid {COLORS['border']};
        border-radius: 8px;
        padding: 8px 14px;
        color: {COLORS['text']};
        font-size: 12px;
        min-height: 18px;
    }}
    QPushButton:hover {{
        background-color: {COLORS['hover']};
        border-color: {COLORS['secondary']};
    }}
    QPushButton#PrimaryButton {{
        background-color: {COLORS['primary']};
        color: #FFFFFF;
        border: none;
        font-weight: 700;
    }}
    QPushButton#PrimaryButton:hover {{
        background-color: {COLORS['secondary']};
    }}
    QPushButton#PrimaryButton:disabled {{
        background-color: #A1B3C5;
    }}
    QPushButton#SuccessButton {{
        background-color: {COLORS['success']};
        color: #FFFFFF;
        border: none;
        font-weight: 700;
    }}
    QPushButton#SuccessButton:hover {{
        background-color: #35AF67;
    }}
    QPushButton#DangerButton {{
        background-color: {COLORS['danger']};
        color: #FFFFFF;
        border: none;
        font-weight: 700;
    }}
    QPushButton#DangerButton:hover {{
        background-color: #E25757;
    }}
    QPushButton#SecondaryButton {{
        background-color: {COLORS['secondary']};
        color: #FFFFFF;
        border: none;
        font-weight: 700;
    }}
    QPushButton#SecondaryButton:hover {{
        background-color: #3C9DC6;
    }}
    QLineEdit,
    QComboBox,
    QTextEdit {{
        background-color: {COLORS['card']};
        border: 1px solid {COLORS['border']};
        border-radius: 8px;
        padding: 8px 10px;
        font-size: 12px;
        color: {COLORS['text']};
    }}
    QLineEdit:focus,
    QComboBox:focus,
    QTextEdit:focus {{
        border: 2px solid {COLORS['secondary']};
        padding: 9px 11px;
    }}
    QComboBox::drop-down {{
        border: none;
        width: 26px;
    }}
    QComboBox QAbstractItemView {{
        background-color: {COLORS['card']};
        border: 1px solid {COLORS['border']};
        selection-background-color: {COLORS['hover']};
        selection-color: {COLORS['text']};
    }}
    QTableView {{
        background-color: {COLORS['card']};
        alternate-background-color: #F8FBFE;
        border: 1px solid {COLORS['border']};
        border-radius: 10px;
        gridline-color: {COLORS['border']};
        selection-background-color: {COLORS['hover']};
        selection-color: {COLORS['text']};
    }}
    QHeaderView::section {{
        background-color: {COLORS['primary']};
        color: #FFFFFF;
        padding: 8px 10px;
        border: none;
        font-size: 11px;
        font-weight: 700;
    }}
    QProgressBar {{
        background-color: #E2EAF2;
        border: 1px solid {COLORS['border']};
        border-radius: 8px;
        min-height: 24px;
        text-align: center;
        color: {COLORS['primary']};
        font-weight: 700;
    }}
    QProgressBar::chunk {{
        border-radius: 7px;
        background-color: {COLORS['secondary']};
    }}
    QFrame#StatusBanner {{
        border-radius: 10px;
        border: 1px solid {COLORS['border']};
        background-color: {COLORS['surface']};
    }}
    QLabel[statusRole="info"] {{
        color: {COLORS['secondary']};
    }}
    QLabel[statusRole="success"] {{
        color: {COLORS['success']};
    }}
    QLabel[statusRole="warning"] {{
        color: {COLORS['warning']};
    }}
    QLabel[statusRole="danger"] {{
        color: {COLORS['danger']};
    }}
    QFrame#MetadataGrid {{
        background-color: {COLORS['card']};
        border: 1px solid {COLORS['border']};
        border-radius: 10px;
    }}
    QLabel#MetadataKey {{
        color: {COLORS['muted']};
        font-size: 10px;
        font-weight: 600;
    }}
    QLabel#MetadataValue {{
        color: {COLORS['text']};
        font-size: 13px;
        font-weight: 700;
    }}
    QTextEdit#LogOutput {{
        background-color: {COLORS['log_bg']};
        color: {COLORS['log_text']};
        font-family: 'Cascadia Code', 'Consolas', monospace;
        font-size: 11px;
    }}
    QMessageBox {{
        background-color: {COLORS['sidebar']};
    }}
    QMessageBox QLabel {{
        color: #FFFFFF;
        font-size: 13px;
    }}
    """
