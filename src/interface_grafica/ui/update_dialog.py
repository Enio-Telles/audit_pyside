from PySide6.QtWidgets import (
    QDialog,
    QVBoxLayout,
    QLabel,
    QTextEdit,
    QDialogButtonBox,
    QProgressBar,
)
from PySide6.QtCore import Qt


class UpdateDialog(QDialog):
    def __init__(self, release_info, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Progresso da Atualização")
        self.setMinimumSize(400, 150)
        self._release_info = release_info

        layout = QVBoxLayout(self)

        self.label = QLabel(f"Baixando versão {release_info.tag_name}...")
        self.label.setStyleSheet("font-weight: bold;")
        layout.addWidget(self.label)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        layout.addWidget(self.progress_bar)

        self.status_label = QLabel("")
        layout.addWidget(self.status_label)

        self.button_box = QDialogButtonBox(QDialogButtonBox.Cancel)
        self.button_box.button(QDialogButtonBox.Cancel).setText("Cancelar")
        self.button_box.rejected.connect(self.reject)
        layout.addWidget(self.button_box)

    def set_downloading(self):
        self.progress_bar.setValue(0)
        self.button_box.setEnabled(True)
        self.status_label.setText("Iniciando download...")

    def set_progress(self, value):
        self.progress_bar.setValue(value)
        self.status_label.setText(f"Baixando: {value}%")

    def set_error(self, message):
        self.status_label.setText(f"Erro: {message}")
        self.status_label.setStyleSheet("color: red;")
        self.button_box.clear()
        self.button_box.addButton(QDialogButtonBox.Retry)
        self.button_box.addButton(QDialogButtonBox.Cancel)
        self.button_box.button(QDialogButtonBox.Retry).setText("Tentar Novamente")
        self.button_box.button(QDialogButtonBox.Cancel).setText("Fechar")

        # Re-connect signals for new buttons
        self.button_box.accepted.disconnect() if self.button_box.accepted.receivers(
            self.accept
        ) > 0 else None
        self.button_box.accepted.connect(self.on_retry)

    def on_retry(self):
        self.status_label.setText("Reiniciando download...")
        self.status_label.setStyleSheet("")
        self.button_box.clear()
        self.button_box.addButton(QDialogButtonBox.Cancel)
        self.button_box.button(QDialogButtonBox.Cancel).setText("Cancelar")

        # Emit signal to parent to restart download
        from interface_grafica.windows.main_window import MainWindow

        if isinstance(self.parent(), MainWindow):
            self.parent().update_service.start_update_download(self._release_info)
