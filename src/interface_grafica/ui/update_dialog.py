from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QLabel, QTextEdit,
    QDialogButtonBox, QProgressBar
)
from PySide6.QtCore import Qt

class UpdateDialog(QDialog):
    def __init__(self, release_info, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Atualização Disponível")
        self.setMinimumSize(400, 300)

        layout = QVBoxLayout(self)

        label = QLabel(f"Uma nova versão ({release_info.tag_name}) está disponível.")
        label.setStyleSheet("font-weight: bold;")
        layout.addWidget(label)

        layout.addWidget(QLabel("Notas de release:"))

        self.notes_edit = QTextEdit()
        self.notes_edit.setPlainText(release_info.body)
        self.notes_edit.setReadOnly(True)
        layout.addWidget(self.notes_edit)

        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)

        self.status_label = QLabel("")
        layout.addWidget(self.status_label)

        self.button_box = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel
        )
        self.button_box.button(QDialogButtonBox.Ok).setText("Atualizar")
        self.button_box.button(QDialogButtonBox.Cancel).setText("Mais tarde")

        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)

        layout.addWidget(self.button_box)

    def set_downloading(self):
        self.progress_bar.setVisible(True)
        self.button_box.setEnabled(False)
        self.status_label.setText("Baixando atualização...")

    def set_progress(self, value):
        self.progress_bar.setValue(value)

    def set_error(self, message):
        self.status_label.setText(f"Erro: {message}")
        self.status_label.setStyleSheet("color: red;")
        self.button_box.setEnabled(True)
        self.button_box.button(QDialogButtonBox.Ok).setText("Tentar Novamente")
