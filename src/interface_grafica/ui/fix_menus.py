import os
from utilitarios.project_paths import SRC_ROOT

path = str(SRC_ROOT / 'interface_grafica' / 'ui' / 'main_window.py')
with open(path, 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace(
    'self.table_view.horizontalHeader().setStretchLastSection(True)\n        layout.addWidget(self.table_view, 1)',
    'self.table_view.horizontalHeader().setStretchLastSection(True)\n        self.table_view.setContextMenuPolicy(Qt.CustomContextMenu)\n        self.table_view.customContextMenuRequested.connect(self._on_table_context_menu)\n        layout.addWidget(self.table_view, 1)'
)

text = text.replace(
    'self.results_table_view.setStyleSheet("QTableView::item { padding: 4px 2px; }")\n        bottom_layout.addWidget(self.results_table_view, 1)',
    'self.results_table_view.setStyleSheet("QTableView::item { padding: 4px 2px; }")\n        self.results_table_view.setContextMenuPolicy(Qt.CustomContextMenu)\n        self.results_table_view.customContextMenuRequested.connect(self._on_results_context_menu)\n        bottom_layout.addWidget(self.results_table_view, 1)'
)

with open(path, 'w', encoding='utf-8') as f:
    f.write(text)
print("Fix aplicado com sucesso!")
