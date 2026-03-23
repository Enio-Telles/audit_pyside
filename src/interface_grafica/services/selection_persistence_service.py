from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from interface_grafica.config import SELECTIONS_FILE


class SelectionPersistenceService:
    """Serviço para persistir seleções da UI (consultas, tabelas, etc.)."""

    def __init__(self, file_path: Path = SELECTIONS_FILE) -> None:
        self.file_path = file_path
        self._cache: dict[str, list[str]] = {}
        self._load()

    def _load(self) -> None:
        if not self.file_path.exists():
            self._cache = {}
            return
        try:
            self._cache = json.loads(self.file_path.read_text(encoding="utf-8"))
        except Exception:
            self._cache = {}

    def _save(self) -> None:
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.file_path.write_text(json.dumps(self._cache, indent=2, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass

    def get_selections(self, category: str) -> list[str]:
        """Retorna a lista de itens selecionados para uma categoria."""
        return self._cache.get(category, [])

    def set_selections(self, category: str, items: list[str]) -> None:
        """Salva a lista de itens selecionados para uma categoria."""
        self._cache[category] = items
        self._save()
