from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from interface_grafica.config import DEFAULT_PAGE_SIZE
from interface_grafica.services.parquet_service import FilterCondition


@dataclass
class ViewState:
    current_cnpj: str | None = None
    current_file: Path | None = None
    current_page: int = 1
    page_size: int = DEFAULT_PAGE_SIZE
    all_columns: list[str] | None = None
    visible_columns: list[str] | None = None
    filters: list[FilterCondition] | None = None
    total_rows: int = 0
