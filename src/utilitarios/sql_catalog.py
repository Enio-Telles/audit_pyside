from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from utilitarios.project_paths import SQL_ARCHIVE_ROOT, SQL_ROOT

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SqlCatalogEntry:
    sql_id: str
    path: Path

    @property
    def display_name(self) -> str:
        return self.path.stem

    @property
    def source_label(self) -> str:
        parent = self.path.parent.relative_to(SQL_ROOT)
        return parent.as_posix()


def _normalizar_texto_relativo(value: str) -> str:
    return value.replace("\\", "/").strip().lstrip("./")


_SQL_ARQUIVOS_PARQUET_ROOT = SQL_ROOT / "arquivos_parquet"


def _iter_sql_paths(include_archive: bool = False) -> Iterable[Path]:
    if not SQL_ROOT.exists():
        return []

    resultado: list[Path] = []
    for path in SQL_ROOT.rglob("*"):
        if not path.is_file() or path.suffix.lower() != ".sql":
            continue
        # Excluir a subpasta arquivos_parquet/ (SQLs atomizados)
        try:
            path.relative_to(_SQL_ARQUIVOS_PARQUET_ROOT)
            continue
        except ValueError:
            pass
        if not include_archive:
            try:
                path.relative_to(SQL_ARCHIVE_ROOT)
                continue
            except ValueError:
                pass
            if any(parte.lower() == "referencia" for parte in path.parts):
                continue
        resultado.append(path)
    return resultado


def list_sql_entries(include_archive: bool = False) -> list[SqlCatalogEntry]:
    entries = [
        SqlCatalogEntry(sql_id=path.relative_to(SQL_ROOT).as_posix(), path=path)
        for path in _iter_sql_paths(include_archive=include_archive)
    ]
    return sorted(entries, key=lambda item: item.sql_id.lower())


def get_sql_id(path: Path | str) -> str | None:
    candidate = Path(path)
    try:
        return candidate.resolve().relative_to(SQL_ROOT.resolve()).as_posix()
    except Exception:
        return None


def _index_entries() -> tuple[dict[str, SqlCatalogEntry], dict[str, list[SqlCatalogEntry]]]:
    by_id: dict[str, SqlCatalogEntry] = {}
    by_name: dict[str, list[SqlCatalogEntry]] = {}
    for entry in list_sql_entries():
        by_id[entry.sql_id.lower()] = entry
        by_name.setdefault(entry.path.name.lower(), []).append(entry)
    return by_id, by_name


def normalize_sql_id(value: Path | str | None) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    by_id, by_name = _index_entries()
    normalized_text = _normalizar_texto_relativo(text)
    direct = by_id.get(normalized_text.lower())
    if direct is not None:
        return direct.sql_id

    path_value = Path(text)
    direct_from_path = get_sql_id(path_value)
    if direct_from_path is not None:
        return direct_from_path

    lowered_path = text.lower().replace("/", "\\")
    for marker in ("\\sql\\", "\\consultas_fonte\\"):
        if marker in lowered_path:
            suffix = lowered_path.split(marker, 1)[1]
            suffix_match = _normalizar_texto_relativo(suffix)
            direct = by_id.get(suffix_match.lower())
            if direct is not None:
                return direct.sql_id
            text = text.split(marker, 1)[1]
            break

    candidate_name = Path(text).name.lower()
    matches = by_name.get(candidate_name, [])
    if len(matches) == 1:
        return matches[0].sql_id

    candidate_suffix = _normalizar_texto_relativo(text).lower()
    suffix_matches = [entry for entry in by_id.values() if entry.sql_id.lower().endswith(candidate_suffix)]
    if len(suffix_matches) == 1:
        return suffix_matches[0].sql_id

    return None


def resolve_sql_path(value: Path | str) -> Path:
    sql_id = normalize_sql_id(value)
    if sql_id is None:
        raise FileNotFoundError(f"SQL nao encontrada no catalogo local: {value}")
    path = SQL_ROOT / Path(sql_id)
    if not path.exists():
        raise FileNotFoundError(f"SQL nao encontrada no caminho resolvido: {sql_id}")
    return path


def migrate_sql_id_list(values: list[str] | None, *, log_context: str = "sql") -> list[str]:
    migrated: list[str] = []
    for item in values or []:
        sql_id = normalize_sql_id(item)
        if sql_id is None:
            logger.warning("Ignorando selecao legada sem correspondencia (%s): %s", log_context, item)
            continue
        if sql_id not in migrated:
            migrated.append(sql_id)
    return migrated
