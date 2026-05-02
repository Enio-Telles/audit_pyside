"""Snapshot da normalizacao de descricao antes da vetorizacao nativa.

Origem: `git show origin/main:src/utilitarios/text.py`, antes da PR #181.
"""
import re
import unicodedata

import polars as pl


def remove_accents(text: str | None) -> str | None:
    """Remove acentos de um texto preservando `None`."""
    if text is None:
        return None
    normalized = unicodedata.normalize("NFKD", str(text))
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def normalize_desc(text: str | None) -> str:
    """Normalizacao canonica de descricao fiscal pre-vetorizacao."""
    if text is None:
        return ""
    t = remove_accents(str(text)) or ""
    t = t.upper()
    t = re.sub(r"(?<=\w)[-/]|[-/](?=\w)", " ", t)
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 -%$#@!.,}{][/\\;")
    t = "".join(ch if ch in allowed else " " for ch in t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def expr_normalizar_descricao(coluna: str | pl.Expr) -> pl.Expr:
    """Expressao Polars antiga baseada em `map_elements`."""
    col = pl.col(coluna) if isinstance(coluna, str) else coluna

    return (
        pl.when(col.is_null())
        .then(pl.lit(""))
        .otherwise(
            col.cast(pl.Utf8, strict=False).map_elements(
                lambda s: normalize_desc(
                    re.sub(r"(?<=\w)[-/]|[-/](?=\w)", " ", s)
                ),
                return_dtype=pl.Utf8,
            )
        )
    )
