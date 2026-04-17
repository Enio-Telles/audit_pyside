from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import math
import numbers
import re
import unicodedata
from typing import Any

STOPWORDS = {
    "A",
    "AS",
    "O",
    "OS",
    "DE",
    "DA",
    "DO",
    "DAS",
    "DOS",
    "COM",
    "PARA",
    "POR",
    "E",
    "EM",
    "NA",
    "NO",
    "NAS",
    "NOS",
    "UM",
    "UMA",
}


def remove_accents(text: str | None) -> str | None:
    if text is None:
        return None
    normalized = unicodedata.normalize("NFKD", str(text))
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def normalize_text(text: str | None) -> str:
    if text is None:
        return ""
    text = remove_accents(text) or ""
    text = text.upper()
    text = re.sub(r"[^A-Z0-9\s]", " ", text)
    tokens = [token for token in text.split() if token and token not in STOPWORDS]
    return " ".join(tokens)


def normalize_desc(text: str | None) -> str:
    """Versao específica para match de descricoes fiscais (unificada)."""
    if text is None:
        return ""
    # Normalizacao manual de caracteres acentuados para garantir consistencia
    t = str(text).upper()
    chars = {
        "Á": "A",
        "À": "A",
        "Ã": "A",
        "Â": "A",
        "Ä": "A",
        "É": "E",
        "È": "E",
        "Ê": "E",
        "Ë": "E",
        "Í": "I",
        "Ì": "I",
        "Î": "I",
        "Ï": "I",
        "Ó": "O",
        "Ò": "O",
        "Õ": "O",
        "Ô": "O",
        "Ö": "O",
        "Ú": "U",
        "Ù": "U",
        "Û": "U",
        "Ü": "U",
        "Ç": "C",
        "Ñ": "N",
    }
    for c, r in chars.items():
        t = t.replace(c, r)

    # Remove qualquer outro especial e limpa espacos
    t = re.sub(r"[^A-Z0-9\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def expr_normalizar_descricao(coluna: str) -> pl.Expr:
    """Expressao Polars unificada para normalizacao de descricoes."""
    import polars as pl

    return (
        pl.when(pl.col(coluna).is_null())
        .then(pl.lit(""))
        .otherwise(
            pl.col(coluna)
            .cast(pl.Utf8, strict=False)
            .str.to_uppercase()
            .str.replace_all(r"[ÁÀÃÂÄ]", "A")
            .str.replace_all(r"[ÉÈÊË]", "E")
            .str.replace_all(r"[ÍÌÎÏ]", "I")
            .str.replace_all(r"[ÓÒÕÔÖ]", "O")
            .str.replace_all(r"[ÚÙÛÜ]", "U")
            .str.replace_all(r"Ç", "C")
            .str.replace_all(r"Ñ", "N")
            .str.replace_all(r"[^A-Z0-9\s]", " ")
            .str.strip_chars()
            .str.replace_all(r"\s+", " ")
        )
    )


def natural_sort_key(value: str | None) -> list[Any]:
    text = "" if value is None else str(value)
    return [
        int(part) if part.isdigit() else part.lower()
        for part in re.split(r"(\d+)", text)
    ]


def _normalizar_nome_coluna(column_name: str | None) -> str:
    return "" if column_name is None else str(column_name).strip().lower()


def is_year_column_name(column_name: str | None) -> bool:
    nome = _normalizar_nome_coluna(column_name)
    if not nome:
        return False
    return nome == "ano" or nome.startswith("ano_") or nome.endswith("_ano")


def _formatar_numero_br(valor: numbers.Real | Decimal, casas_decimais: int) -> str:
    if isinstance(valor, Decimal):
        numero = float(valor)
    else:
        numero = float(valor)
    texto = f"{numero:,.{casas_decimais}f}"
    return texto.replace(",", "_").replace(".", ",").replace("_", ".")


def _formatar_data(valor: date | datetime) -> str:
    if isinstance(valor, datetime):
        return valor.strftime("%d/%m/%Y %H:%M:%S")
    return valor.strftime("%d/%m/%Y")


def _parse_data_iso(texto: str) -> datetime | date | None:
    texto = texto.strip()
    if not texto:
        return None

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", texto):
        try:
            return datetime.strptime(texto, "%Y-%m-%d").date()
        except ValueError:
            return None

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?(\.\d{1,6})?", texto):
        normalizado = texto.replace("T", " ")
        formatos = ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M")
        for formato in formatos:
            try:
                return datetime.strptime(normalizado, formato)
            except ValueError:
                continue
    return None


def display_cell(value: Any, column_name: str | None = None) -> str:
    if value is None:
        return ""

    # Handle Polars Series or other objects with to_list()
    if hasattr(value, "to_list") and callable(getattr(value, "to_list")):
        try:
            value = value.to_list()
        except Exception:
            pass

    if isinstance(value, (list, tuple)):
        # Join elements, recursively calling display_cell for each
        return ", ".join(
            display_cell(v, column_name=column_name) for v in value if v is not None
        )

    if isinstance(value, bool):
        return "true" if value else "false"

    if isinstance(value, datetime):
        return _formatar_data(value)

    if isinstance(value, date):
        return _formatar_data(value)

    if isinstance(value, str):
        if is_year_column_name(column_name):
            return value.strip()
        valor_data = _parse_data_iso(value)
        if valor_data is not None:
            return _formatar_data(valor_data)
        return value

    if isinstance(value, Decimal):
        if math.isnan(float(value)) or math.isinf(float(value)):
            return ""
        if is_year_column_name(column_name):
            return str(int(value))
        return _formatar_numero_br(value, 2)

    if isinstance(value, numbers.Real):
        numero = float(value)
        if math.isnan(numero) or math.isinf(numero):
            return ""
        if is_year_column_name(column_name):
            return str(int(numero))
        if isinstance(value, numbers.Integral):
            return _formatar_numero_br(value, 0)
        return _formatar_numero_br(value, 2)

    return str(value)
