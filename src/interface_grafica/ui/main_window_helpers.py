"""Helpers puros (sem Qt) extraidos de main_window_impl.

Modulo importavel sem PySide6 para permitir cobertura de testes na suite padrao.
"""

from __future__ import annotations

import hashlib
import re

import polars as pl


# ---------------------------------------------------------------------------
# Estilo
# ---------------------------------------------------------------------------


def estilo_botao_destacar() -> str:
    """Retorna o stylesheet para botoes de destaque."""
    return (
        "QPushButton { background: #0e639c; color: #ffffff; border: 1px solid #1177bb; "
        "border-radius: 4px; padding: 6px 10px; font-weight: bold; }"
        "QPushButton:hover { background: #1177bb; }"
        "QPushButton:pressed { background: #0b4f7c; }"
    )


# ---------------------------------------------------------------------------
# Filtros / parsing
# ---------------------------------------------------------------------------


def parse_numero_filtro(valor: str) -> float | None:
    """Converte string de filtro numerico para float; retorna None se invalido."""
    bruto = (valor or "").strip()
    if not bruto:
        return None
    try:
        return float(bruto.replace(",", "."))
    except Exception:
        return None


def filtrar_texto_em_colunas(df: pl.DataFrame, texto: str) -> pl.DataFrame:
    """Filtra df mantendo linhas onde alguma coluna texto contem o trecho buscado."""
    texto = (texto or "").strip().lower()
    if not texto or df.is_empty():
        return df

    colunas_busca = [c for c in df.columns if df.schema[c] in [pl.Utf8, pl.Categorical]]
    if not colunas_busca:
        return df

    expr = None
    for col in colunas_busca:
        atual = (
            pl.col(col)
            .cast(pl.Utf8, strict=False)
            .fill_null("")
            .str.to_lowercase()
            .str.contains(texto, literal=True)
        )
        expr = atual if expr is None else (expr | atual)
    return df.filter(expr) if expr is not None else df


def filtrar_intervalo_numerico(
    df: pl.DataFrame,
    coluna: str | None,
    valor_min: str,
    valor_max: str,
) -> pl.DataFrame:
    """Filtra df por intervalo numerico em coluna; ignora limites vazios."""
    if not coluna or coluna not in df.columns:
        return df

    minimo = parse_numero_filtro(valor_min)
    maximo = parse_numero_filtro(valor_max)
    if minimo is None and maximo is None:
        return df

    expr_col = pl.col(coluna).cast(pl.Float64, strict=False)
    if minimo is not None:
        df = df.filter(expr_col >= minimo)
    if maximo is not None:
        df = df.filter(expr_col <= maximo)
    return df


# ---------------------------------------------------------------------------
# Formatacao de textos de resumo
# ---------------------------------------------------------------------------


def formatar_resumo_filtros(pares: list[tuple[str, str]]) -> str:
    """Formata lista de (rotulo, valor) como string de filtros ativos."""
    ativos = [f"{rotulo}: {valor}" for rotulo, valor in pares if valor]
    return "Filtros ativos: " + (" | ".join(ativos) if ativos else "nenhum")


# ---------------------------------------------------------------------------
# Tokenizacao de termos de busca rapida
# ---------------------------------------------------------------------------


def split_terms(value: str) -> list[str]:
    """Divide string de busca em termos individuais.

    Separa por ponto-e-virgula, virgula ou dois ou mais espacos.
    Se o resultado for um unico token contendo espaco, separa por espaco simples.
    Exemplos: "buch 18", "buch;18" ou "buch, 18".
    """
    texto = (value or "").strip()
    if not texto:
        return []
    partes = re.split(r"[;,]+|\s{2,}", texto)
    if len(partes) == 1 and " " in texto:
        partes = texto.split()
    return [p.strip() for p in partes if p and p.strip()]


# ---------------------------------------------------------------------------
# Resolvedores de cor — aba mensal
# ---------------------------------------------------------------------------


def aba_mensal_foreground(row: dict, _col_name: str) -> str | None:
    """Cor de texto para linha da aba mensal."""
    entradas_desacob = float(row.get("entradas_desacob") or 0)
    icms_entr = float(row.get("ICMS_entr_desacob") or 0)
    if entradas_desacob > 0 or icms_entr > 0:
        return "#fff7ed"
    return "#f5f5f5"


def aba_mensal_background(row: dict, _col_name: str) -> str | None:
    """Cor de fundo para linha da aba mensal."""
    entradas_desacob = float(row.get("entradas_desacob") or 0)
    icms_entr = float(row.get("ICMS_entr_desacob") or 0)
    if entradas_desacob > 0 or icms_entr > 0:
        return "#5b3a06"
    mes = int(row.get("mes") or 0)
    return "#1f1f1f" if (mes % 2) == 0 else "#262626"


# ---------------------------------------------------------------------------
# Resolvedores de cor — aba anual
# ---------------------------------------------------------------------------


def aba_anual_foreground(row: dict, _col_name: str) -> str | None:
    """Cor de texto para linha da aba anual."""
    entradas_desacob = float(row.get("entradas_desacob") or 0)
    saidas_desacob = float(row.get("saidas_desacob") or 0)
    estoque_final_desacob = float(row.get("estoque_final_desacob") or 0)
    if entradas_desacob > 0 or saidas_desacob > 0 or estoque_final_desacob > 0:
        return "#fff7ed"
    return "#f5f5f5"


def aba_anual_background(row: dict, _col_name: str) -> str | None:
    """Cor de fundo para linha da aba anual."""
    entradas_desacob = float(row.get("entradas_desacob") or 0)
    saidas_desacob = float(row.get("saidas_desacob") or 0)
    estoque_final_desacob = float(row.get("estoque_final_desacob") or 0)
    if entradas_desacob > 0 or saidas_desacob > 0 or estoque_final_desacob > 0:
        return "#5b3a06"
    val = str(row.get("id_agregado", ""))
    h = int(hashlib.md5(val.encode()).hexdigest(), 16)
    return "#1f1f1f" if (h % 2) == 0 else "#262626"


# ---------------------------------------------------------------------------
# Resolvedores de cor — movimentacao de estoque
# ---------------------------------------------------------------------------

_EXCLUIR_VALS = {"TRUE", "1", "S", "Y", "SIM"}
_MOV_REP_VALS = {"TRUE", "1", "S", "Y", "SIM"}


def mov_estoque_foreground(row: dict, _col_name: str) -> str | None:
    """Cor de texto para linha de movimentacao de estoque."""
    tipo = str(row.get("Tipo_operacao") or "").upper()
    if float(row.get("entr_desac_anual") or 0) > 0:
        return "#fdba74"
    if str(row.get("excluir_estoque", "")).strip().upper() in _EXCLUIR_VALS:
        return "#94a3b8"
    if "ESTOQUE FINAL" in tipo:
        return "#fde68a"
    if "ESTOQUE INICIAL" in tipo:
        return "#bfdbfe"
    if "ENTRADA" in tipo:
        return "#93c5fd"
    if "SAIDA" in tipo:
        return "#fca5a5"
    return None


def mov_estoque_background(row: dict, _col_name: str) -> str | None:
    """Cor de fundo para linha de movimentacao de estoque."""
    tipo = str(row.get("Tipo_operacao") or "").upper()
    if float(row.get("entr_desac_anual") or 0) > 0:
        return "#431407"
    if str(row.get("excluir_estoque", "")).strip().upper() in _EXCLUIR_VALS:
        return "#1e293b"
    if str(row.get("mov_rep", "")).strip().upper() in _MOV_REP_VALS:
        return "#111827"
    if "ESTOQUE FINAL" in tipo:
        return "#3f2f10"
    if "ESTOQUE INICIAL" in tipo:
        return "#0f172a"
    if "ENTRADA" in tipo:
        return "#10213f"
    if "SAIDA" in tipo:
        return "#3b1212"
    return None
