"""Normalizacao de unidades de medida em descricoes fiscais.

Este modulo converte expressoes equivalentes de volume e massa para uma
forma canonica antes da extracao de numeros usada no calculo de similaridade.

Exemplos:
    "0,5L"      -> "500ML"
    "1,5 L"     -> "1500ML"
    "1KG"       -> "1000G"
    "1.5 KG"    -> "1500G"
    "350 ML"    -> "350ML"

O proposito e fazer com que descricoes equivalentes em forma diferente
gerem o mesmo conjunto de numeros, evitando falsos negativos no score
sim_score_numeros.

Nao faz parte do pipeline canonico de descricao fiscal (preserva-se a
descricao original). E aplicado apenas no fluxo interno de similaridade.
"""
from __future__ import annotations

import re

# Capturadores de quantidade + unidade. A ordem importa: KG antes de G,
# ML antes de L, para evitar match parcial quando o unico cuidado e
# garantir que a unidade nao e prefixo de outra.
_PADROES = [
    # Volume: L -> ML
    (
        re.compile(r"(\d+(?:[.,]\d+)?)\s*(?:LITROS?|LITRO|LT|L)\b", re.IGNORECASE),
        lambda valor: f"{int(round(valor * 1000))}ML",
    ),
    # Volume: ML (apenas normaliza a forma "350 ML" -> "350ML")
    (
        re.compile(r"(\d+(?:[.,]\d+)?)\s*(?:MILILITROS?|ML)\b", re.IGNORECASE),
        lambda valor: f"{int(round(valor))}ML" if valor == int(valor) else f"{valor:.3f}ML".rstrip("0").rstrip("."),
    ),
    # Massa: KG -> G
    (
        re.compile(r"(\d+(?:[.,]\d+)?)\s*(?:QUILOS?|QUILO|KILOS?|KILO|KG)\b", re.IGNORECASE),
        lambda valor: f"{int(round(valor * 1000))}G",
    ),
    # Massa: G (normaliza forma "100 G" -> "100G", "100GR" -> "100G")
    (
        re.compile(r"(\d+(?:[.,]\d+)?)\s*(?:GRAMAS?|GRAMA|GR|G)\b", re.IGNORECASE),
        lambda valor: f"{int(round(valor))}G" if valor == int(valor) else f"{valor:.3f}G".rstrip("0").rstrip("."),
    ),
]


def _converter_valor(texto: str) -> float:
    """Converte 'NN[,.]NN' para float."""
    return float(texto.replace(",", "."))


def normalizar_unidades_em_texto(texto: str) -> str:
    """Substitui expressoes de unidade equivalentes pela forma canonica.

    Trabalha caso-insensitivo na entrada e emite saida em maiusculas.
    Multiplas ocorrencias na mesma string sao todas convertidas.

    Args:
        texto: descricao em qualquer forma (com ou sem normalizacao previa).

    Returns:
        Texto com unidades canonizadas. Mantem o restante intacto.
    """
    if not texto:
        return texto

    resultado = texto
    for padrao, formatador in _PADROES:
        def _sub(match: re.Match[str], _formatador=formatador) -> str:
            try:
                valor = _converter_valor(match.group(1))
            except (ValueError, IndexError):
                return match.group(0)
            return _formatador(valor)

        resultado = padrao.sub(_sub, resultado)
    return resultado
