from __future__ import annotations

import re

import polars as pl


def _limpar_parte(valor: str | None) -> str:
    """Normaliza uma parte textual usada no codigo de fonte."""
    texto = str(valor or "").strip()
    texto = re.sub(r"\s+", " ", texto)
    return texto


def gerar_codigo_fonte(cnpj: str | None, codigo: str | None, descricao: str | None = None) -> str | None:
    """Gera o identificador de fonte combinando CNPJ, codigo e descricao quando disponiveis."""
    cnpj_limpo = re.sub(r"\D", "", str(cnpj or ""))
    codigo_limpo = _limpar_parte(codigo)
    if not codigo_limpo:
        return None

    # Normalizacao da descricao se fornecida
    desc_parte = ""
    if descricao is not None:
        from utilitarios.text import normalize_desc

        desc_norm = normalize_desc(descricao)
        if desc_norm:
            desc_parte = f"|{desc_norm}"

    if cnpj_limpo:
        return f"{cnpj_limpo}|{codigo_limpo}{desc_parte}"
    return f"{codigo_limpo}{desc_parte}"


def normalizar_codigo_fonte(valor: str | None) -> str | None:
    """Normaliza um codigo de fonte para o formato canonico (suporta 2 ou 3 partes)."""
    texto = _limpar_parte(valor)
    if not texto:
        return None
    if "|" not in texto:
        return texto

    partes = texto.split("|")
    if len(partes) < 2:
        return texto

    cnpj_raw = partes[0]
    codigo_raw = partes[1]
    desc_raw = partes[2] if len(partes) > 2 else None

    cnpj_limpo = re.sub(r"\D", "", cnpj_raw)
    codigo_limpo = _limpar_parte(codigo_raw)

    res = ""
    if cnpj_limpo and codigo_limpo:
        res = f"{cnpj_limpo}|{codigo_limpo}"
    elif codigo_limpo:
        res = codigo_limpo
    else:
        return None

    if desc_raw:
        from utilitarios.text import normalize_desc

        desc_norm = normalize_desc(desc_raw)
        if desc_norm:
            res += f"|{desc_norm}"

    return res


def expr_normalizar_codigo_fonte(col: str, alias: str = "codigo_fonte") -> pl.Expr:
    """Cria expressao Polars para normalizar uma coluna de codigo de fonte."""
    from utilitarios.text import expr_normalizar_descricao

    raw = (
        pl.col(col)
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.strip_chars()
        .str.replace_all(r"\s+", " ")
        .str.replace_all(r"\s*\|\s*", "|")
    )

    has_pipe = raw.str.contains("|", literal=True)
    parts = raw.str.split("|")

    cnpj_clean = parts.list.get(0, null_on_oob=True).fill_null("").str.replace_all(r"\D", "")
    code_clean = (
        parts.list.get(1, null_on_oob=True)
        .fill_null("")
        .str.strip_chars()
        .str.replace_all(r"\s+", " ")
    )
    desc_raw = parts.list.get(2, null_on_oob=True).fill_null("")
    desc_clean = expr_normalizar_descricao(desc_raw)

    base = (
        pl.when(code_clean == "")
        .then(pl.lit(None, dtype=pl.Utf8))
        .when(cnpj_clean != "")
        .then(pl.concat_str([cnpj_clean, pl.lit("|"), code_clean]))
        .otherwise(code_clean)
    )

    with_desc = (
        pl.when((desc_clean != "") & base.is_not_null())
        .then(pl.concat_str([base, pl.lit("|"), desc_clean]))
        .otherwise(base)
    )

    return (
        pl.when(raw == "")
        .then(pl.lit(None, dtype=pl.Utf8))
        .when(~has_pipe)
        .then(raw)
        .otherwise(with_desc)
        .alias(alias)
    )


def expr_gerar_codigo_fonte(
    col_cnpj: str | pl.Expr, col_codigo: str | pl.Expr, col_descricao: str | pl.Expr | None = None, alias: str = "codigo_fonte"
) -> pl.Expr:
    """Cria expressao Polars para gerar codigo de fonte a partir de CNPJ, codigo e descricao."""
    def _to_expr(c):
        if isinstance(c, str):
            return pl.col(c)
        return c

    cnpj_expr = _to_expr(col_cnpj).cast(pl.Utf8).fill_null("").str.replace_all(r"\D", "")
    cod_expr = (
        _to_expr(col_codigo)
        .cast(pl.Utf8)
        .fill_null("")
        .str.strip_chars()
        .str.replace_all(r"\s+", " ")
    )

    base_expr = (
        pl.when(cod_expr == "")
        .then(pl.lit(None))
        .when(cnpj_expr != "")
        .then(pl.concat_str([cnpj_expr, pl.lit("|"), cod_expr]))
        .otherwise(cod_expr)
    )

    if col_descricao is not None:
        from utilitarios.text import expr_normalizar_descricao

        desc_norm_expr = expr_normalizar_descricao(_to_expr(col_descricao))
        return (
            pl.when(desc_norm_expr != "")
            .then(pl.concat_str([base_expr, pl.lit("|"), desc_norm_expr]))
            .otherwise(base_expr)
            .alias(alias)
        )

    return base_expr.alias(alias)
