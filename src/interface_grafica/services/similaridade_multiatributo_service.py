from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

import polars as pl

from interface_grafica.services.descricao_similarity_service import (
    ordenar_blocos_similaridade_descricao,
)
from utilitarios.text import remove_accents

ALIASES_CODIGO_PRODUTO_FISCAL = [
    "cod_item",
    "Cod_item",
    "COD_ITEM",
    "prod_cprod",
]

CODIGOS_PRODUTO_FISCAIS_FRACOS = {
    "",
    "0",
    "00",
    "000",
    "0000",
    "1",
    "01",
    "001",
    "999",
    "9999",
    "SEM",
    "S/C",
    "NA",
    "N/A",
}


def _normalizar_nome_coluna(nome: str) -> str:
    return (remove_accents(nome) or "").lower().strip()


def _resolver_coluna_codigo_fiscal(df: pl.DataFrame) -> str | None:
    if df.is_empty():
        return None
    cols = list(df.columns)
    for alias in ALIASES_CODIGO_PRODUTO_FISCAL:
        if alias in cols:
            return alias
    normalizadas = {_normalizar_nome_coluna(col): col for col in cols}
    for alias in ALIASES_CODIGO_PRODUTO_FISCAL:
        col = normalizadas.get(_normalizar_nome_coluna(alias))
        if col:
            return col
    return None


def _normalizar_codigo_fiscal(valor: Any) -> str:
    if valor is None:
        return ""
    if isinstance(valor, pl.Series):
        valor = valor.to_list()
    if isinstance(valor, (list, tuple, set)):
        partes = [_normalizar_codigo_fiscal(item) for item in valor]
        return "|".join(sorted({p for p in partes if p}))
    texto = str(valor).strip().upper()
    if not texto:
        return ""
    texto = remove_accents(texto) or ""
    texto = re.sub(r"\s+", "", texto)
    texto = re.sub(r"[^A-Z0-9._/-]", "", texto)
    return texto


def _codigo_forte(codigo: str) -> bool:
    if not codigo or codigo in CODIGOS_PRODUTO_FISCAIS_FRACOS:
        return False
    # Codigos muito curtos geralmente sao sequenciais locais e geram falsos positivos.
    return len(re.sub(r"[^A-Z0-9]", "", codigo)) >= 3


def _codigo_partes(codigo: str) -> set[str]:
    return {parte for parte in codigo.split("|") if _codigo_forte(parte)}


def _extrair_codigos(df: pl.DataFrame) -> list[str]:
    coluna = _resolver_coluna_codigo_fiscal(df)
    if not coluna:
        return [""] * df.height
    return [_normalizar_codigo_fiscal(valor) for valor in df.get_column(coluna).to_list()]


def _linhas_por_codigo(codigos: list[str], max_bucket_size: int) -> dict[str, list[int]]:
    buckets: dict[str, list[int]] = defaultdict(list)
    for idx, codigo in enumerate(codigos):
        for parte in _codigo_partes(codigo):
            buckets[parte].append(idx)
    return {
        codigo: linhas
        for codigo, linhas in buckets.items()
        if 1 < len(linhas) <= max_bucket_size
    }


def _codigo_lookup(codigos: list[str], max_bucket_size: int) -> tuple[list[int | None], list[str]]:
    buckets = _linhas_por_codigo(codigos, max_bucket_size)
    codigo_por_linha = [""] * len(codigos)
    bloco_por_linha: list[int | None] = [None] * len(codigos)
    for bloco_idx, (codigo, linhas) in enumerate(sorted(buckets.items()), start=1):
        for linha in linhas:
            if bloco_por_linha[linha] is None:
                bloco_por_linha[linha] = bloco_idx
                codigo_por_linha[linha] = codigo
    return bloco_por_linha, codigo_por_linha


def ordenar_blocos_similaridade_multiatributo(
    df: pl.DataFrame,
    janela: int = 4,
    limite_bloco: int = 82,
    usar_ncm_cest: bool = True,
    max_bucket_size: int = 250,
) -> pl.DataFrame:
    """Ordena candidatos para agregacao manual usando o score existente + codigo fiscal.

    Esta funcao reaproveita a similaridade de descricao/NCM/CEST/GTIN existente e
    acrescenta `cod_item`/`Cod_item`/`COD_ITEM`/`prod_cprod` como mais uma evidencia.
    Nao executa agregacao automatica.
    """
    if df.is_empty():
        return df

    base = ordenar_blocos_similaridade_descricao(
        df,
        janela=janela,
        limite_bloco=limite_bloco,
        usar_ncm_cest=usar_ncm_cest,
    )
    codigos = _extrair_codigos(base)
    bloco_codigo, codigo_referencia = _codigo_lookup(codigos, max_bucket_size)

    score_codigo = [100 if bloco is not None else None for bloco in bloco_codigo]
    score_base = base.get_column("sim_score").cast(pl.Int64, strict=False).fill_null(0).to_list()
    score_gtin = (
        base.get_column("sim_score_gtin").cast(pl.Int64, strict=False).fill_null(-1).to_list()
        if "sim_score_gtin" in base.columns
        else [-1] * base.height
    )
    score_ncm = (
        base.get_column("sim_score_ncm").cast(pl.Int64, strict=False).fill_null(-1).to_list()
        if "sim_score_ncm" in base.columns
        else [-1] * base.height
    )
    score_desc = (
        base.get_column("sim_score_desc").cast(pl.Int64, strict=False).fill_null(0).to_list()
        if "sim_score_desc" in base.columns
        else [0] * base.height
    )

    score_total: list[int] = []
    riscos: list[str] = []
    motivos_extra: list[str] = []
    for idx, atual in enumerate(score_base):
        bloco = bloco_codigo[idx]
        if bloco is None:
            score_total.append(int(atual))
            riscos.append("")
            motivos_extra.append("")
            continue

        piso = 88
        if score_gtin[idx] == 100:
            piso = 95
        elif score_ncm[idx] >= 100:
            piso = 90
        elif score_ncm[idx] >= 85:
            piso = 88
        elif score_ncm[idx] >= 70:
            piso = 84
        elif score_desc[idx] < 35:
            piso = 78

        score_total.append(max(int(atual), piso))
        if score_desc[idx] < 35 and score_gtin[idx] != 100 and score_ncm[idx] < 70:
            riscos.append("CODIGO_IGUAL_COM_DESCRICAO_DIVERGENTE_SEM_OUTRA_EVIDENCIA")
            motivos_extra.append("CODIGO_FISCAL_DESC_DIVERGENTE")
        else:
            riscos.append("")
            motivos_extra.append("CODIGO_FISCAL_IGUAL")

    motivos_originais = (
        base.get_column("sim_motivos").cast(pl.Utf8, strict=False).fill_null("").to_list()
        if "sim_motivos" in base.columns
        else [""] * base.height
    )
    motivos = []
    for original, extra in zip(motivos_originais, motivos_extra, strict=False):
        if original and extra:
            motivos.append(f"{original}; {extra}")
        elif extra:
            motivos.append(extra)
        else:
            motivos.append(original)

    out = base.with_columns(
        [
            pl.Series("sim_codigo_fiscal_norm", codigos),
            pl.Series("sim_codigo_fiscal_referencia", codigo_referencia),
            pl.Series("sim_bloco_codigo_fiscal", bloco_codigo),
            pl.Series("sim_score_codigo_fiscal", score_codigo),
            pl.Series("sim_score_total", score_total),
            pl.Series("sim_risco_falso_positivo", riscos),
            pl.Series("sim_motivos", motivos),
        ]
    )

    # Mantem blocos de codigo fiscal juntos quando existirem, mas preserva a
    # ordenacao multiatributo original dentro dos demais casos.
    sort_cols = []
    if "sim_bloco_codigo_fiscal" in out.columns:
        sort_cols.append(pl.col("sim_bloco_codigo_fiscal").is_null())
        sort_cols.append(pl.col("sim_bloco_codigo_fiscal").fill_null(10**9))
    sort_cols.extend(
        [
            pl.col("sim_bloco").fill_null(10**9) if "sim_bloco" in out.columns else pl.lit(0),
            pl.col("sim_score_total").fill_null(0) * -1,
            pl.col("sim_chave_ordem") if "sim_chave_ordem" in out.columns else pl.lit(""),
        ]
    )
    return out.sort(sort_cols)
