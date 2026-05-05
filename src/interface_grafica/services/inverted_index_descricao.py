"""Agrupamento de itens apenas por descricao via inverted index.

Estrategia: itens so sao comparados se compartilham pelo menos
N tokens fortes em comum. Tokens com document frequency muito
alto sao podados (LATA, CAIXA, etc nao geram candidatos).

Custo: O(N x T) construcao + O(soma k_bucket^2) Jaccard, onde
T e tokens medios por item e k_bucket e o tamanho de cada
bucket de candidatos.
"""
from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from interface_grafica.services.particionamento_fiscal import _Linha

_LOG = structlog.get_logger(__name__)


CONFIG = {
    "df_max_ratio": 0.05,
    "df_max_absoluto": 500,
    "min_tokens_compartilhados": 2,
    "limite_pares_por_token": 1000,
}


def agrupar_por_inverted_index(
    linhas: list["_Linha"],
    threshold: float = 0.5,
    config: dict | None = None,
) -> list[list["_Linha"]]:
    """Agrupa linhas pelo conteudo da descricao usando inverted index.

    Parameters
    ----------
    linhas : list[_Linha]
        Linhas com atributos `idx` e `tokens` (frozenset[str]).
    threshold : float
        Jaccard minimo (0-1) para unir dois itens.
    config : dict | None
        Override de CONFIG.

    Returns
    -------
    list[list[_Linha]]
        Componentes conexos de itens similares.
    """
    cfg = {**CONFIG, **(config or {})}

    if len(linhas) < 2:
        return [[l] for l in linhas]

    indice: dict[str, list[int]] = defaultdict(list)
    linha_por_idx = {l.idx: l for l in linhas}
    for l in linhas:
        for token in l.tokens:
            indice[token].append(l.idx)

    n = len(linhas)
    df_max = min(
        int(n * cfg["df_max_ratio"]) if n * cfg["df_max_ratio"] >= 2 else 2,
        cfg["df_max_absoluto"],
    )
    bons = {tok: idxs for tok, idxs in indice.items() if len(idxs) <= df_max}

    _LOG.info(
        "inverted_index_construido",
        n_linhas=n,
        n_tokens_total=len(indice),
        n_tokens_apos_poda=len(bons),
        df_max=df_max,
    )

    contagem_por_par: dict[tuple[int, int], int] = defaultdict(int)
    for token, idxs in bons.items():
        if len(idxs) < 2:
            continue
        if len(idxs) > cfg["limite_pares_por_token"]:
            idxs = sorted(idxs)[: cfg["limite_pares_por_token"]]
        idxs_sorted = sorted(set(idxs))
        for i in range(len(idxs_sorted)):
            for j in range(i + 1, len(idxs_sorted)):
                contagem_por_par[(idxs_sorted[i], idxs_sorted[j])] += 1

    candidatos = [
        par for par, c in contagem_por_par.items()
        if c >= cfg["min_tokens_compartilhados"]
    ]

    from interface_grafica.services.particionamento_fiscal import (
        _UnionFind, _jaccard,
    )
    uf = _UnionFind(l.idx for l in linhas)
    for a, b in candidatos:
        sim = _jaccard(linha_por_idx[a].tokens, linha_por_idx[b].tokens)
        if sim >= threshold:
            uf.union(a, b)

    componentes: dict[int, list] = defaultdict(list)
    for l in linhas:
        componentes[uf.find(l.idx)].append(l)
    return list(componentes.values())


def ordenar_blocos_apenas_por_descricao(
    df,
    *,
    threshold: float = 0.5,
    config: dict | None = None,
):
    """Funcao publica standalone: agrupa um DataFrame inteiro
    apenas pela descricao, ignorando NCM/CEST/GTIN/UNIDADE.

    Util para analise exploratoria. Em producao, prefira
    ordenar_blocos_por_particionamento_fiscal com
    incluir_camada_so_descricao=True.
    """
    import polars as pl
    from interface_grafica.services.particionamento_fiscal import (
        _construir_linhas, _resolver_coluna,
        COLUNAS_DESCRICAO,
    )

    if df.is_empty():
        return df.with_columns(
            [
                pl.lit(1, dtype=pl.Int64).alias("sim_bloco"),
                pl.lit("DESC_TOKENS", dtype=pl.Utf8).alias("sim_motivo"),
                pl.lit(5, dtype=pl.Int64).alias("sim_camada"),
                pl.lit("", dtype=pl.Utf8).alias("sim_desc_norm"),
            ]
        )

    col_desc = _resolver_coluna(df, COLUNAS_DESCRICAO)
    if col_desc is None:
        raise ValueError(
            "Nenhuma coluna de descricao encontrada para o agrupamento textual."
        )

    linhas = _construir_linhas(
        df, col_desc,
        col_ncm=None, col_cest=None, col_gtin=None, col_unidade=None,
    )

    componentes = agrupar_por_inverted_index(linhas, threshold, config)

    bloco_por_idx: dict[int, int] = {}
    for bloco_id, comp in enumerate(componentes, start=1):
        for l in comp:
            bloco_por_idx[l.idx] = bloco_id

    n = df.height
    df_out = df.with_columns([
        pl.Series("sim_bloco", [bloco_por_idx.get(i, 0) for i in range(n)]),
        pl.Series("sim_motivo", ["DESC_TOKENS"] * n),
        pl.Series("sim_camada", [5] * n),
        pl.Series("sim_desc_norm", [linhas[i].desc_norm for i in range(n)]),
    ])
    return df_out.sort(["sim_bloco", "sim_desc_norm"])
