"""Metodologia de similaridade por particionamento fiscal.

Particiona o DataFrame por identificadores fiscais (GTIN, NCM,
CEST, unidade) e compara descricoes apenas dentro de cada
particao. Resulta em pipeline previsivel, sem comparacao N x N
direta, com configuracao minima.

Filosofia: 'ordenar != agrupar'. Esta funcao apenas reorganiza o
DataFrame e adiciona colunas indicadoras. Nao agrega, nao salva
arquivos e nao altera identificadores fiscais.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import re
from typing import Iterable

import polars as pl
import structlog

from utilitarios.text import normalize_desc, remove_accents
from utilitarios.unidades_descricao import normalizar_unidades_em_texto

_LOG = structlog.get_logger(__name__)


COLUNAS_DESCRICAO = [
    "descr_padrao", "descricao_normalizada", "descricao",
    "descricao_final", "lista_descricoes", "lista_itens_agrupados",
]
ALIASES_NCM = ["ncm_padrao", "NCM_padrao", "lista_ncm", "ncm_final", "ncm"]
ALIASES_CEST = ["cest_padrao", "CEST_padrao", "lista_cest", "cest_final", "cest"]
ALIASES_GTIN = ["gtin_padrao", "GTIN_padrao", "lista_gtin", "gtin", "cod_barra", "cod_barras"]
ALIASES_UNIDADE = ["unid_padrao", "unidade_padrao", "unid", "unidade", "un"]


THRESHOLDS_DEFAULT = {
    "camada_1": 50,
    "camada_2": 65,
    "camada_3": 80,
    "max_bucket_size": 200,
    "min_jaccard_para_par": 0.30,
}


@dataclass(frozen=True)
class _Linha:
    idx: int
    desc_norm: str
    tokens: frozenset[str]
    ncm: str
    ncm4: str
    cest: str
    gtin: str
    unidade: str


def _normalizar_codigo(valor: object) -> str:
    if valor is None:
        return ""
    if isinstance(valor, (list, tuple, set)):
        partes = [_normalizar_codigo(v) for v in valor]
        return "|".join(sorted({p for p in partes if p}))
    texto = str(valor).strip().upper()
    if not texto:
        return ""
    texto = remove_accents(texto) or ""
    return re.sub(r"\s+", "", texto)


def _normalizar_ncm(valor: object) -> str:
    bruto = _normalizar_codigo(valor)
    digitos = re.sub(r"\D", "", bruto)
    return digitos[:8]


def _ncm_quatro_digitos(ncm: str) -> str:
    return ncm[:4] if len(ncm) >= 4 else ""


def _resolver_coluna(df: pl.DataFrame, aliases: list[str]) -> str | None:
    if df.is_empty():
        return None
    cols = list(df.columns)
    for alias in aliases:
        if alias in cols:
            return alias
    norm = lambda s: (remove_accents(s) or "").lower().strip()
    cols_norm = {norm(c): c for c in cols}
    for alias in aliases:
        col = cols_norm.get(norm(alias))
        if col:
            return col
    return None


def _tokens_fortes(texto: str) -> frozenset[str]:
    STOP = frozenset({
        "DE", "DA", "DO", "DAS", "DOS", "COM", "PARA", "POR", "EM",
        "NA", "NO", "NAS", "NOS", "UN", "UND", "UNID", "PCT", "CX",
    })
    out: set[str] = set()
    for tok in re.split(r"\s+", texto or ""):
        if len(tok) < 3:
            continue
        if not re.search(r"[A-Z]", tok):
            continue
        if tok in STOP:
            continue
        out.add(tok)
    return frozenset(out)


def _jaccard(a: frozenset[str], b: frozenset[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    uniao = len(a | b)
    return inter / uniao if uniao else 0.0


def _construir_linhas(
    df: pl.DataFrame,
    col_desc: str,
    col_ncm: str | None,
    col_cest: str | None,
    col_gtin: str | None,
    col_unidade: str | None,
) -> list[_Linha]:
    linhas: list[_Linha] = []
    descricoes = df.get_column(col_desc).to_list()
    ncms = df.get_column(col_ncm).to_list() if col_ncm else [""] * df.height
    cests = df.get_column(col_cest).to_list() if col_cest else [""] * df.height
    gtins = df.get_column(col_gtin).to_list() if col_gtin else [""] * df.height
    unidades = df.get_column(col_unidade).to_list() if col_unidade else [""] * df.height

    for idx in range(df.height):
        desc_raw = descricoes[idx]
        if isinstance(desc_raw, (list, tuple)):
            desc_raw = " | ".join(str(x) for x in desc_raw if x)
        desc_norm = normalize_desc(str(desc_raw or ""))
        desc_para_tokens = normalizar_unidades_em_texto(desc_norm)
        ncm = _normalizar_ncm(ncms[idx])
        linhas.append(
            _Linha(
                idx=idx,
                desc_norm=desc_norm,
                tokens=_tokens_fortes(desc_para_tokens),
                ncm=ncm,
                ncm4=_ncm_quatro_digitos(ncm),
                cest=_normalizar_codigo(cests[idx]),
                gtin=_normalizar_codigo(gtins[idx]),
                unidade=_normalizar_codigo(unidades[idx]),
            )
        )
    return linhas


def _agrupar_por_chave(
    linhas: Iterable[_Linha],
    chave_fn: callable,
    pendentes: set[int],
) -> list[list[_Linha]]:
    grupos: dict[str, list[_Linha]] = defaultdict(list)
    for linha in linhas:
        if linha.idx not in pendentes:
            continue
        chave = chave_fn(linha)
        if not chave:
            continue
        grupos[chave].append(linha)
    return [grupo for grupo in grupos.values() if len(grupo) >= 2]


class _UnionFind:
    def __init__(self, vals: Iterable[int]) -> None:
        self.parent = {v: v for v in vals}

    def find(self, v: int) -> int:
        while self.parent[v] != v:
            self.parent[v] = self.parent[self.parent[v]]
            v = self.parent[v]
        return v

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb


def _subdividir_por_descricao(
    grupo: list[_Linha],
    threshold: float,
    max_bucket_size: int,
) -> list[list[_Linha]]:
    if len(grupo) > max_bucket_size:
        _LOG.warning(
            "particionamento_bucket_grande",
            tamanho=len(grupo),
            max_bucket_size=max_bucket_size,
        )
        grupo = sorted(grupo, key=lambda l: -len(l.tokens))[:max_bucket_size]

    if len(grupo) <= 1:
        return [grupo]

    uf = _UnionFind(l.idx for l in grupo)
    n = len(grupo)
    for i in range(n):
        for j in range(i + 1, n):
            sim = _jaccard(grupo[i].tokens, grupo[j].tokens)
            if sim >= threshold:
                uf.union(grupo[i].idx, grupo[j].idx)

    componentes: dict[int, list[_Linha]] = defaultdict(list)
    for linha in grupo:
        componentes[uf.find(linha.idx)].append(linha)
    return [c for c in componentes.values() if len(c) >= 1]



class _ContextoParticionamento:
    def __init__(self, pendentes: set[int]):
        self.bloco_por_idx: dict[int, int] = {}
        self.motivo_por_idx: dict[int, str] = {}
        self.camada_por_idx: dict[int, int] = {}
        self.score_por_idx: dict[int, int] = {}
        self.chave_fiscal_por_idx: dict[int, str] = {}
        self.proximo_bloco = 1
        self.pendentes: set[int] = pendentes

    def atribuir(
        self,
        comp: list[_Linha],
        motivo: str,
        camada: int,
        score_base: int,
        chave: str,
    ) -> None:
        for l in comp:
            self.bloco_por_idx[l.idx] = self.proximo_bloco
            self.motivo_por_idx[l.idx] = motivo
            self.camada_por_idx[l.idx] = camada
            self.score_por_idx[l.idx] = score_base
            self.chave_fiscal_por_idx[l.idx] = chave
            self.pendentes.discard(l.idx)
        self.proximo_bloco += 1


def _processar_camada_subdividida(
    linhas: list[_Linha],
    ctx: _ContextoParticionamento,
    chave_fn: callable,
    chave_str_fn: callable,
    motivo: str,
    camada: int,
    score: int,
    threshold: float,
    max_bucket_size: int,
) -> None:
    grupos = _agrupar_por_chave(linhas, chave_fn=chave_fn, pendentes=ctx.pendentes)
    for grupo in grupos:
        for comp in _subdividir_por_descricao(grupo, threshold, max_bucket_size):
            ctx.atribuir(comp, motivo, camada, score, chave_str_fn(grupo[0]))

def ordenar_blocos_por_particionamento_fiscal(
    df: pl.DataFrame,
    *,
    incluir_camada_so_descricao: bool = False,
    thresholds: dict | None = None,
) -> pl.DataFrame:
    """Ordena o DataFrame em blocos de similaridade usando
    particionamento por chaves fiscais.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame com colunas de descricao, ncm, cest, gtin e unidade
        (resolvidas por aliases).
    incluir_camada_so_descricao : bool
        Se True, ativa a camada 5 (inverted index sobre tokens).
        Default False.
    thresholds : dict | None
        Override dos thresholds. None usa THRESHOLDS_DEFAULT.

    Returns
    -------
    pl.DataFrame
        DataFrame original reordenado, com colunas adicionais:
        - sim_bloco (int): id do bloco visual
        - sim_motivo (str): GTIN_IGUAL | NCM+CEST+UNID | NCM+UNID
                           | NCM4+UNID | DESC_TOKENS | ISOLADO
        - sim_camada (int): 0 a 5
        - sim_score (int): 0-100, indicativo
        - sim_desc_norm (str): descricao normalizada
        - sim_chave_fiscal (str): chave que originou o bloco
    """
    if df.is_empty():
        return df

    cfg = {**THRESHOLDS_DEFAULT, **(thresholds or {})}

    col_desc = _resolver_coluna(df, COLUNAS_DESCRICAO)
    if col_desc is None:
        raise ValueError(
            "Nenhuma coluna de descricao encontrada para o particionamento."
        )
    col_ncm = _resolver_coluna(df, ALIASES_NCM)
    col_cest = _resolver_coluna(df, ALIASES_CEST)
    col_gtin = _resolver_coluna(df, ALIASES_GTIN)
    col_unidade = _resolver_coluna(df, ALIASES_UNIDADE)

    linhas = _construir_linhas(
        df, col_desc, col_ncm, col_cest, col_gtin, col_unidade
    )

    ctx = _ContextoParticionamento({l.idx for l in linhas})

    # --- Camada 0: GTIN igual ---
    grupos = _agrupar_por_chave(
        linhas,
        chave_fn=lambda l: l.gtin if l.gtin else None,
        pendentes=ctx.pendentes,
    )
    for grupo in grupos:
        ctx.atribuir(grupo, "GTIN_IGUAL", 0, 100, f"GTIN={grupo[0].gtin}")

    # --- Camada 1: NCM + CEST + UNIDADE ---
    _processar_camada_subdividida(
        linhas, ctx,
        chave_fn=lambda l: f"{l.ncm}|{l.cest}|{l.unidade}" if (l.ncm and l.cest and l.unidade) else None,
        chave_str_fn=lambda l: f"NCM={l.ncm}|CEST={l.cest}|UN={l.unidade}",
        motivo="NCM+CEST+UNID", camada=1, score=85,
        threshold=cfg["camada_1"] / 100, max_bucket_size=cfg["max_bucket_size"],
    )

    # --- Camada 2: NCM + UNIDADE (sem CEST) ---
    _processar_camada_subdividida(
        linhas, ctx,
        chave_fn=lambda l: f"{l.ncm}|{l.unidade}" if (l.ncm and l.unidade) else None,
        chave_str_fn=lambda l: f"NCM={l.ncm}|UN={l.unidade}",
        motivo="NCM+UNID", camada=2, score=75,
        threshold=cfg["camada_2"] / 100, max_bucket_size=cfg["max_bucket_size"],
    )

    # --- Camada 3: NCM4 + UNIDADE ---
    _processar_camada_subdividida(
        linhas, ctx,
        chave_fn=lambda l: f"{l.ncm4}|{l.unidade}" if (l.ncm4 and l.unidade) else None,
        chave_str_fn=lambda l: f"NCM4={l.ncm4}|UN={l.unidade}",
        motivo="NCM4+UNID", camada=3, score=65,
        threshold=cfg["camada_3"] / 100, max_bucket_size=cfg["max_bucket_size"],
    )

    # --- Camada 5 (opcional): inverted index sobre descricao ---
    if incluir_camada_so_descricao:
        from interface_grafica.services.inverted_index_descricao import (
            agrupar_por_inverted_index,
        )
        pendentes_lista = [l for l in linhas if l.idx in ctx.pendentes]
        for comp in agrupar_por_inverted_index(
            pendentes_lista, threshold=cfg.get("camada_5", 70) / 100
        ):
            if len(comp) >= 2:
                ctx.atribuir(
                    comp, "DESC_TOKENS", 5, 60,
                    "INVERTED_INDEX",
                )

    # --- Camada 4: residual (singletons) ---
    for idx in sorted(ctx.pendentes):
        ctx.bloco_por_idx[idx] = ctx.proximo_bloco
        ctx.motivo_por_idx[idx] = "ISOLADO"
        ctx.camada_por_idx[idx] = 4
        ctx.score_por_idx[idx] = 0
        ctx.chave_fiscal_por_idx[idx] = ""
        ctx.proximo_bloco += 1

    # --- Materializa colunas e ordena ---
    n = df.height
    df_out = df.with_columns([
        pl.Series("sim_bloco", [ctx.bloco_por_idx[i] for i in range(n)]),
        pl.Series("sim_motivo", [ctx.motivo_por_idx[i] for i in range(n)]),
        pl.Series("sim_camada", [ctx.camada_por_idx[i] for i in range(n)]),
        pl.Series("sim_score", [ctx.score_por_idx[i] for i in range(n)]),
        pl.Series("sim_desc_norm", [linhas[i].desc_norm for i in range(n)]),
        pl.Series("sim_chave_fiscal", [ctx.chave_fiscal_por_idx[i] for i in range(n)]),
    ])

    distribuicao_camada: dict[int, int] = defaultdict(int)
    for c in ctx.camada_por_idx.values():
        distribuicao_camada[c] += 1
    _LOG.info(
        "particionamento_fiscal_executado",
        n_linhas=n,
        n_blocos=ctx.proximo_bloco - 1,
        distribuicao_camada=dict(distribuicao_camada),
        incluir_camada_so_descricao=incluir_camada_so_descricao,
    )

    return df_out.sort(["sim_camada", "sim_bloco", "sim_desc_norm"])
