from __future__ import annotations

import re
from typing import Any

import polars as pl

from utilitarios.text import normalize_desc

COLUNAS_DESCRICAO = [
    "descr_padrao",
    "descricao_normalizada",
    "descricao",
    "descricao_final",
    "lista_descricoes",
    "lista_itens_agrupados",
]

ALIASES_NCM = ["ncm_padrao", "NCM_padrao", "lista_ncm", "ncm_final", "ncm"]
ALIASES_CEST = ["cest_padrao", "CEST_padrao", "lista_cest", "cest_final", "cest"]

COLUNAS_SIMILARIDADE = [
    "sim_bloco",
    "sim_score",
    "sim_nivel",
    "sim_desc_norm",
    "sim_chave_ordem",
    "sim_desc_referencia",
]


def _normalizar_nome_coluna(nome: str) -> str:
    return normalize_desc(nome).lower()


def _resolver_coluna(df: pl.DataFrame, aliases: list[str]) -> str | None:
    colunas = list(df.columns)
    for alias in aliases:
        if alias in colunas:
            return alias

    por_nome_normalizado = {_normalizar_nome_coluna(col): col for col in colunas}
    for alias in aliases:
        coluna = por_nome_normalizado.get(_normalizar_nome_coluna(alias))
        if coluna:
            return coluna
    return None


def _resolver_coluna_descricao(df: pl.DataFrame) -> str | None:
    return _resolver_coluna(df, COLUNAS_DESCRICAO)


def _stringify_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, pl.Series):
        value = value.to_list()
    if isinstance(value, (list, tuple, set)):
        return " | ".join(_stringify_value(item) for item in value if item is not None)
    return str(value)


def _tokens_relevantes(texto: str) -> list[str]:
    return [token for token in re.findall(r"[A-Z0-9]+", texto or "") if token]


def _criar_chave_ordem(desc_norm: str) -> str:
    tokens = _tokens_relevantes(desc_norm)
    palavras = [token for token in tokens if not token.isdigit()]
    numeros = [token for token in tokens if any(ch.isdigit() for ch in token)]
    partes = palavras[:2] + numeros[:3]
    if not partes and desc_norm:
        partes = [desc_norm[:12]]
    return "|".join(partes)


def _trigrams(texto: str) -> set[str]:
    texto = texto or ""
    if len(texto) <= 3:
        return {texto} if texto else set()
    return {texto[i : i + 3] for i in range(len(texto) - 2)}


def _dice_score(a: str, b: str) -> int:
    if a == b and a:
        return 100
    ga = _trigrams(a)
    gb = _trigrams(b)
    if not ga or not gb:
        return 0
    return round(200 * len(ga & gb) / (len(ga) + len(gb)))


def _nivel(score: int) -> str:
    if score >= 100:
        return "EXATO"
    if score >= 90:
        return "MUITO PARECIDO"
    if score >= 82:
        return "PARECIDO"
    return "FRACO"


def _valor_texto(row: dict[str, Any], coluna: str | None) -> str:
    if not coluna:
        return ""
    return _stringify_value(row.get(coluna)).strip()


def _preparar_linhas(
    df: pl.DataFrame,
    col_desc: str,
    col_ncm: str | None,
    col_cest: str | None,
    usar_ncm_cest: bool,
) -> list[dict[str, Any]]:
    linhas: list[dict[str, Any]] = []
    for pos, row in enumerate(df.to_dicts()):
        desc_original = _stringify_value(row.get(col_desc))
        desc_norm = normalize_desc(desc_original)
        chave_ordem = _criar_chave_ordem(desc_norm)
        ncm = _valor_texto(row, col_ncm) if usar_ncm_cest else ""
        cest = _valor_texto(row, col_cest) if usar_ncm_cest else ""
        linhas.append(
            {
                "__row": row,
                "__pos_original": pos,
                "__desc_norm": desc_norm,
                "__chave_ordem": chave_ordem,
                "__ncm": ncm,
                "__cest": cest,
                "__best_score": 0,
                "__best_desc": "",
                "__sim_bloco": 0,
            }
        )
    return linhas


def _ordenar_precomparacao(linhas: list[dict[str, Any]], usar_ncm_cest: bool) -> list[dict[str, Any]]:
    return sorted(
        linhas,
        key=lambda item: (
            item["__ncm"] if usar_ncm_cest else "",
            item["__cest"] if usar_ncm_cest else "",
            item["__chave_ordem"],
            item["__desc_norm"],
            item["__pos_original"],
        ),
    )


def _mesmo_escopo(a: dict[str, Any], b: dict[str, Any], usar_ncm_cest: bool) -> bool:
    if not usar_ncm_cest:
        return True
    return a["__ncm"] == b["__ncm"] and a["__cest"] == b["__cest"]


def _calcular_scores_vizinhos(
    linhas: list[dict[str, Any]],
    janela: int,
    usar_ncm_cest: bool,
) -> None:
    total = len(linhas)
    janela = max(1, int(janela or 1))
    for idx, atual in enumerate(linhas):
        desc_atual = atual["__desc_norm"]
        melhor_score = 0
        melhor_desc = ""
        ini = max(0, idx - janela)
        fim = min(total, idx + janela + 1)
        for j in range(ini, fim):
            if j == idx:
                continue
            outro = linhas[j]
            if not _mesmo_escopo(atual, outro, usar_ncm_cest):
                continue
            score = _dice_score(desc_atual, outro["__desc_norm"])
            if score > melhor_score:
                melhor_score = score
                melhor_desc = outro["__desc_norm"]
        atual["__best_score"] = melhor_score
        atual["__best_desc"] = melhor_desc


def _atribuir_blocos(
    linhas: list[dict[str, Any]],
    limite_bloco: int,
    usar_ncm_cest: bool,
) -> None:
    bloco_atual = 0
    chave_bloco_anterior: tuple[str, str, str] | None = None
    desc_anterior = ""
    limite_bloco = int(limite_bloco or 0)

    for item in linhas:
        chave_bloco = (
            item["__ncm"] if usar_ncm_cest else "",
            item["__cest"] if usar_ncm_cest else "",
            item["__chave_ordem"],
        )
        score_anterior = _dice_score(desc_anterior, item["__desc_norm"]) if desc_anterior else 0
        inicia_novo = (
            chave_bloco != chave_bloco_anterior
            or not desc_anterior
            or score_anterior < limite_bloco
        )
        if inicia_novo:
            bloco_atual += 1
        item["__sim_bloco"] = bloco_atual if item["__best_score"] >= limite_bloco else 0
        chave_bloco_anterior = chave_bloco
        desc_anterior = item["__desc_norm"]


def ordenar_blocos_similaridade_descricao(
    df: pl.DataFrame,
    janela: int = 4,
    limite_bloco: int = 82,
    usar_ncm_cest: bool = True,
) -> pl.DataFrame:
    """Ordena visualmente descricoes similares sem executar agrupamento.

    A funcao adiciona colunas de apoio `sim_*` e retorna um DataFrame reordenado.
    Ela nao persiste arquivos, nao altera ids e nao executa agregacao automatica.
    """
    if df.is_empty():
        return df

    col_desc = _resolver_coluna_descricao(df)
    if not col_desc:
        raise ValueError("Nao foi encontrada coluna de descricao para calcular similaridade.")

    col_ncm = _resolver_coluna(df, ALIASES_NCM)
    col_cest = _resolver_coluna(df, ALIASES_CEST)
    usar_chaves_fiscais = bool(usar_ncm_cest and col_ncm and col_cest)

    base_df = df.drop(COLUNAS_SIMILARIDADE, strict=False)
    linhas = _preparar_linhas(base_df, col_desc, col_ncm, col_cest, usar_chaves_fiscais)
    linhas = _ordenar_precomparacao(linhas, usar_chaves_fiscais)
    _calcular_scores_vizinhos(linhas, janela=janela, usar_ncm_cest=usar_chaves_fiscais)
    _atribuir_blocos(linhas, limite_bloco=limite_bloco, usar_ncm_cest=usar_chaves_fiscais)

    linhas_saida: list[dict[str, Any]] = []
    for item in linhas:
        row = dict(item["__row"])
        score = int(item["__best_score"])
        row.update(
            {
                "sim_bloco": int(item["__sim_bloco"]),
                "sim_score": score,
                "sim_nivel": _nivel(score),
                "sim_desc_norm": item["__desc_norm"],
                "sim_chave_ordem": item["__chave_ordem"],
                "sim_desc_referencia": item["__best_desc"],
            }
        )
        linhas_saida.append(row)

    out = pl.DataFrame(linhas_saida, infer_schema_length=None)
    sort_cols = []
    descending = []
    if usar_chaves_fiscais:
        sort_cols.extend([col for col in [col_ncm, col_cest] if col in out.columns])
        descending.extend([False] * len(sort_cols))
    sort_cols.extend(["sim_bloco", "sim_chave_ordem", "sim_score", "sim_desc_norm"])
    descending.extend([False, False, True, False])
    return out.sort(sort_cols, descending=descending, nulls_last=True)
