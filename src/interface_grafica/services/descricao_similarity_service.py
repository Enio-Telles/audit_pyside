from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from itertools import combinations
import re
from typing import Any

import polars as pl

from utilitarios.text import expr_normalizar_descricao, normalize_desc, remove_accents

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
ALIASES_GTIN = ["gtin_padrao", "GTIN_padrao", "lista_gtin", "gtin", "cod_barra", "cod_barras"]

STOPWORDS_CHAVE = {
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
    "UN",
    "UND",
    "UNID",
}


@dataclass(frozen=True)
class _RowSimilarityData:
    row_pos: int
    desc_norm: str
    ncm_norm: str
    cest_norm: str
    gtin_norm: str
    sim_chave_ordem: str
    tokens: tuple[str, ...]
    strong_tokens: tuple[str, ...]
    numeros: tuple[str, ...]
    ncm_partes: frozenset[str]
    ncm4_partes: frozenset[str]
    cest_partes: frozenset[str]
    gtin_partes: frozenset[str]


@dataclass(frozen=True)
class _ScoreDetalhe:
    score: int
    score_desc: int
    score_tokens: int
    score_numeros: int | None
    score_ncm: int | None
    score_cest: int | None
    score_gtin: int | None
    motivos: str


class _UnionFind:
    def __init__(self, valores: list[int]) -> None:
        self.parent = {valor: valor for valor in valores}
        self.rank = {valor: 0 for valor in valores}

    def find(self, valor: int) -> int:
        parent = self.parent[valor]
        if parent != valor:
            self.parent[valor] = self.find(parent)
        return self.parent[valor]

    def union(self, a: int, b: int) -> None:
        ra = self.find(a)
        rb = self.find(b)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            self.parent[ra] = rb
        elif self.rank[ra] > self.rank[rb]:
            self.parent[rb] = ra
        else:
            self.parent[rb] = ra
            self.rank[ra] += 1


def _normalizar_nome_coluna(nome: str) -> str:
    return (remove_accents(nome) or "").lower().strip()


def _resolver_coluna(df: pl.DataFrame, aliases: list[str]) -> str | None:
    if df.is_empty():
        return None
    cols = list(df.columns)
    for alias in aliases:
        if alias in cols:
            return alias

    normalizadas = {_normalizar_nome_coluna(col): col for col in cols}
    for alias in aliases:
        col = normalizadas.get(_normalizar_nome_coluna(alias))
        if col:
            return col
    return None


def _resolver_coluna_descricao(df: pl.DataFrame) -> str | None:
    return _resolver_coluna(df, COLUNAS_DESCRICAO)


def _normalizar_codigo(valor: Any) -> str:
    if valor is None:
        return ""
    if isinstance(valor, pl.Series):
        valor = valor.to_list()
    if isinstance(valor, (list, tuple, set)):
        partes = [_normalizar_codigo(item) for item in valor]
        return "|".join(sorted({p for p in partes if p}))
    texto = str(valor).strip().upper()
    if not texto:
        return ""
    texto = remove_accents(texto) or ""
    texto = re.sub(r"\s+", "", texto)
    return texto


def _normalizar_descricao_valor(valor: Any) -> str:
    if valor is None:
        return ""
    if isinstance(valor, pl.Series):
        valor = valor.to_list()
    if isinstance(valor, (list, tuple, set)):
        return normalize_desc(" | ".join(str(item) for item in valor if item is not None))
    return normalize_desc(str(valor))


def _tokens(texto: str) -> tuple[str, ...]:
    return tuple(token for token in re.split(r"\s+", texto or "") if token)


def _strong_tokens(tokens: tuple[str, ...]) -> tuple[str, ...]:
    vistos: set[str] = set()
    fortes: list[str] = []
    for token in tokens:
        if token in STOPWORDS_CHAVE:
            continue
        if len(token) < 2:
            continue
        if not re.search(r"[A-Z]", token):
            continue
        if token not in vistos:
            vistos.add(token)
            fortes.append(token)
    return tuple(fortes)


def _numeros(texto: str) -> tuple[str, ...]:
    vistos: set[str] = set()
    nums: list[str] = []
    for numero in re.findall(r"\d+", texto or ""):
        normalizado = numero.lstrip("0") or "0"
        if normalizado not in vistos:
            vistos.add(normalizado)
            nums.append(normalizado)
    return tuple(nums)


def _partes_codigo(codigo: str) -> frozenset[str]:
    partes = {parte for parte in codigo.split("|") if parte}
    return frozenset(partes)


def _ncm4(partes: frozenset[str]) -> frozenset[str]:
    prefixos = {re.sub(r"\D", "", parte)[:4] for parte in partes}
    return frozenset(prefixo for prefixo in prefixos if len(prefixo) == 4)


def _tokens_chave(texto: str) -> str:
    tokens = _tokens(texto)
    fortes = _strong_tokens(tokens)
    numeros = _numeros(texto)
    partes = list(fortes[:2]) + list(numeros[:3])
    if not partes:
        partes = list(tokens[:3])
    chave = "|".join(partes)
    return chave or texto[:20]


def _trigrams(texto: str) -> set[str]:
    texto = texto or ""
    if len(texto) <= 3:
        return {texto} if texto else set()
    return {texto[i : i + 3] for i in range(len(texto) - 2)}


def _dice_score(a: str, b: str) -> int:
    ga = _trigrams(a)
    gb = _trigrams(b)
    if not ga or not gb:
        return 0
    return round(200 * len(ga & gb) / (len(ga) + len(gb)))


def _jaccard_score(a: set[str] | frozenset[str], b: set[str] | frozenset[str]) -> int:
    if not a or not b:
        return 0
    return round(100 * len(a & b) / len(a | b))


def _codigo_score(a: frozenset[str], b: frozenset[str]) -> int | None:
    if not a or not b:
        return None
    return 100 if a & b else 0


def _ncm_score(a: _RowSimilarityData, b: _RowSimilarityData) -> int | None:
    if not a.ncm_partes or not b.ncm_partes:
        return None
    if a.ncm_partes & b.ncm_partes:
        return 100
    if a.ncm4_partes and b.ncm4_partes and a.ncm4_partes & b.ncm4_partes:
        return 70
    return 0


def _numero_score(a: _RowSimilarityData, b: _RowSimilarityData) -> int | None:
    if not a.numeros or not b.numeros:
        return None
    return 100 if set(a.numeros) & set(b.numeros) else 0


def _score_composto(a: _RowSimilarityData, b: _RowSimilarityData) -> _ScoreDetalhe:
    score_char = _dice_score(a.desc_norm, b.desc_norm)
    score_tokens = _jaccard_score(set(a.strong_tokens), set(b.strong_tokens))
    score_desc = round((score_char * 0.55) + (score_tokens * 0.45))
    score_numeros = _numero_score(a, b)
    score_ncm = _ncm_score(a, b)
    score_cest = _codigo_score(a.cest_partes, b.cest_partes)
    score_gtin = _codigo_score(a.gtin_partes, b.gtin_partes)

    componentes: list[tuple[int, int]] = [(score_desc, 60)]
    for score, peso in [
        (score_numeros, 15),
        (score_ncm, 10),
        (score_cest, 5),
        (score_gtin, 10),
    ]:
        if score is not None:
            componentes.append((score, peso))

    soma = sum(score * peso for score, peso in componentes)
    soma_pesos = sum(peso for _score, peso in componentes)
    score_final = round(soma / soma_pesos) if soma_pesos else 0

    # GTIN igual e descricao minimamente relacionada deve aparecer como candidato forte.
    if score_gtin == 100 and score_desc >= 50:
        score_final = max(score_final, 92)

    # CEST + NCM4 boost: when CEST equals and NCM equal/4-digit match and description is reasonable,
    # favor grouping even if description similarity is moderate.
    if score_cest == 100 and (score_ncm == 100 or score_ncm == 70) and score_desc >= 50:
        score_final = max(score_final, 86)

    motivos: list[str] = []
    if score_desc >= 90:
        motivos.append("DESC_ALTA")
    elif score_desc >= 75:
        motivos.append("DESC_MEDIA")
    if score_tokens >= 70:
        motivos.append("TOKENS")
    if score_numeros == 100:
        motivos.append("NUMEROS_IGUAIS")
    if score_ncm == 100:
        motivos.append("NCM_IGUAL")
    elif score_ncm == 70:
        motivos.append("NCM4_IGUAL")
    if score_cest == 100:
        motivos.append("CEST_IGUAL")
    if score_gtin == 100:
        motivos.append("GTIN_IGUAL")

    return _ScoreDetalhe(
        score=score_final,
        score_desc=score_desc,
        score_tokens=score_tokens,
        score_numeros=score_numeros,
        score_ncm=score_ncm,
        score_cest=score_cest,
        score_gtin=score_gtin,
        motivos="; ".join(motivos),
    )


def _nivel(score: int) -> str:
    if score >= 100:
        return "EXATO"
    if score >= 90:
        return "MUITO PARECIDO"
    if score >= 82:
        return "PARECIDO"
    return "FRACO"


def _normalizar_lista_coluna(df: pl.DataFrame, coluna: str, destino: str) -> pl.DataFrame:
    valores = [_normalizar_codigo(valor) for valor in df.get_column(coluna).to_list()]
    return df.with_columns(pl.Series(destino, valores))


def _normalizar_descricao_coluna(df: pl.DataFrame, coluna: str) -> pl.DataFrame:
    dtype = df.schema.get(coluna)
    if dtype is not None and dtype.is_nested():
        valores = [_normalizar_descricao_valor(valor) for valor in df.get_column(coluna).to_list()]
        return df.with_columns(pl.Series("sim_desc_norm", valores))
    return df.with_columns(expr_normalizar_descricao(coluna).alias("sim_desc_norm"))


def _criar_dados_linhas(df: pl.DataFrame) -> dict[int, _RowSimilarityData]:
    dados: dict[int, _RowSimilarityData] = {}
    for row in df.select(
        [
            "__sim_row_pos",
            "sim_desc_norm",
            "sim_ncm_norm",
            "sim_cest_norm",
            "sim_gtin_norm",
            "sim_chave_ordem",
        ]
    ).iter_rows(named=True):
        desc = str(row.get("sim_desc_norm") or "")
        tokens = _tokens(desc)
        fortes = _strong_tokens(tokens)
        ncm_partes = _partes_codigo(str(row.get("sim_ncm_norm") or ""))
        dados[int(row["__sim_row_pos"])] = _RowSimilarityData(
            row_pos=int(row["__sim_row_pos"]),
            desc_norm=desc,
            ncm_norm=str(row.get("sim_ncm_norm") or ""),
            cest_norm=str(row.get("sim_cest_norm") or ""),
            gtin_norm=str(row.get("sim_gtin_norm") or ""),
            sim_chave_ordem=str(row.get("sim_chave_ordem") or ""),
            tokens=tokens,
            strong_tokens=fortes,
            numeros=_numeros(desc),
            ncm_partes=ncm_partes,
            ncm4_partes=_ncm4(ncm_partes),
            cest_partes=_partes_codigo(str(row.get("sim_cest_norm") or "")),
            gtin_partes=_partes_codigo(str(row.get("sim_gtin_norm") or "")),
        )
    return dados


def _candidate_keys(row: _RowSimilarityData, usar_ncm_cest: bool) -> set[str]:
    keys: set[str] = set()
    fortes = row.strong_tokens[:6]
    numeros = row.numeros[:4]

    for gtin in row.gtin_partes:
        keys.add(f"GTIN:{gtin}")

    if usar_ncm_cest:
        for ncm in row.ncm_partes:
            for token in fortes[:4]:
                keys.add(f"NCM:{ncm}|T:{token}")
        for ncm4 in row.ncm4_partes:
            for token in fortes[:5]:
                keys.add(f"NCM4:{ncm4}|T:{token}")
        for cest in row.cest_partes:
            for token in fortes[:4]:
                keys.add(f"CEST:{cest}|T:{token}")

    for token_a, token_b in combinations(sorted(fortes[:5]), 2):
        keys.add(f"TOK2:{token_a}|{token_b}")

    for token in fortes[:5]:
        for numero in numeros:
            keys.add(f"NUM:{numero}|T:{token}")

    # Fallback leve para descricoes muito curtas.
    if not keys and fortes:
        keys.add(f"TOK:{fortes[0]}")
    return keys


def _candidate_pairs(
    dados: dict[int, _RowSimilarityData],
    janela_fallback: int,
    usar_ncm_cest: bool,
    max_group_size: int = 250,
) -> set[tuple[int, int]]:
    key_to_rows: dict[str, list[int]] = defaultdict(list)
    for row in dados.values():
        for key in _candidate_keys(row, usar_ncm_cest=usar_ncm_cest):
            key_to_rows[key].append(row.row_pos)

    max_pairs = min(750_000, max(20_000, len(dados) * 60))
    pairs: set[tuple[int, int]] = set()
    for rows in key_to_rows.values():
        if len(rows) < 2 or len(rows) > max_group_size:
            continue
        rows = sorted(set(rows))
        for a, b in combinations(rows, 2):
            pairs.add((a, b) if a < b else (b, a))
            if len(pairs) >= max_pairs:
                break
        if len(pairs) >= max_pairs:
            break

    # Fallback: ainda compara vizinhos de uma ordenacao textual simples para
    # nao perder casos com poucas chaves compartilhadas.
    ordenados = sorted(dados.values(), key=lambda row: (row.sim_chave_ordem, row.desc_norm))
    posicoes = [row.row_pos for row in ordenados]
    janela = max(1, int(janela_fallback or 1))
    for idx, pos in enumerate(posicoes):
        for j in range(idx + 1, min(len(posicoes), idx + janela + 1)):
            outro = posicoes[j]
            pairs.add((pos, outro) if pos < outro else (outro, pos))
    return pairs


def _calcular_pares(
    dados: dict[int, _RowSimilarityData],
    pairs: set[tuple[int, int]],
) -> tuple[dict[tuple[int, int], _ScoreDetalhe], dict[int, tuple[int | None, _ScoreDetalhe | None]]]:
    pair_scores: dict[tuple[int, int], _ScoreDetalhe] = {}
    melhores: dict[int, tuple[int | None, _ScoreDetalhe | None]] = {
        pos: (None, None) for pos in dados
    }

    for a, b in pairs:
        detalhe = _score_composto(dados[a], dados[b])
        pair_scores[(a, b)] = detalhe
        for origem, destino in [(a, b), (b, a)]:
            _melhor_pos, melhor_detalhe = melhores[origem]
            if melhor_detalhe is None or detalhe.score > melhor_detalhe.score:
                melhores[origem] = (destino, detalhe)
    return pair_scores, melhores


def _formar_blocos(
    dados: dict[int, _RowSimilarityData],
    pair_scores: dict[tuple[int, int], _ScoreDetalhe],
    limite_bloco: int,
) -> dict[int, list[int]]:
    uf = _UnionFind(list(dados.keys()))
    for (a, b), detalhe in pair_scores.items():
        if detalhe.score >= limite_bloco:
            uf.union(a, b)

    blocos: dict[int, list[int]] = defaultdict(list)
    for pos in dados:
        blocos[uf.find(pos)].append(pos)
    return {root: sorted(posicoes) for root, posicoes in blocos.items()}


def _score_par(pair_scores: dict[tuple[int, int], _ScoreDetalhe], a: int, b: int) -> int:
    key = (a, b) if a < b else (b, a)
    detalhe = pair_scores.get(key)
    return detalhe.score if detalhe else -1


def _ordenar_bloco(
    posicoes: list[int],
    dados: dict[int, _RowSimilarityData],
    pair_scores: dict[tuple[int, int], _ScoreDetalhe],
) -> list[int]:
    if len(posicoes) <= 2:
        return sorted(posicoes, key=lambda pos: (dados[pos].sim_chave_ordem, dados[pos].desc_norm))

    melhor_por_linha = {
        pos: max((_score_par(pair_scores, pos, outro) for outro in posicoes if outro != pos), default=-1)
        for pos in posicoes
    }
    atual = max(posicoes, key=lambda pos: (melhor_por_linha[pos], -len(dados[pos].desc_norm)))
    ordenados = [atual]
    restantes = set(posicoes)
    restantes.remove(atual)

    while restantes:
        proximo = max(
            restantes,
            key=lambda pos: (
                _score_par(pair_scores, ordenados[-1], pos),
                melhor_por_linha[pos],
                -len(dados[pos].desc_norm),
            ),
        )
        ordenados.append(proximo)
        restantes.remove(proximo)
    return ordenados


def _ordenar_blocos(
    blocos: dict[int, list[int]],
    dados: dict[int, _RowSimilarityData],
    pair_scores: dict[tuple[int, int], _ScoreDetalhe],
) -> list[int]:
    blocos_ordenados: list[tuple[tuple[int, str, int], list[int]]] = []
    for posicoes in blocos.values():
        bloco_ordenado = _ordenar_bloco(posicoes, dados, pair_scores)
        max_score = max(
            (_score_par(pair_scores, a, b) for a, b in combinations(bloco_ordenado, 2)),
            default=0,
        )
        chave = min((dados[pos].sim_chave_ordem for pos in bloco_ordenado), default="")
        # Blocos com mais de uma linha primeiro dentro da mesma chave visual.
        prioridade_singleton = 1 if len(bloco_ordenado) == 1 else 0
        blocos_ordenados.append(((prioridade_singleton, chave, -max_score), bloco_ordenado))

    resultado: list[int] = []
    for _key, posicoes in sorted(blocos_ordenados, key=lambda item: item[0]):
        resultado.extend(posicoes)
    return resultado


def ordenar_blocos_similaridade_descricao(
    df: pl.DataFrame,
    janela: int = 4,
    limite_bloco: int = 82,
    usar_ncm_cest: bool = True,
) -> pl.DataFrame:
    """Ordena linhas em blocos visuais de similaridade.

    A funcao apenas reorganiza o DataFrame e adiciona indicadores. Ela nao
    agrega, nao salva arquivos e nao altera identificadores fiscais.
    """
    if df.is_empty():
        return df

    col_desc = _resolver_coluna_descricao(df)
    if col_desc is None:
        raise ValueError("Nenhuma coluna de descricao encontrada para calcular similaridade.")

    janela = max(1, int(janela or 1))
    limite_bloco = max(0, min(100, int(limite_bloco or 0)))

    col_ncm = _resolver_coluna(df, ALIASES_NCM)
    col_cest = _resolver_coluna(df, ALIASES_CEST)
    col_gtin = _resolver_coluna(df, ALIASES_GTIN)

    work = df.with_row_index("__sim_row_pos")
    work = _normalizar_descricao_coluna(work, col_desc)

    for col, destino in [
        (col_ncm, "sim_ncm_norm"),
        (col_cest, "sim_cest_norm"),
        (col_gtin, "sim_gtin_norm"),
    ]:
        if col:
            work = _normalizar_lista_coluna(work, col, destino)
        else:
            work = work.with_columns(pl.lit("").alias(destino))

    work = work.with_columns(
        pl.Series("sim_chave_ordem", [_tokens_chave(v) for v in work["sim_desc_norm"].to_list()])
    )

    dados = _criar_dados_linhas(work)
    pairs = _candidate_pairs(dados, janela_fallback=janela, usar_ncm_cest=usar_ncm_cest)
    pair_scores, melhores = _calcular_pares(dados, pairs)
    blocos = _formar_blocos(dados, pair_scores, limite_bloco=limite_bloco)
    ordem_final = _ordenar_blocos(blocos, dados, pair_scores)
    bloco_por_pos: dict[int, int] = {}
    bloco_atual = 0
    bloco_anterior: int | None = None
    root_por_pos = {pos: root for root, posicoes in blocos.items() for pos in posicoes}
    for pos in ordem_final:
        root = root_por_pos[pos]
        if root != bloco_anterior:
            bloco_atual += 1
            bloco_anterior = root
        bloco_por_pos[pos] = bloco_atual

    sort_idx_por_pos = {pos: idx for idx, pos in enumerate(ordem_final)}
    posicoes_originais = [int(pos) for pos in work.get_column("__sim_row_pos").to_list()]

    def _detalhe(pos: int) -> _ScoreDetalhe | None:
        return melhores.get(pos, (None, None))[1]

    def _ref_desc(pos: int) -> str:
        ref_pos = melhores.get(pos, (None, None))[0]
        if ref_pos is None:
            return ""
        return dados[ref_pos].desc_norm

    work = work.with_columns(
        [
            pl.Series("__sim_sort_idx", [sort_idx_por_pos.get(pos, pos) for pos in posicoes_originais]),
            pl.Series("sim_bloco", [bloco_por_pos.get(pos, 0) for pos in posicoes_originais]),
            pl.Series("sim_score", [(_detalhe(pos).score if _detalhe(pos) else 0) for pos in posicoes_originais]),
            pl.Series("sim_score_desc", [(_detalhe(pos).score_desc if _detalhe(pos) else 0) for pos in posicoes_originais]),
            pl.Series("sim_score_tokens", [(_detalhe(pos).score_tokens if _detalhe(pos) else 0) for pos in posicoes_originais]),
            pl.Series("sim_score_numeros", [(_detalhe(pos).score_numeros if _detalhe(pos) else None) for pos in posicoes_originais]),
            pl.Series("sim_score_ncm", [(_detalhe(pos).score_ncm if _detalhe(pos) else None) for pos in posicoes_originais]),
            pl.Series("sim_score_cest", [(_detalhe(pos).score_cest if _detalhe(pos) else None) for pos in posicoes_originais]),
            pl.Series("sim_score_gtin", [(_detalhe(pos).score_gtin if _detalhe(pos) else None) for pos in posicoes_originais]),
            pl.Series("sim_nivel", [_nivel(_detalhe(pos).score if _detalhe(pos) else 0) for pos in posicoes_originais]),
            pl.Series("sim_motivos", [(_detalhe(pos).motivos if _detalhe(pos) else "") for pos in posicoes_originais]),
            pl.Series("sim_desc_referencia", [_ref_desc(pos) for pos in posicoes_originais]),
        ]
    )

    return work.sort("__sim_sort_idx").drop(["__sim_row_pos", "__sim_sort_idx"], strict=False)

