from __future__ import annotations

import re
from dataclasses import dataclass
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


@dataclass(frozen=True)
class _RowSimilarityData:
    row_pos: int
    desc_norm: str
    ncm_norm: str
    cest_norm: str
    gtin_norm: str


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


def _expr_texto_para_normalizacao(coluna: str) -> pl.Expr:
    expr = pl.col(coluna)
    dtype = None
    try:
        dtype = expr.meta.root_names()
    except Exception:
        dtype = None
    # A decisao real sobre List precisa usar o schema do DataFrame; este helper
    # recebe apenas o nome e usa cast permissivo. Para listas, o servico cria
    # uma coluna auxiliar antes de chamar esta funcao.
    return expr.cast(pl.Utf8, strict=False)


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


def _tokens_chave(texto: str) -> str:
    tokens = [token for token in re.split(r"\s+", texto or "") if token]
    palavras = [token for token in tokens if re.search(r"[A-Z]", token)]
    numeros = re.findall(r"\d+", texto or "")
    partes = palavras[:2] + numeros[:3]
    if not partes:
        partes = tokens[:3]
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


def _codigo_score(a: str, b: str) -> int | None:
    if not a or not b:
        return None
    if a == b:
        return 100

    sa = set(a.split("|"))
    sb = set(b.split("|"))
    sa.discard("")
    sb.discard("")
    if not sa or not sb:
        return None
    if sa & sb:
        return 100
    return 0


def _score_composto(a: _RowSimilarityData, b: _RowSimilarityData) -> tuple[int, int, int | None, int | None, int | None]:
    score_desc = _dice_score(a.desc_norm, b.desc_norm)
    score_ncm = _codigo_score(a.ncm_norm, b.ncm_norm)
    score_cest = _codigo_score(a.cest_norm, b.cest_norm)
    score_gtin = _codigo_score(a.gtin_norm, b.gtin_norm)

    soma_pesos = 70
    soma = score_desc * 70

    for score, peso in [(score_ncm, 10), (score_cest, 8), (score_gtin, 12)]:
        if score is None:
            continue
        soma += score * peso
        soma_pesos += peso

    return round(soma / soma_pesos), score_desc, score_ncm, score_cest, score_gtin


def _nivel(score: int) -> str:
    if score >= 100:
        return "EXATO"
    if score >= 90:
        return "MUITO PARECIDO"
    if score >= 82:
        return "PARECIDO"
    return "FRACO"


def _criar_dados_linhas(df: pl.DataFrame) -> dict[int, _RowSimilarityData]:
    rows = df.select(
        [
            "__sim_row_pos",
            "sim_desc_norm",
            "sim_ncm_norm",
            "sim_cest_norm",
            "sim_gtin_norm",
        ]
    ).iter_rows(named=True)
    return {
        int(row["__sim_row_pos"]): _RowSimilarityData(
            row_pos=int(row["__sim_row_pos"]),
            desc_norm=str(row.get("sim_desc_norm") or ""),
            ncm_norm=str(row.get("sim_ncm_norm") or ""),
            cest_norm=str(row.get("sim_cest_norm") or ""),
            gtin_norm=str(row.get("sim_gtin_norm") or ""),
        )
        for row in rows
    }


def _melhores_vizinhos(
    df_ordenado: pl.DataFrame,
    dados: dict[int, _RowSimilarityData],
    janela: int,
) -> dict[int, dict[str, Any]]:
    posicoes = df_ordenado.get_column("__sim_row_pos").to_list()
    melhores: dict[int, dict[str, Any]] = {}

    for idx, pos in enumerate(posicoes):
        atual = dados[int(pos)]
        melhor: dict[str, Any] = {
            "sim_score": 0,
            "sim_score_desc": 0,
            "sim_score_ncm": None,
            "sim_score_cest": None,
            "sim_score_gtin": None,
            "sim_desc_referencia": "",
            "sim_ref_row_pos": None,
        }
        ini = max(0, idx - janela)
        fim = min(len(posicoes), idx + janela + 1)
        for j in range(ini, fim):
            if j == idx:
                continue
            outro = dados[int(posicoes[j])]
            score, score_desc, score_ncm, score_cest, score_gtin = _score_composto(atual, outro)
            if score > int(melhor["sim_score"]):
                melhor = {
                    "sim_score": score,
                    "sim_score_desc": score_desc,
                    "sim_score_ncm": score_ncm,
                    "sim_score_cest": score_cest,
                    "sim_score_gtin": score_gtin,
                    "sim_desc_referencia": outro.desc_norm,
                    "sim_ref_row_pos": outro.row_pos,
                }
        melhores[int(pos)] = melhor
    return melhores


def _atribuir_blocos(df_ordenado: pl.DataFrame, melhores: dict[int, dict[str, Any]], limite_bloco: int) -> dict[int, int]:
    bloco_por_pos: dict[int, int] = {}
    bloco_atual = 0
    posicoes = [int(pos) for pos in df_ordenado.get_column("__sim_row_pos").to_list()]

    for idx, pos in enumerate(posicoes):
        score = int(melhores.get(pos, {}).get("sim_score") or 0)
        ref = melhores.get(pos, {}).get("sim_ref_row_pos")
        if idx == 0:
            bloco_atual = 1
        elif score < limite_bloco or ref not in {posicoes[idx - 1], posicoes[idx - 2] if idx >= 2 else None}:
            bloco_atual += 1
        bloco_por_pos[pos] = bloco_atual
    return bloco_por_pos


def _normalizar_lista_coluna(df: pl.DataFrame, coluna: str, destino: str) -> pl.DataFrame:
    valores = [_normalizar_codigo(valor) for valor in df.get_column(coluna).to_list()]
    return df.with_columns(pl.Series(destino, valores))


def _normalizar_descricao_coluna(df: pl.DataFrame, coluna: str) -> pl.DataFrame:
    dtype = df.schema.get(coluna)
    if dtype is not None and dtype.is_nested():
        valores = [_normalizar_descricao_valor(valor) for valor in df.get_column(coluna).to_list()]
        return df.with_columns(pl.Series("sim_desc_norm", valores))
    return df.with_columns(expr_normalizar_descricao(coluna).alias("sim_desc_norm"))


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

    sort_cols = []
    if usar_ncm_cest:
        if col_ncm:
            sort_cols.append("sim_ncm_norm")
        if col_cest:
            sort_cols.append("sim_cest_norm")
    if col_gtin:
        sort_cols.append("sim_gtin_norm")
    sort_cols.extend(["sim_chave_ordem", "sim_desc_norm"])

    ordenado_base = work.sort(sort_cols, nulls_last=True) if sort_cols else work
    dados = _criar_dados_linhas(ordenado_base)
    melhores = _melhores_vizinhos(ordenado_base, dados, janela=janela)
    blocos = _atribuir_blocos(ordenado_base, melhores, limite_bloco=limite_bloco)

    posicoes_originais = work.get_column("__sim_row_pos").to_list()
    work = work.with_columns(
        [
            pl.Series("sim_bloco", [blocos.get(int(pos), 0) for pos in posicoes_originais]),
            pl.Series("sim_score", [melhores[int(pos)]["sim_score"] for pos in posicoes_originais]),
            pl.Series("sim_score_desc", [melhores[int(pos)]["sim_score_desc"] for pos in posicoes_originais]),
            pl.Series("sim_score_ncm", [melhores[int(pos)]["sim_score_ncm"] for pos in posicoes_originais]),
            pl.Series("sim_score_cest", [melhores[int(pos)]["sim_score_cest"] for pos in posicoes_originais]),
            pl.Series("sim_score_gtin", [melhores[int(pos)]["sim_score_gtin"] for pos in posicoes_originais]),
            pl.Series("sim_nivel", [_nivel(int(melhores[int(pos)]["sim_score"])) for pos in posicoes_originais]),
            pl.Series("sim_desc_referencia", [melhores[int(pos)]["sim_desc_referencia"] for pos in posicoes_originais]),
        ]
    )

    final_sort = ["sim_bloco", "sim_chave_ordem", "sim_score", "sim_desc_norm"]
    return work.sort(
        final_sort,
        descending=[False, False, True, False],
        nulls_last=True,
    ).drop("__sim_row_pos", strict=False)
