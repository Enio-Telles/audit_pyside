# Plano: metodologia de similaridade por particionamento fiscal

> Plano para o **Claude Code** implementar uma metodologia
> alternativa de similaridade no `audit_pyside`, em paralelo ao
> metodo atual.

---

## 1. Visao geral

### O problema com o metodo atual

O servico `descricao_similarity_service.py` calcula similaridade
em duas etapas:

1. Gera pares candidatos por chaves textuais (TOK2, NCM+token, etc),
2. Calcula um score composto onde NCM, CEST e GTIN entram como
   componentes ponderados (10%, 5%, 10% do total).

Em base fiscal grande (10k+ produtos), isso tem dois problemas:

- **Eficiencia**: chaves textuais geram buckets desbalanceados
  (TOK2 com palavra comum tem milhares de pares); o calculo
  textual O(k²) por bucket pode explodir.
- **Eficacia**: identificadores fiscais sao tratados como sinais
  ponderados, nao como evidencia primaria. GTIN igual ainda
  precisa "passar" no score textual para entrar num bloco.

### A metodologia nova

Inverter a logica: **particionar por identificadores fiscais
primeiro, comparar descricoes apenas dentro de cada particao**.

Cada par cai em exatamente uma camada da hierarquia abaixo, da
mais forte para a mais fraca:

| Camada | Chave de particao | Decisao | Custo textual |
|--------|---------------------|---------|----------------|
| 0 | `gtin` igual e nao vazio | Bloco automatico, score 100 | zero |
| 1 | `ncm + cest + unidade` iguais | Bloco com texto leve (>=50) | Jaccard tokens |
| 2 | `ncm + unidade` iguais | Bloco com texto medio (>=65) | Jaccard tokens |
| 3 | `ncm4 + unidade` iguais | Bloco com texto exigente (>=80) | Jaccard tokens |
| 4 | sem chave fiscal compativel | Sem agrupamento (singleton) | — |
| 5 | (opcional) inverted index sobre tokens fortes | Bloco com texto exigente (>=70) | Jaccard tokens |

A camada 5 e ativada por flag e processa apenas itens que ficaram
fora das camadas 0-4 (ou e usada num modo standalone que ignora
identificadores fiscais inteiramente).

**Os 5 thresholds e o cap de bucket sao a configuracao inteira do
metodo.** Comparar com os ~25 parametros do metodo atual.

### Convivencia com o metodo atual

A nova metodologia **nao substitui** a existente. Vira uma segunda
funcao publica:

```python
# Mantida como esta (legacy, com 4 commits de melhorias):
ordenar_blocos_similaridade_descricao(df, ...)

# Nova:
ordenar_blocos_por_particionamento_fiscal(df, ...)
```

A UI ganha um seletor de metodo na aba Agregacao.

---

## 2. Estado de partida

A branch `feat/similaridade-particionamento` parte de `main` + 4
patches (`0001-*` a `0004-*`) que entregam melhorias incrementais ao
metodo atual:

- `0001` — NCM hierarquico de 5 niveis (item/subposicao/posicao/capitulo)
- `0002` — Canonizacao de unidades antes de extrair numeros (ML/L, G/KG)
- `0003` — Caps de bucket e top-k por linha (eficiencia)
- `0004` — Cap de tamanho de bloco e coesao minima intra-bloco

Esses commits ficam **na mesma branch** que entrega o metodo novo,
porque o usuario ainda quer manter o metodo atual funcionando bem
para uso paralelo.

### Validacao do ponto de partida

```bash
git clone https://github.com/Enio-Telles/audit_pyside.git
cd audit_pyside
git config core.autocrlf false
cp ../CLAUDE.md .
git checkout -b feat/similaridade-particionamento
git am ../0001-*.patch ../0002-*.patch ../0003-*.patch ../0004-*.patch

# Verificar que os 4 patches aplicam limpos:
git log --oneline -5
# Esperado: 4 commits feat(similarity): ... acima do main

# Validar testes:
PYTHONPATH=src python3 -m pytest \
    tests/test_descricao_similarity_service.py \
    tests/test_unidades_descricao.py -q
# Esperado: 17 passed
```

---

## 3. Plano de execucao

5 fases, **1 commit por fase**. Apos cada commit, rodar a suite
de testes especifica e parar para confirmacao do usuario antes
da proxima fase.

### Checklist (use TodoWrite)

```
[ ] Fase 0: Setup e validacao do baseline
[ ] Fase 1: Modulo particionamento_fiscal.py (camadas 0-4)
[ ] Fase 2: Modulo inverted_index_descricao.py (camada 5)
[ ] Fase 3: Integracao na UI via patch
[ ] Fase 4: Documentacao
[ ] Fase 5: Validacao final + relatorio
```

---

## 4. Fase 1 — Modulo `particionamento_fiscal.py`

### 4.1 Arquivo a criar

`src/interface_grafica/services/particionamento_fiscal.py`

### 4.2 Estrutura

```python
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


# Aliases reaproveitados do metodo atual:
COLUNAS_DESCRICAO = [
    "descr_padrao", "descricao_normalizada", "descricao",
    "descricao_final", "lista_descricoes", "lista_itens_agrupados",
]
ALIASES_NCM = ["ncm_padrao", "NCM_padrao", "lista_ncm", "ncm_final", "ncm"]
ALIASES_CEST = ["cest_padrao", "CEST_padrao", "lista_cest", "cest_final", "cest"]
ALIASES_GTIN = ["gtin_padrao", "GTIN_padrao", "lista_gtin", "gtin", "cod_barra", "cod_barras"]
ALIASES_UNIDADE = ["unid_padrao", "unidade_padrao", "unid", "unidade", "un"]


# Thresholds por camada. Camadas com chave fiscal mais forte
# aceitam similaridade textual mais fraca:
THRESHOLDS_DEFAULT = {
    "camada_1": 50,       # NCM+CEST+UNID iguais -> texto pode ser fraco
    "camada_2": 65,       # NCM+UNID iguais (sem CEST) -> texto medio
    "camada_3": 80,       # NCM4+UNID iguais -> texto forte
    "max_bucket_size": 200,  # buckets maiores sao subdivididos por amostragem
    "min_jaccard_para_par": 0.30,  # piso global, descarta pares muito fracos
}
```

### 4.3 Funcoes helpers (cole estes blocos)

```python
@dataclass(frozen=True)
class _Linha:
    """Representacao compacta de uma linha para o particionamento."""
    idx: int
    desc_norm: str
    tokens: frozenset[str]
    ncm: str
    ncm4: str
    cest: str
    gtin: str
    unidade: str


def _normalizar_codigo(valor: object) -> str:
    """Normaliza codigos fiscais: remove espacos, acentos, deixa
    em maiusculas. Listas/tuplas sao concatenadas com '|'."""
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
    """NCM: somente digitos, ate 8 caracteres. '' se vazio/invalido."""
    bruto = _normalizar_codigo(valor)
    digitos = re.sub(r"\D", "", bruto)
    return digitos[:8]


def _ncm_quatro_digitos(ncm: str) -> str:
    return ncm[:4] if len(ncm) >= 4 else ""


def _resolver_coluna(df: pl.DataFrame, aliases: list[str]) -> str | None:
    """Encontra a primeira coluna que bate (case-insensitive, sem acento)."""
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
    """Tokens com >=3 chars contendo pelo menos uma letra.

    Stoplist minima para tokens que so atrapalham agrupamento."""
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
    """Jaccard simples sobre conjuntos de tokens."""
    if not a or not b:
        return 0.0
    inter = len(a & b)
    uniao = len(a | b)
    return inter / uniao if uniao else 0.0
```

### 4.4 Construcao das linhas

```python
def _construir_linhas(
    df: pl.DataFrame,
    col_desc: str,
    col_ncm: str | None,
    col_cest: str | None,
    col_gtin: str | None,
    col_unidade: str | None,
) -> list[_Linha]:
    """Materializa lista de _Linha a partir do DataFrame, com todas
    as normalizacoes aplicadas."""
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
        # Canonizacao de unidades antes da tokenizacao:
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
```

### 4.5 Particionamento e atribuicao de blocos

```python
def _agrupar_por_chave(
    linhas: Iterable[_Linha],
    chave_fn: callable,
    pendentes: set[int],
) -> list[list[_Linha]]:
    """Agrupa linhas pendentes pela chave produzida por chave_fn.
    Chave vazia (None ou '') faz a linha ser ignorada nesta camada."""
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
    """Dentro de um bucket fiscalmente compativel, faz Jaccard
    O(k^2) e une via union-find pares acima do threshold.

    Buckets maiores que max_bucket_size: ordena por len(tokens)
    decrescente e mantem so os top-N (heuristica simples para
    evitar explosao - ainda processa o grupo, so com cap)."""
    if len(grupo) > max_bucket_size:
        _LOG.warning(
            "particionamento_bucket_grande",
            tamanho=len(grupo),
            max_bucket_size=max_bucket_size,
        )
        # Ordena pelos com mais tokens (descricoes mais ricas) e
        # processa em blocos de max_bucket_size.
        grupo = sorted(grupo, key=lambda l: -len(l.tokens))[:max_bucket_size]

    if len(grupo) <= 1:
        return [grupo]

    uf = _UnionFind(l.idx for l in grupo)
    indice_por_idx = {l.idx: l for l in grupo}
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
```

### 4.6 Funcao publica principal

```python
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
        Itens sem chave fiscal sao agrupados pela descricao.
        Default False - exige decisao explicita do usuario.
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

    # Estado de atribuicao por linha:
    bloco_por_idx: dict[int, int] = {}
    motivo_por_idx: dict[int, str] = {}
    camada_por_idx: dict[int, int] = {}
    score_por_idx: dict[int, int] = {}
    chave_fiscal_por_idx: dict[int, str] = {}
    proximo_bloco = 1
    pendentes: set[int] = {l.idx for l in linhas}

    def _atribuir(
        comp: list[_Linha],
        bloco_id: int,
        motivo: str,
        camada: int,
        score_base: int,
        chave: str,
    ) -> None:
        for l in comp:
            bloco_por_idx[l.idx] = bloco_id
            motivo_por_idx[l.idx] = motivo
            camada_por_idx[l.idx] = camada
            score_por_idx[l.idx] = score_base
            chave_fiscal_por_idx[l.idx] = chave
            pendentes.discard(l.idx)

    # --- Camada 0: GTIN igual ---
    grupos = _agrupar_por_chave(
        linhas,
        chave_fn=lambda l: l.gtin if l.gtin else None,
        pendentes=pendentes,
    )
    for grupo in grupos:
        _atribuir(grupo, proximo_bloco, "GTIN_IGUAL", 0, 100, f"GTIN={grupo[0].gtin}")
        proximo_bloco += 1

    # --- Camada 1: NCM + CEST + UNIDADE ---
    grupos = _agrupar_por_chave(
        linhas,
        chave_fn=lambda l: f"{l.ncm}|{l.cest}|{l.unidade}"
                            if (l.ncm and l.cest and l.unidade) else None,
        pendentes=pendentes,
    )
    for grupo in grupos:
        for comp in _subdividir_por_descricao(grupo, cfg["camada_1"] / 100, cfg["max_bucket_size"]):
            _atribuir(
                comp, proximo_bloco, "NCM+CEST+UNID", 1, 85,
                f"NCM={grupo[0].ncm}|CEST={grupo[0].cest}|UN={grupo[0].unidade}",
            )
            proximo_bloco += 1

    # --- Camada 2: NCM + UNIDADE (sem CEST) ---
    grupos = _agrupar_por_chave(
        linhas,
        chave_fn=lambda l: f"{l.ncm}|{l.unidade}"
                            if (l.ncm and l.unidade) else None,
        pendentes=pendentes,
    )
    for grupo in grupos:
        for comp in _subdividir_por_descricao(grupo, cfg["camada_2"] / 100, cfg["max_bucket_size"]):
            _atribuir(
                comp, proximo_bloco, "NCM+UNID", 2, 75,
                f"NCM={grupo[0].ncm}|UN={grupo[0].unidade}",
            )
            proximo_bloco += 1

    # --- Camada 3: NCM4 + UNIDADE ---
    grupos = _agrupar_por_chave(
        linhas,
        chave_fn=lambda l: f"{l.ncm4}|{l.unidade}"
                            if (l.ncm4 and l.unidade) else None,
        pendentes=pendentes,
    )
    for grupo in grupos:
        for comp in _subdividir_por_descricao(grupo, cfg["camada_3"] / 100, cfg["max_bucket_size"]):
            _atribuir(
                comp, proximo_bloco, "NCM4+UNID", 3, 65,
                f"NCM4={grupo[0].ncm4}|UN={grupo[0].unidade}",
            )
            proximo_bloco += 1

    # --- Camada 5 (opcional): inverted index sobre descricao ---
    if incluir_camada_so_descricao:
        from interface_grafica.services.inverted_index_descricao import (
            agrupar_por_inverted_index,
        )
        pendentes_lista = [l for l in linhas if l.idx in pendentes]
        for comp in agrupar_por_inverted_index(
            pendentes_lista, threshold=cfg.get("camada_5", 70) / 100
        ):
            if len(comp) >= 2:
                _atribuir(
                    comp, proximo_bloco, "DESC_TOKENS", 5, 60,
                    "INVERTED_INDEX",
                )
                proximo_bloco += 1

    # --- Camada 4: residual (singletons) ---
    for idx in pendentes:
        bloco_por_idx[idx] = proximo_bloco
        motivo_por_idx[idx] = "ISOLADO"
        camada_por_idx[idx] = 4
        score_por_idx[idx] = 0
        chave_fiscal_por_idx[idx] = ""
        proximo_bloco += 1

    # --- Materializa colunas e ordena ---
    n = df.height
    df_out = df.with_columns([
        pl.Series("sim_bloco", [bloco_por_idx[i] for i in range(n)]),
        pl.Series("sim_motivo", [motivo_por_idx[i] for i in range(n)]),
        pl.Series("sim_camada", [camada_por_idx[i] for i in range(n)]),
        pl.Series("sim_score", [score_por_idx[i] for i in range(n)]),
        pl.Series("sim_desc_norm", [linhas[i].desc_norm for i in range(n)]),
        pl.Series("sim_chave_fiscal", [chave_fiscal_por_idx[i] for i in range(n)]),
    ])

    # Telemetria estruturada:
    distribuicao_camada: dict[int, int] = defaultdict(int)
    for c in camada_por_idx.values():
        distribuicao_camada[c] += 1
    _LOG.info(
        "particionamento_fiscal_executado",
        n_linhas=n,
        n_blocos=proximo_bloco - 1,
        distribuicao_camada=dict(distribuicao_camada),
        incluir_camada_so_descricao=incluir_camada_so_descricao,
    )

    # Ordena: por camada (0 primeiro), depois bloco, depois desc_norm:
    return df_out.sort(["sim_camada", "sim_bloco", "sim_desc_norm"])
```

### 4.7 Testes (criar `tests/test_particionamento_fiscal.py`)

```python
import polars as pl
import pytest

from interface_grafica.services.particionamento_fiscal import (
    ordenar_blocos_por_particionamento_fiscal,
)


def _df_basico() -> pl.DataFrame:
    return pl.DataFrame({
        "id_agrupado": ["1", "2", "3", "4", "5"],
        "descr_padrao": [
            "CERVEJA HEINEKEN LATA 350ML",
            "ARROZ TIPO 1 5KG",
            "CERVEJA HEINEKEN 350 ML LATA",
            "CERVEJA HEINEKEN LONG NECK 330ML",
            "BISCOITO RECHEADO MORANGO 100G",
        ],
        "ncm_padrao": ["22030000", "10063021", "22030000", "22030000", "19053100"],
        "cest_padrao": ["0302100", "", "0302100", "0302100", ""],
        "gtin_padrao": ["7891001", "", "7891001", "7891999", ""],
        "unid_padrao": ["UN", "KG", "UN", "UN", "PCT"],
    })


def test_camada_0_gtin_igual_forma_bloco_automatico():
    df = _df_basico()
    out = ordenar_blocos_por_particionamento_fiscal(df)
    # Itens 1 e 3 tem mesmo GTIN -> camada 0
    bloco_gtin = out.filter(
        pl.col("id_agrupado").is_in(["1", "3"])
    )["sim_bloco"]
    assert bloco_gtin.n_unique() == 1
    assert (
        out.filter(pl.col("id_agrupado").is_in(["1", "3"]))["sim_motivo"]
        .unique().to_list()[0]
    ) == "GTIN_IGUAL"


def test_camada_1_ncm_cest_unidade_iguais():
    df = _df_basico()
    out = ordenar_blocos_por_particionamento_fiscal(df)
    # Itens 1, 3, 4 tem NCM=22030000, CEST=0302100, UN=UN.
    # 1 e 3 ja foram para camada 0 (GTIN). Sobra item 4 sozinho na
    # camada 1 - como singleton, vai para residual ou cai na propria
    # camada se houver outros. Aqui vai para camada 4 (residual).
    item_4 = out.filter(pl.col("id_agrupado") == "4")
    # GTIN diferente -> nao caiu na camada 0.
    assert item_4["sim_motivo"].to_list()[0] != "GTIN_IGUAL"


def test_camada_4_residual_para_itens_isolados():
    df = pl.DataFrame({
        "id_agrupado": ["1"],
        "descr_padrao": ["PRODUTO UNICO"],
        "ncm_padrao": ["12345678"],
        "cest_padrao": [""],
        "gtin_padrao": [""],
        "unid_padrao": ["UN"],
    })
    out = ordenar_blocos_por_particionamento_fiscal(df)
    assert out["sim_motivo"].to_list() == ["ISOLADO"]
    assert out["sim_camada"].to_list() == [4]


def test_preserva_todas_as_linhas():
    df = _df_basico()
    out = ordenar_blocos_por_particionamento_fiscal(df)
    assert out.height == df.height
    assert set(out["id_agrupado"].to_list()) == set(df["id_agrupado"].to_list())


def test_colunas_indicadoras_adicionadas():
    df = _df_basico()
    out = ordenar_blocos_por_particionamento_fiscal(df)
    for col in ["sim_bloco", "sim_motivo", "sim_camada", "sim_score",
                "sim_desc_norm", "sim_chave_fiscal"]:
        assert col in out.columns


def test_camada_5_desativada_por_default():
    """Sem flag, itens sem chave fiscal viram singletons."""
    df = pl.DataFrame({
        "id_agrupado": ["1", "2"],
        "descr_padrao": ["CAFE EM PO 250G", "CAFE TORRADO MOIDO 250G"],
        "ncm_padrao": ["", ""],
        "cest_padrao": ["", ""],
        "gtin_padrao": ["", ""],
        "unid_padrao": ["", ""],
    })
    out = ordenar_blocos_por_particionamento_fiscal(df)
    assert out["sim_motivo"].unique().to_list() == ["ISOLADO"]


def test_camada_5_ativa_agrupa_por_descricao():
    """Com flag, itens similares por texto sao agrupados."""
    df = pl.DataFrame({
        "id_agrupado": ["1", "2", "3"],
        "descr_padrao": [
            "CAFE TORRADO MOIDO 250G",
            "CAFE TORRADO MOIDO PACOTE 250G",
            "REFRIGERANTE COCA-COLA 2L",
        ],
        "ncm_padrao": ["", "", ""],
        "cest_padrao": ["", "", ""],
        "gtin_padrao": ["", "", ""],
        "unid_padrao": ["", "", ""],
    })
    out = ordenar_blocos_por_particionamento_fiscal(
        df, incluir_camada_so_descricao=True,
    )
    bloco_cafe = out.filter(
        pl.col("id_agrupado").is_in(["1", "2"])
    )["sim_bloco"].n_unique()
    assert bloco_cafe == 1


def test_thresholds_customizaveis():
    df = _df_basico()
    # Threshold absurdamente alto na camada 1: forca subdivisao.
    out = ordenar_blocos_por_particionamento_fiscal(
        df, thresholds={"camada_1": 99},
    )
    assert out.height == df.height  # nao quebra
```

### 4.8 Validacao

```bash
PYTHONPATH=src python3 -m pytest tests/test_particionamento_fiscal.py -q
# Esperado: 8 passed
```

### 4.9 Commit

```
feat(similarity): metodologia de particionamento por chaves fiscais

Adiciona src/interface_grafica/services/particionamento_fiscal.py
com a funcao publica ordenar_blocos_por_particionamento_fiscal.

A metodologia inverte a logica do metodo atual:
1. Particiona o DataFrame por chaves fiscais (GTIN, NCM, CEST, UNID)
2. So calcula similaridade textual dentro de cada particao

Hierarquia de camadas, da mais forte para a mais fraca:
  0. GTIN igual: bloco automatico, score 100
  1. NCM + CEST + UNIDADE: bloco com texto leve (>=50)
  2. NCM + UNIDADE: bloco com texto medio (>=65)
  3. NCM4 + UNIDADE: bloco com texto exigente (>=80)
  4. Sem chave: singleton (residual)
  5. (opcional, off por default) inverted index sobre descricao

Cada item participa apenas da camada mais forte em que encontra
companhia. Ganhos:
- Eficiencia: comparacao textual O(k^2) so dentro de buckets pequenos
- Eficacia: identificadores fiscais sao evidencia primaria, nao
  apenas peso ponderado
- Configuracao minima: 4 thresholds (vs ~25 do metodo atual)

Roda em paralelo ao metodo atual sem substitui-lo. A camada 5 sera
implementada na proxima fase.
```

---

## 5. Fase 2 — Modulo `inverted_index_descricao.py`

### 5.1 Arquivo a criar

`src/interface_grafica/services/inverted_index_descricao.py`

### 5.2 Por que inverted index

A camada 5 da metodologia precisa agrupar itens **so por
descricao**, sem comparar todos contra todos. Inverted index
resolve com complexidade O(N x T) na construcao + O(soma de
tamanhos de bucket²) no Jaccard, onde T = tokens medios por
descricao (~5-10).

Em base fiscal de 50 mil itens, isso roda em segundos numa
maquina modesta.

### 5.3 Estrutura

```python
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
    "df_max_ratio": 0.05,           # tokens em mais de 5% do corpus = podados
    "df_max_absoluto": 500,         # ou em > 500 itens, podados
    "min_tokens_compartilhados": 2, # exige >=2 tokens em comum para virar par
    "limite_pares_por_token": 1000, # cap por seguranca em buckets explosivos
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

    # 1. Inverted index: token -> [idx, idx, ...]
    indice: dict[str, list[int]] = defaultdict(list)
    linha_por_idx = {l.idx: l for l in linhas}
    for l in linhas:
        for token in l.tokens:
            indice[token].append(l.idx)

    # 2. Poda de tokens com DF muito alto (genericos)
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

    # 3. Geracao de pares candidatos (com cap por seguranca)
    contagem_por_par: dict[tuple[int, int], int] = defaultdict(int)
    for token, idxs in bons.items():
        if len(idxs) < 2:
            continue
        if len(idxs) > cfg["limite_pares_por_token"]:
            # Bucket muito grande - amostra os primeiros (deterministico).
            idxs = sorted(idxs)[: cfg["limite_pares_por_token"]]
        idxs_sorted = sorted(set(idxs))
        # Gera todos os pares ordenados (a < b) deste bucket:
        for i in range(len(idxs_sorted)):
            for j in range(i + 1, len(idxs_sorted)):
                contagem_por_par[(idxs_sorted[i], idxs_sorted[j])] += 1

    # 4. Filtra pares que compartilham menos que min_tokens_compartilhados
    candidatos = [
        par for par, c in contagem_por_par.items()
        if c >= cfg["min_tokens_compartilhados"]
    ]

    # 5. Calcula Jaccard nos candidatos e une via union-find
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
        COLUNAS_DESCRICAO, ALIASES_NCM, ALIASES_CEST,
        ALIASES_GTIN, ALIASES_UNIDADE,
    )

    if df.is_empty():
        return df

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
```

### 5.4 Testes (`tests/test_inverted_index_descricao.py`)

```python
import polars as pl
import pytest

from interface_grafica.services.inverted_index_descricao import (
    agrupar_por_inverted_index,
    ordenar_blocos_apenas_por_descricao,
)
from interface_grafica.services.particionamento_fiscal import (
    _construir_linhas, _resolver_coluna, COLUNAS_DESCRICAO,
)


def _construir_linhas_de(descricoes: list[str]):
    df = pl.DataFrame({
        "descr_padrao": descricoes,
    })
    col = _resolver_coluna(df, COLUNAS_DESCRICAO)
    return _construir_linhas(df, col, None, None, None, None)


def test_agrupa_descricoes_com_tokens_em_comum():
    linhas = _construir_linhas_de([
        "CAFE TORRADO MOIDO 250G",
        "CAFE TORRADO MOIDO PACOTE 250G",
        "REFRIGERANTE COCA-COLA 2L",
    ])
    componentes = agrupar_por_inverted_index(linhas, threshold=0.4)
    # Os dois cafes devem ficar juntos; refri sozinho.
    n_componentes_de_2_ou_mais = sum(1 for c in componentes if len(c) >= 2)
    assert n_componentes_de_2_ou_mais == 1


def test_pares_com_apenas_um_token_em_comum_nao_agrupam():
    """Default min_tokens_compartilhados=2 evita falso positivo."""
    linhas = _construir_linhas_de([
        "CAFE EXPRESSO ITALIANO",
        "MELANCIA CAFE NAO COMBINA",
    ])
    componentes = agrupar_por_inverted_index(linhas, threshold=0.5)
    # So 'CAFE' em comum -> nao deve unir.
    assert all(len(c) == 1 for c in componentes)


def test_threshold_alto_separa_descricoes_diferentes():
    linhas = _construir_linhas_de([
        "CAFE TORRADO MOIDO 250G",
        "CAFE SOLUVEL EM PO 250G",
    ])
    # Compartilham CAFE e 250G (apos canonizacao). Threshold alto separa.
    componentes_alto = agrupar_por_inverted_index(linhas, threshold=0.95)
    n_de_2 = sum(1 for c in componentes_alto if len(c) >= 2)
    assert n_de_2 == 0


def test_funcao_publica_standalone():
    df = pl.DataFrame({
        "id_agrupado": ["1", "2", "3"],
        "descr_padrao": [
            "CAFE TORRADO MOIDO 250G",
            "CAFE TORRADO MOIDO PACOTE 250G",
            "REFRIGERANTE COCA-COLA 2L",
        ],
    })
    out = ordenar_blocos_apenas_por_descricao(df, threshold=0.4)
    assert out.height == 3
    assert "sim_bloco" in out.columns
    assert out["sim_motivo"].unique().to_list() == ["DESC_TOKENS"]


def test_dataframe_vazio():
    df = pl.DataFrame({"descr_padrao": []})
    out = ordenar_blocos_apenas_por_descricao(df)
    assert out.height == 0


def test_lista_de_uma_unica_linha():
    linhas = _construir_linhas_de(["PRODUTO UNICO TESTE"])
    componentes = agrupar_por_inverted_index(linhas)
    assert len(componentes) == 1
    assert len(componentes[0]) == 1


def test_corpus_grande_com_tokens_genericos_podados():
    """Tokens 'LATA' e 'GARRAFA' aparecendo em 50% do corpus
    nao devem dominar a geracao de pares."""
    descricoes = [f"CERVEJA MARCA{i} LATA 350ML" for i in range(100)]
    descricoes += ["VINHO ESPECIAL GARRAFA 750ML"] * 2
    linhas = _construir_linhas_de(descricoes)
    componentes = agrupar_por_inverted_index(linhas, threshold=0.6)
    # Os dois vinhos devem ficar juntos (sao identicos).
    componentes_grandes = [c for c in componentes if len(c) >= 2]
    # Os 100 cervejas devem nao formar 1 bloco de 100 (LATA podado).
    assert all(len(c) <= 50 for c in componentes_grandes)
```

### 5.5 Validacao

```bash
PYTHONPATH=src python3 -m pytest \
    tests/test_inverted_index_descricao.py \
    tests/test_particionamento_fiscal.py -q
# Esperado: 14+ passed (8 da fase 1 + 7 desta)
```

### 5.6 Commit

```
feat(similarity): inverted index para agrupar itens so por descricao

Adiciona src/interface_grafica/services/inverted_index_descricao.py
com:
- agrupar_por_inverted_index: algoritmo central, recebe lista de
  _Linha e retorna componentes conexos
- ordenar_blocos_apenas_por_descricao: funcao publica standalone
  para uso quando o usuario quer ignorar identificadores fiscais

A metodologia complementa o particionamento_fiscal:
- Quando ativada como camada 5, processa apenas itens que
  ficaram fora das camadas 0-3 (sem chave fiscal compativel)
- Quando usada standalone, ignora identificadores fiscais
  inteiramente

Algoritmo: indice invertido token -> [idx], poda de tokens com
DF alto (genericos), Jaccard sobre os pares que compartilham
>= 2 tokens nao-genericos. O(N x T) na construcao, evita o
O(N^2) ingenuo.

Sem dependencias novas. Custo de memoria proporcional ao
vocabulario unico do corpus.
```

---

## 6. Fase 3 — Integracao na UI

### 6.1 Arquivo a editar

`src/interface_grafica/patches/similaridade_agregacao.py`

(O arquivo ja existe e instala botoes via patch incremental sobre
o `AgregacaoWindowMixin`. Adicionar selecao de metodo nesse mesmo
patch.)

### 6.2 Mudancas na UI

Substituir o checkbox unico "Priorizar NCM/CEST" por:

- `QComboBox` com 3 opcoes: "Composto (atual)", "Particionamento
  fiscal (novo)", "Apenas descricao"
- `QCheckBox` "Incluir descricao em produtos sem NCM" (so visivel
  quando metodo = "Particionamento fiscal")
- Aviso `QLabel` quando "Apenas descricao" for selecionado:
  "*Identificadores fiscais nao serao consultados. Revise os
  agrupamentos manualmente.*"

### 6.3 Patch da funcao

```python
# Adicionar no topo do arquivo similaridade_agregacao.py:
from PySide6.QtWidgets import (
    QCheckBox, QComboBox, QHBoxLayout, QLabel, QPushButton, QWidget,
)
from PySide6.QtCore import Qt
```

```python
# Dentro de _build_tab_agregacao_com_similaridade, substituir o
# bloco que cria btn_ordenar_similaridade_desc + chk_similarity_ncm_cest
# por:

if not hasattr(self, "btn_ordenar_similaridade_desc"):
    self.btn_ordenar_similaridade_desc = QPushButton(
        "Ordenar por similaridade"
    )
    self.btn_ordenar_similaridade_desc.setToolTip(
        "Ordena a tabela em blocos visuais. Nao executa agrupamento."
    )

    self.cmb_metodo_similaridade = QComboBox()
    self.cmb_metodo_similaridade.addItems([
        "Composto (legacy)",
        "Particionamento fiscal",
        "Apenas descricao",
    ])
    self.cmb_metodo_similaridade.setToolTip(
        "Composto: metodo classico baseado em score ponderado.\n"
        "Particionamento: agrupa por GTIN/NCM/CEST/UNIDADE primeiro.\n"
        "Apenas descricao: ignora identificadores, so texto."
    )

    self.chk_incluir_desc_sem_ncm = QCheckBox(
        "Incluir descricao em itens sem NCM"
    )
    self.chk_incluir_desc_sem_ncm.setVisible(False)

    self.lbl_aviso_similaridade = QLabel("")
    self.lbl_aviso_similaridade.setStyleSheet("color: #c47900;")
    self.lbl_aviso_similaridade.setVisible(False)

    # Atualiza visibilidade quando muda o metodo:
    def _on_metodo_changed(idx: int) -> None:
        is_particionamento = idx == 1
        is_so_descricao = idx == 2
        self.chk_incluir_desc_sem_ncm.setVisible(is_particionamento)
        if is_so_descricao:
            self.lbl_aviso_similaridade.setText(
                "Identificadores fiscais ignorados. Revise manualmente."
            )
            self.lbl_aviso_similaridade.setVisible(True)
        else:
            self.lbl_aviso_similaridade.setVisible(False)

    self.cmb_metodo_similaridade.currentIndexChanged.connect(_on_metodo_changed)

    parent = self.btn_reprocessar_agregacao.parentWidget()
    layout = parent.layout() if parent is not None else None
    inserted = False
    if layout is not None:
        for idx in range(layout.count()):
            item = layout.itemAt(idx)
            if item is not None and item.widget() is self.btn_reprocessar_agregacao:
                layout.insertWidget(idx + 1, self.btn_ordenar_similaridade_desc)
                layout.insertWidget(idx + 2, self.cmb_metodo_similaridade)
                layout.insertWidget(idx + 3, self.chk_incluir_desc_sem_ncm)
                layout.insertWidget(idx + 4, self.lbl_aviso_similaridade)
                inserted = True
                break
    if not inserted and layout is not None:
        layout.addWidget(self.btn_ordenar_similaridade_desc)
        layout.addWidget(self.cmb_metodo_similaridade)
        layout.addWidget(self.chk_incluir_desc_sem_ncm)
        layout.addWidget(self.lbl_aviso_similaridade)
```

```python
# Substituir a funcao ordenar_agregacao_por_similaridade por:

def ordenar_agregacao_por_similaridade(self) -> None:
    df = self.aggregation_table_model.get_dataframe()
    if df.is_empty():
        self.status.showMessage("Nenhuma linha para ordenar por similaridade.")
        return

    metodo_idx = 0
    if hasattr(self, "cmb_metodo_similaridade"):
        metodo_idx = self.cmb_metodo_similaridade.currentIndex()

    try:
        if metodo_idx == 0:
            # Composto (legacy)
            from interface_grafica.services.descricao_similarity_service import (
                ordenar_blocos_similaridade_descricao,
            )
            df_ordenado = ordenar_blocos_similaridade_descricao(
                df, janela=4, limite_bloco=82, usar_ncm_cest=True,
            )
            mensagem = (
                "Tabela ordenada (metodo composto). "
                "Nenhum agrupamento foi executado."
            )
        elif metodo_idx == 1:
            # Particionamento fiscal
            from interface_grafica.services.particionamento_fiscal import (
                ordenar_blocos_por_particionamento_fiscal,
            )
            incluir_desc = (
                self.chk_incluir_desc_sem_ncm.isChecked()
                if hasattr(self, "chk_incluir_desc_sem_ncm") else False
            )
            df_ordenado = ordenar_blocos_por_particionamento_fiscal(
                df, incluir_camada_so_descricao=incluir_desc,
            )
            mensagem = (
                f"Tabela ordenada (particionamento fiscal). "
                f"{'Camada de descricao ATIVA. ' if incluir_desc else ''}"
                "Nenhum agrupamento foi executado."
            )
        else:
            # Apenas descricao
            from interface_grafica.services.inverted_index_descricao import (
                ordenar_blocos_apenas_por_descricao,
            )
            df_ordenado = ordenar_blocos_apenas_por_descricao(df, threshold=0.5)
            mensagem = (
                "Tabela ordenada (apenas descricao). "
                "Identificadores fiscais ignorados. "
                "Revise os agrupamentos manualmente."
            )

        self.aggregation_table_model.set_dataframe(df_ordenado)
        self._resize_table_once(self.aggregation_table_view, "agregacao_top")
        self.status.showMessage(mensagem)
    except Exception as exc:
        self.show_error("Erro ao ordenar por similaridade", str(exc))
```

### 6.4 Validacao

Esta fase nao tem teste automatizado direto (a UI requer
display server). Validacao manual:

```bash
# Verificar que o codigo importa sem erro:
PYTHONPATH=src python3 -c "
from interface_grafica.patches.similaridade_agregacao import (
    apply_similarity_patch,
)
print('OK')
"

# Suite completa para garantir que nada quebrou:
PYTHONPATH=src python3 -m pytest -q
```

### 6.5 Commit

```
feat(ui): seletor de metodo de similaridade na aba Agregacao

Substitui o checkbox unico 'Priorizar NCM/CEST' por seletor com 3
opcoes:
  - Composto (legacy)
  - Particionamento fiscal (novo)
  - Apenas descricao (ignora identificadores)

Quando 'Particionamento fiscal' e selecionado, exibe checkbox para
ativar a camada 5 (descricao para itens sem NCM).
Quando 'Apenas descricao' e selecionado, exibe aviso para revisao
manual (identificadores fiscais nao consultados).

A funcao ordenar_agregacao_por_similaridade do controller foi
estendida para rotear conforme o metodo selecionado.
```

---

## 7. Fase 4 — Documentacao

### 7.1 Atualizar `docs/similaridade_agregacao.md`

Adicionar secoes apos a 18:

```markdown
## 19. Metodologia alternativa: particionamento por chaves fiscais

Em paralelo ao metodo composto, o sistema oferece uma metodologia
nova baseada em particionamento por identificadores fiscais.

### Hierarquia de camadas

| Camada | Chave de particao | Threshold textual |
|--------|---------------------|---------------------|
| 0 | GTIN igual | nenhum (bloco automatico) |
| 1 | NCM + CEST + UNIDADE | Jaccard >= 0.50 |
| 2 | NCM + UNIDADE | Jaccard >= 0.65 |
| 3 | NCM4 + UNIDADE | Jaccard >= 0.80 |
| 4 | (sem chave) | singleton |
| 5 | (opcional) inverted index | Jaccard >= 0.70 |

Cada item participa apenas da camada mais forte em que encontra
companhia.

### Vantagens

- Configuracao com 4 thresholds em vez de ~25 parametros do composto.
- Pipeline previsivel: tempo cresce linearmente com tamanho medio
  de bucket, nao com N^2.
- Auditavel: cada bloco traz a chave fiscal que o originou em
  `sim_chave_fiscal`.

### Quando usar

- **Particionamento fiscal**: quando os identificadores estao bem
  preenchidos (>80% dos itens com NCM e UNIDADE).
- **Composto (legacy)**: quando a base tem muito ruido em NCM/CEST
  e o sinal textual e mais confiavel.
- **Apenas descricao**: analise exploratoria, comparacao cross-CNPJ,
  ou casos onde o operador quer deliberadamente ignorar fiscais.

## 20. Modo "apenas descricao" via inverted index

Quando os identificadores fiscais sao pouco confiaveis ou o
usuario quer ignora-los deliberadamente, ha um terceiro metodo
disponivel: agrupamento via indice invertido sobre tokens da
descricao.

### Como funciona

1. Cada descricao e tokenizada (palavras com >=3 chars contendo letra).
2. Um indice invertido `token -> [idx, ...]` e construido em O(N*T).
3. Tokens muito comuns (DF > 5% do corpus) sao podados como genericos.
4. Pares candidatos sao itens que compartilham >= 2 tokens nao-genericos.
5. Jaccard e calculado apenas nos candidatos. Pares com sim >= 0.5
   sao unidos via union-find.

### Caracteristicas

- Sem dependencias novas. Roda em Polars puro.
- Custo memoria: proporcional ao vocabulario unico do corpus.
- Recall tipico: 85-95% em descricoes fiscais.
- Atencao: nao deve ser usado para decisoes fiscais sem revisao
  humana - itens com NCMs incompativeis podem ficar no mesmo bloco.

## 21. Colunas geradas pela metodologia de particionamento

Alem das colunas do metodo composto, o particionamento adiciona:

```text
sim_camada       (int)    Camada que originou o agrupamento (0-5)
sim_motivo       (str)    GTIN_IGUAL | NCM+CEST+UNID | NCM+UNID
                          | NCM4+UNID | DESC_TOKENS | ISOLADO
sim_chave_fiscal (str)    Chave concreta usada (ex: 'NCM=22030000|UN=UN')
```

Estas colunas substituem `sim_motivos` (com 's') e `sim_score_*`
do metodo composto.

## 22. Selecao de metodo na UI

Na aba Agregacao, alem do botao **Ordenar por similaridade**,
existe agora um seletor com 3 opcoes:

- **Composto (legacy)**: o metodo da secao 1-18 deste documento.
- **Particionamento fiscal**: as secoes 19-21.
- **Apenas descricao**: secao 20 isolada.

Apos o particionamento fiscal, um checkbox adicional permite
ativar a camada 5 (descricao para itens sem NCM). Por seguranca
fiscal, esse checkbox e desligado por default.
```

### 7.2 Criar `docs/PLANO_MELHORIAS_SIMILARIDADE.md`

Documento curto registrando o que foi entregue:

```markdown
# Plano de melhorias do servico de similaridade

Status: implementado em feat/similaridade-particionamento.

## Sprint 1 - melhorias incrementais ao metodo composto

- NCM hierarquico de 5 niveis (item/subposicao/posicao/capitulo)
- Canonizacao de unidades antes de extrair numeros (ML/L, G/KG)
- Caps de bucket e top-k por linha
- Cap de tamanho de bloco e coesao minima

## Sprint 2 - metodologia de particionamento por chaves fiscais

- Modulo particionamento_fiscal.py com 4 camadas obrigatorias
  (GTIN, NCM+CEST+UNID, NCM+UNID, NCM4+UNID) + 1 opcional
  (descricao via inverted index).

## Sprint 3 - modo apenas descricao via inverted index

- Modulo inverted_index_descricao.py com poda por document
  frequency, comparacao apenas dentro de buckets de tokens
  compartilhados.

## Sprint 4 - integracao na UI

- Seletor de metodo na aba Agregacao.
- Checkbox para camada 5 visivel apenas no metodo de
  particionamento.
- Aviso visual quando metodo "apenas descricao" e selecionado.

## Possiveis melhorias futuras (nao implementadas)

- MinHash + LSH para corpus muito ruidoso (datasketch opcional).
- Cache parquet sidecar para vocabulario do inverted index.
- Embeddings semanticos (sentence-transformers) com cache de
  vetores - alto custo de pre-computacao, melhor recall.
- Dicionario de marcas alimentavel para sinal explicito de marca.
```

### 7.3 Commit

```
docs(similarity): documenta metodologia de particionamento e camada de descricao

Atualiza docs/similaridade_agregacao.md com 4 novas secoes (19-22)
cobrindo:
- particionamento por chaves fiscais
- modo apenas descricao via inverted index
- novas colunas sim_camada, sim_motivo, sim_chave_fiscal
- seletor de metodo na UI

Cria docs/PLANO_MELHORIAS_SIMILARIDADE.md como registro do que
foi implementado nesta branch.
```

---

## 8. Fase 5 — Validacao final

### 8.1 Suite completa

```bash
PYTHONPATH=src python3 -m pytest \
    tests/test_descricao_similarity_service.py \
    tests/test_unidades_descricao.py \
    tests/test_particionamento_fiscal.py \
    tests/test_inverted_index_descricao.py \
    tests/test_text_normalizacao_descricao.py \
    -q
# Esperado: ~30 passed
```

### 8.2 Suite global do projeto

```bash
PYTHONPATH=src python3 -m pytest -q
```

Expectativa: tudo verde. Se algum teste fora da area de similaridade
quebrar, **investigar** antes de propor merge.

### 8.3 Smoke import

```bash
PYTHONPATH=src python3 -c "
from interface_grafica.services.descricao_similarity_service import (
    ordenar_blocos_similaridade_descricao,
)
from interface_grafica.services.particionamento_fiscal import (
    ordenar_blocos_por_particionamento_fiscal,
)
from interface_grafica.services.inverted_index_descricao import (
    ordenar_blocos_apenas_por_descricao,
)
from interface_grafica.patches.similaridade_agregacao import (
    apply_similarity_patch,
)
print('Todos os imports OK')
"
```

### 8.4 Diagnostico de performance comparativo

Criar script `benchmarks/comparar_metodos_similaridade.py`:

```python
"""Comparacao rapida do composto vs particionamento.

Uso:
    PYTHONPATH=src python3 benchmarks/comparar_metodos_similaridade.py
"""
import time
import polars as pl

from interface_grafica.services.descricao_similarity_service import (
    ordenar_blocos_similaridade_descricao,
)
from interface_grafica.services.particionamento_fiscal import (
    ordenar_blocos_por_particionamento_fiscal,
)


def gerar_dataset(n: int) -> pl.DataFrame:
    import random
    random.seed(42)
    marcas = ["HEINEKEN", "BRAHMA", "SKOL", "ANTARTICA", "ITAIPAVA"]
    formatos = ["LATA 350ML", "LONG NECK 330ML", "GARRAFA 600ML"]
    descricoes, ncms, cests, gtins, unidades = [], [], [], [], []
    for i in range(n):
        m = random.choice(marcas)
        f = random.choice(formatos)
        descricoes.append(f"CERVEJA {m} {f}")
        ncms.append(random.choice(["22030000", "22030010", "22030090"]))
        cests.append(random.choice(["0302100", "0302101", ""]))
        gtins.append(f"789{i:09}")
        unidades.append(random.choice(["UN", "CX", ""]))
    return pl.DataFrame({
        "id_agrupado": [str(i) for i in range(n)],
        "descr_padrao": descricoes,
        "ncm_padrao": ncms,
        "cest_padrao": cests,
        "gtin_padrao": gtins,
        "unid_padrao": unidades,
    })


for n in [100, 1000, 5000]:
    df = gerar_dataset(n)
    t0 = time.perf_counter()
    out_a = ordenar_blocos_similaridade_descricao(df)
    dt_a = time.perf_counter() - t0
    t0 = time.perf_counter()
    out_b = ordenar_blocos_por_particionamento_fiscal(df)
    dt_b = time.perf_counter() - t0
    print(f"n={n:5d} | composto={dt_a:.2f}s | particionamento={dt_b:.2f}s "
          f"| speedup={dt_a/dt_b:.1f}x")
```

Rodar e incluir o output no relatorio final.

### 8.5 Relatorio final

Gerar `git format-patch -N HEAD~N` (onde N = numero de commits da
fase). Apresentar ao usuario:

- Lista dos commits
- Resultado da suite (tests passed)
- Output do benchmark
- Decisoes que foram diferentes do plano e por que
- Pontos abertos / sugestoes

---

## 9. Apendice — Armadilhas conhecidas

1. **CRLF preservado**: o arquivo `descricao_similarity_service.py`
   usa CRLF. `git config core.autocrlf false` antes de comecar.
2. **`pl.UInt32` em indices**: `with_row_index` retorna UInt32.
   Comparar com `int` Python costuma funcionar mas em alguns casos
   exige cast explicito.
3. **`structlog.get_logger`** em testes: o logger e configurado
   pelo app real. Em teste isolado pode logar para stdout. Usar
   `structlog.testing.capture_logs()` se for necessario assertar
   logs.
4. **Tokens fortes** com `_strong_tokens` do servico legacy x
   `_tokens_fortes` (com 's' no final) deste novo modulo: sao
   funcoes diferentes em modulos diferentes. Nao confundir.
5. **`utilitarios.text.normalize_desc`** e a normalizacao canonica
   de descricao fiscal. Manter ela como ponte; nao reimplementar.
6. **`utilitarios.unidades_descricao.normalizar_unidades_em_texto`**
   foi adicionado pelo patch 0002. Reutilizar no novo modulo
   (em `_construir_linhas`).
7. **Camada 5 ativada** em DataFrame onde ja existem chaves
   fiscais: a camada 5 so processa o que sobrou das camadas 0-3.
   Nao duplica blocos.
8. **GTIN vazio** nao deve gerar bloco da camada 0. A `chave_fn`
   deve retornar `None` para gtin == "".
9. **Aliases de unidade**: o projeto pode ter colunas com nome
   diferente. A lista `ALIASES_UNIDADE` cobre `unid_padrao`,
   `unidade_padrao`, etc. Se o teste nao encontrar a coluna, e
   porque o nome real esta fora da lista - estender a lista.

---

## 10. Checklist final antes de pedir review

- [ ] 5 fases, 5+ commits
- [ ] Suite de testes verde (incluindo os 17 do baseline)
- [ ] `git format-patch` limpo, sem mudanca de line endings
- [ ] `docs/similaridade_agregacao.md` atualizado
- [ ] `docs/PLANO_MELHORIAS_SIMILARIDADE.md` criado
- [ ] Benchmark rodado e resultado anotado
- [ ] Comportamento default 100% retrocompativel verificado
- [ ] Nenhuma dependencia nova no `pyproject.toml`
- [ ] Smoke import OK
