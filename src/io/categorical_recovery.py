"""
src/io/categorical_recovery.py
================================

Hook único de leitura tipada de Parquets fiscais do `audit_pyside`.

Este módulo é a **PR 2** do plano de auditoria de campos categóricos
(<https://www.notion.so/358edc8b7d5d81cfb33ce023d4cee84f>). Implementa:

- ``scan_parquet_typed(path)`` — wrapper de ``pl.scan_parquet`` que aplica
  casts categóricos defensivamente após o scan, contornando a regressão
  Polars #19389 (rebaixamento Enum→Categorical em alguns paths lazy).
- ``ENUM_MAP`` — mapeamento coluna → ``pl.Enum`` carregado de
  ``ref/fiscal_codes_2026.json``.
- ``CATEG_MAP`` — mapeamento coluna → ``pl.Categorical()`` para domínios
  dinâmicos (NCM, CEST, unid).
- ``INVARIANT_BLOCKLIST`` — defesa em profundidade: nenhum cast é
  aplicado em colunas listadas, mesmo se aparecerem nos mapas.
- ``cast_dataframe_typed(df)`` — versão eager para casos onde o caller já
  tem um ``DataFrame``.

Este módulo NÃO escreve Parquets nem altera schemas persistidos. É só
leitura + cast em RAM. A persistência (PR 4) é tratada em outro módulo
com gates do ``differential_harness``.

Usage
-----
    from audit_pyside.io.categorical_recovery import scan_parquet_typed

    lf = scan_parquet_typed("dados/c170_xml.parquet")
    # lf agora tem cfop como pl.Enum, ncm como pl.Categorical, etc.
    df = lf.filter(pl.col("cfop") == "5102").collect()

Design constraints
------------------
1. **Invariantes fiscais nunca recebem cast** — defesa em profundidade
   contra Polars #24034 (high-cardinality regression). Listadas em
   ``INVARIANT_BLOCKLIST``; bloqueio é aplicado no final de cada função
   pública, não no carregamento do JSON.
2. **Determinístico** — mesma combinação (Parquet + JSON) sempre produz
   o mesmo schema de saída.
3. **Idempotente** — chamar em LazyFrame já tipado é no-op.
4. **Falha rápida em CFOP/CST inválido** — `pl.Enum` levanta
   ``InvalidOperationError`` na primeira coleta; isso é desejado para
   capturar dados sujos do SPED.
5. **Tolerante a NCM/CEST novos** — `pl.Categorical()` aceita valores
   ainda não vistos sem erro.

References
----------
- Plano Notion: ``358edc8b7d5d81cfb33ce023d4cee84f`` §C, §E.4
- Polars #19389: ``scan_parquet`` rebaixa Enum
- Polars #18868: predicate pushdown não funciona em Categorical/Enum
- Polars #24034: high-cardinality Categorical inflation
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Iterable, Mapping
from functools import lru_cache
from pathlib import Path
from typing import Any, Final

import polars as pl

logger = logging.getLogger(__name__)


# =====================================================================
# Constantes de configuração
# =====================================================================

#: Caminho default do cadastro de códigos fiscais. Pode ser sobrescrito
#: via parâmetro ``codes_path`` em ``load_fiscal_codes`` ou variável de
#: ambiente ``AUDIT_PYSIDE_FISCAL_CODES``.
DEFAULT_CODES_PATH: Final[Path] = (
    Path(__file__).resolve().parent.parent.parent
    / "ref"
    / "fiscal_codes_2026.json"
)

#: Invariantes fiscais — nunca aplicar cast categórico (defesa em
#: profundidade). Mesmo se um mapa adicionar essas colunas por engano.
#: Issue Polars #24034: categorizar colunas high-cardinality infla 10×.
INVARIANT_BLOCKLIST: Final[frozenset[str]] = frozenset({
    "id_agrupado",
    "id_agregado",
    "__qtd_decl_final_audit__",
    "q_conv",
    "q_conv_fisica",
})

#: Mapeamento coluna → chave do JSON. Cada chave do JSON deve produzir
#: um ``pl.Enum``. Coluna pode aparecer em múltiplas variantes (case ou
#: prefixo) refletindo o schema heterogêneo do `audit_pyside`.
COLUMN_TO_ENUM_KEY: Final[Mapping[str, str]] = {
    # UF (sigla)
    "uf": "uf",
    "UF": "uf",
    "emit_UF": "uf",
    "dest_UF": "uf",
    # UF (código IBGE)
    "co_uf_emit": "uf_codigo_ibge",
    "co_uf_dest": "uf_codigo_ibge",
    # CFOP (composto: união de todas as listas cfop_*)
    "cfop": "cfop_all",
    "Cfop": "cfop_all",
    "Cfop_c170": "cfop_all",
    "prod_CFOP": "cfop_all",
    # CST ICMS
    "cst_icms": "cst_icms_completo",
    "Cst": "cst_icms_completo",
    "Cst_c170": "cst_icms_completo",
    "icms_CST": "cst_icms_completo",
    # CSOSN
    "csosn": "csosn",
    "icms_CSOSN": "csosn",
    # CST PIS/COFINS
    "pis_CST": "cst_pis_cofins_completo",
    "cofins_CST": "cst_pis_cofins_completo",
    "cst_pis": "cst_pis_cofins_completo",
    "cst_cofins": "cst_pis_cofins_completo",
    # Modelo
    "mod": "modelo_documento",
    "ide_mod": "modelo_documento",
    # Tipo operação NF-e
    "ide_tpNF": "tipo_operacao_nfe",
    # Tipo operação SPED
    "tipo_operacao": "indicador_operacao_sped_c170",
    "Tipo_operacao": "indicador_operacao_sped_c170",
    "Tipo_operacao_c170": "indicador_operacao_sped_c170",
    "ind_oper": "indicador_operacao_sped_c170",
    # Outros indicadores NF-e
    "ide_tpEmis": "tipo_emissao_nfe",
    "ide_tpAmb": "tipo_ambiente_nfe",
    "ide_finNFe": "finalidade_nfe",
    "ide_indPres": "indicador_presenca_nfe",
    "indPres": "indicador_presenca_nfe",
    "indIEDest": "indicador_ie_destinatario",
    "dest_indIEDest": "indicador_ie_destinatario",
    "modBC": "modalidade_base_calculo_icms",
    "icms_modBC": "modalidade_base_calculo_icms",
    "modBCST": "modalidade_base_calculo_icms_st",
    "icms_modBCST": "modalidade_base_calculo_icms_st",
    "motDesICMS": "motivo_desoneracao_icms",
    "icms_motDesICMS": "motivo_desoneracao_icms",
    "modFrete": "modalidade_frete_nfe",
    "transp_modFrete": "modalidade_frete_nfe",
    "tPag": "meio_pagamento_nfe",
    "detPag_tPag": "meio_pagamento_nfe",
    "tBand": "bandeira_cartao_nfe",
    "card_tBand": "bandeira_cartao_nfe",
    "tpIntegra": "tipo_integracao_pagamento_nfe",
    "card_tpIntegra": "tipo_integracao_pagamento_nfe",
    "cRegTrib": "regime_especial_tributacao_issqn",
    "ISSQNtot_cRegTrib": "regime_especial_tributacao_issqn",
    "finnfe": "finalidade_nfe",
    "tpEmis": "tipo_emissao_nfe",
    # Regime tributário
    "CRT": "regime_tributario_crt",
    "crt": "regime_tributario_crt",
    "regime_tributario": "regime_tributario_crt",
    # Origem ICMS
    "icms_orig": "cst_icms_origem",
    "orig": "cst_icms_origem",
    # SPED
    "cod_sit": "codigo_situacao_documento_sped",
    "IND_MOV": "indicador_movimento_sped",
    "ind_mov": "indicador_movimento_sped",
    "tipo_item": "tipo_item_sped",
    "Tipo_item": "tipo_item_sped",
    "Tipo_item_c170": "tipo_item_sped",
}

BOOLEAN_TRUE_VALUES_BY_COLUMN: Final[Mapping[str, frozenset[str]]] = {
    "indFinal": frozenset({"1"}),
    "ide_indFinal": frozenset({"1"}),
    "indIntermed": frozenset({"1"}),
    "ide_indIntermed": frozenset({"1"}),
    "indEscala": frozenset({"S", "s", "1"}),
    "prod_indEscala": frozenset({"S", "s", "1"}),
    "indTot": frozenset({"1"}),
    "prod_indTot": frozenset({"1"}),
    "indPag": frozenset({"1"}),
    "detPag_indPag": frozenset({"1"}),
    "IND_DAD": frozenset({"1", "S", "s"}),
    "IND_PROF": frozenset({"1", "S", "s"}),
    "IND_SOC": frozenset({"1", "S", "s"}),
    "IND_ESC": frozenset({"1", "S", "s"}),
    "IND_ARM": frozenset({"1", "S", "s"}),
    "IND_REC": frozenset({"1", "S", "s"}),
    "IND_VEIC": frozenset({"1", "S", "s"}),
    "IND_NAV": frozenset({"1", "S", "s"}),
    "IND_AJ": frozenset({"1", "S", "s"}),
    "NAT_EXP": frozenset({"1", "S", "s"}),
}

BOOLEAN_FALSE_VALUES_BY_COLUMN: Final[Mapping[str, frozenset[str]]] = {
    column: frozenset({"0", "N", "n"}) for column in BOOLEAN_TRUE_VALUES_BY_COLUMN
}

# IND_MOV no EFD e invertido: 0=SIM, 1=NAO. Manter como Enum evita bool ambigua.
INVERTED_BOOLEAN_FIELDS: Final[frozenset[str]] = frozenset({"IND_MOV", "ind_mov", "Ind_mov"})

#: Colunas que são ``pl.Categorical()`` (domínio dinâmico, não fechado).
#: NCM/CEST mudam ao longo do ano (NCM 2022→2024); unid tem strings
#: sujas no SPED. Nunca usar ``pl.Enum`` aqui.
DYNAMIC_CATEGORICAL_COLUMNS: Final[frozenset[str]] = frozenset({
    # NCM
    "ncm", "Ncm", "Ncm_c170", "prod_NCM", "ncm_padrao",
    # CEST
    "cest", "Cest", "Cest_c170", "prod_CEST", "cest_padrao",
    # Unidade comercial
    "unid", "Unid", "Unid_c170", "prod_uCom", "prod_uTrib", "unid_ref",
    # CNAE
    "cnae", "cnae_principal", "cnae_secundario",
    # Fontes/regras XML
    "fonte", "fonte_xml", "regra_vinculo_xml",
    "status_xml", "fator_origem",
    # SEFIN agregação
    "co_sefin_agr", "co_sefin_final",
    # Indicadores fiscais booleanos textuais
    "it_in_st", "it_in_reducao_credito",
})


# =====================================================================
# Carregamento do JSON e construção do ENUM_MAP
# =====================================================================


@lru_cache(maxsize=4)
def load_fiscal_codes(codes_path: Path | None = None) -> dict[str, list[str]]:
    """
    Carrega o JSON ``ref/fiscal_codes_2026.json`` e retorna apenas as
    listas de códigos (sem `_metadata` e sem `_observacoes_aplicacao`).

    Para o agregado ``cfop_all`` (não presente no JSON), une todas as
    chaves que começam com ``cfop_`` em ordem determinística.

    O resultado é cacheado por path. Em testes, use ``cache_clear()``
    explicitamente entre runs ou passe um ``codes_path`` distinto.

    Args:
        codes_path: Caminho do JSON. Se ``None``, usa
            ``DEFAULT_CODES_PATH``.

    Returns:
        Dicionário ``{enum_key: [valor1, valor2, ...]}``. Inclui a chave
        sintética ``cfop_all`` agregando todas as variantes de CFOP.

    Raises:
        FileNotFoundError: Se o JSON não existe.
        ValueError: Se o JSON está malformado ou faltam chaves esperadas.
    """
    env_path = os.environ.get("AUDIT_PYSIDE_FISCAL_CODES")
    path = codes_path or (Path(env_path) if env_path else DEFAULT_CODES_PATH)
    if not path.exists():
        raise FileNotFoundError(
            f"Cadastro de códigos fiscais não encontrado em {path}. "
            f"Veja plano Notion 358edc8b7d5d81cfb33ce023d4cee84f §A."
        )
    raw: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))

    codes: dict[str, list[str]] = {}
    for key, value in raw.items():
        if key.startswith("_"):
            continue
        if isinstance(value, dict):
            value = list(value.keys())
        elif not isinstance(value, list):
            continue
        if not all(isinstance(v, str) for v in value):
            raise ValueError(
                f"Chave {key!r} em {path} contém valores não-string"
            )
        codes[key] = list(value)

    # Constrói cfop_all = união determinística de todas cfop_*
    cfop_all: list[str] = []
    seen: set[str] = set()
    for key in sorted(codes):
        if key.startswith("cfop_"):
            for code in codes[key]:
                if code not in seen:
                    cfop_all.append(code)
                    seen.add(code)
    if cfop_all:
        codes["cfop_all"] = cfop_all
    else:
        logger.warning(
            "Nenhuma chave cfop_* encontrada em %s; cfop_all não criado", path
        )

    logger.debug(
        "Cadastro fiscal carregado: %d listas, %d CFOPs em cfop_all",
        len(codes), len(codes.get("cfop_all", [])),
    )
    return codes


def build_enum_map(
    codes_path: Path | None = None,
) -> dict[str, pl.Enum]:
    """
    Constrói o mapeamento ``coluna → pl.Enum`` a partir do JSON.

    Este é o ENUM_MAP da §E.4 do plano. Cada coluna em
    ``COLUMN_TO_ENUM_KEY`` é mapeada para um ``pl.Enum`` com a lista
    correspondente do JSON.

    Args:
        codes_path: Caminho do JSON (default: DEFAULT_CODES_PATH).

    Returns:
        Dicionário ``{coluna: pl.Enum(VALUES)}``.

    Raises:
        ValueError: Se uma chave de ``COLUMN_TO_ENUM_KEY`` não existe
            no JSON.
    """
    codes = load_fiscal_codes(codes_path)
    enum_map: dict[str, pl.Enum] = {}
    missing: list[str] = []
    for column, enum_key in COLUMN_TO_ENUM_KEY.items():
        if enum_key not in codes:
            missing.append(f"{column} → {enum_key}")
            continue
        enum_map[column] = pl.Enum(codes[enum_key])
    if missing:
        raise ValueError(
            f"Chaves do ENUM_MAP ausentes no JSON: {missing}. "
            f"Atualize ref/fiscal_codes_2026.json."
        )
    return enum_map


def build_categorical_map() -> dict[str, pl.DataType]:
    """
    Constrói o mapeamento ``coluna → pl.Categorical()``.

    Categorical não tem valores pré-definidos; todas as colunas em
    ``DYNAMIC_CATEGORICAL_COLUMNS`` recebem o mesmo dtype factory.

    Returns:
        Dicionário ``{coluna: pl.Categorical()}``.
    """
    # pl.Categorical é uma factory; reutilizamos a mesma instância.
    cat = pl.Categorical()
    return {col: cat for col in DYNAMIC_CATEGORICAL_COLUMNS}


# =====================================================================
# Hook principal de leitura
# =====================================================================


def _is_enum_dtype(dtype: pl.DataType, target: pl.Enum) -> bool:
    """
    Verifica se um dtype Polars já é o Enum desejado.

    Polars #19389: ``scan_parquet`` pode retornar Categorical onde
    deveria retornar Enum. Esta verificação é estrita.
    """
    if not isinstance(dtype, pl.Enum):
        return False
    # Comparar as listas de categorias (Polars 1.x: .categories)
    try:
        return list(dtype.categories) == list(target.categories)
    except AttributeError:
        # Fallback: igualdade direta (alguns paths Polars)
        return dtype == target


def _is_categorical_dtype(dtype: pl.DataType) -> bool:
    """Verifica se dtype é ``pl.Categorical`` (não Enum)."""
    # pl.Categorical é diferente de pl.Enum em Polars 1.x
    return isinstance(dtype, pl.Categorical) and not isinstance(dtype, pl.Enum)


def _boolean_cast_expr(column: str) -> pl.Expr:
    """Expressao de cast 0/1 ou S/N para Boolean preservando nulos."""
    true_values = BOOLEAN_TRUE_VALUES_BY_COLUMN[column]
    false_values = BOOLEAN_FALSE_VALUES_BY_COLUMN[column]
    value = pl.col(column).cast(pl.String)
    return (
        pl.when(value.is_in(true_values))
        .then(pl.lit(True))
        .when(value.is_in(false_values))
        .then(pl.lit(False))
        .otherwise(None)
        .alias(column)
    )


def scan_parquet_typed(
    path: str | Path,
    *,
    codes_path: Path | None = None,
    extra_enum_map: Mapping[str, pl.Enum] | None = None,
    extra_categorical_columns: Iterable[str] = (),
    blocklist: Iterable[str] = (),
    apply_boolean_cast: bool = True,
) -> pl.LazyFrame:
    """
    Wrapper de ``pl.scan_parquet`` com casts categóricos defensivos.

    Este é o ponto de entrada único para leitura de Parquets fiscais
    pelo `audit_pyside`. Faz scan lazy + re-cast no schema observado:

    1. Aplica ``pl.Enum`` em colunas conhecidas (CFOP, CST, UF, etc.).
    2. Aplica ``pl.Categorical`` em colunas dinâmicas (NCM, CEST, unid).
    3. **Nunca** toca colunas em ``INVARIANT_BLOCKLIST`` ou ``blocklist``
       extra (defesa em profundidade vs Polars #24034).
    4. Skip de cast quando a coluna já tem o dtype desejado
       (idempotente).

    Args:
        path: Caminho do Parquet (arquivo único; para múltiplos use
            glob compatível com ``pl.scan_parquet``).
        codes_path: Caminho do JSON de códigos. Default:
            ``DEFAULT_CODES_PATH``.
        extra_enum_map: Mapeamentos adicionais coluna → ``pl.Enum``,
            úteis para campos ad-hoc não no cadastro.
        extra_categorical_columns: Colunas adicionais para
            ``pl.Categorical()``.
        blocklist: Colunas a NÃO castar, somadas a
            ``INVARIANT_BLOCKLIST``.

    Returns:
        ``pl.LazyFrame`` com schema tipado. Se uma coluna do mapa não
        existir no Parquet, é silenciosamente ignorada.

    Raises:
        polars.exceptions.InvalidOperationError: Se o Parquet contém
            valores fora do domínio de um ``pl.Enum`` (CFOP inválido,
            etc.). Esta é a falha por design — captura dados sujos.

    Example:
        >>> lf = scan_parquet_typed("dados/c170.parquet")
        >>> lf.collect_schema()["cfop"]  # pl.Enum, não pl.String
    """
    enum_map = dict(build_enum_map(codes_path))
    if extra_enum_map:
        enum_map.update(extra_enum_map)

    categorical_columns = set(DYNAMIC_CATEGORICAL_COLUMNS)
    categorical_columns.update(extra_categorical_columns)

    full_blocklist = set(INVARIANT_BLOCKLIST)
    full_blocklist.update(blocklist)

    # Defesa em profundidade: remover blocklist dos mapas
    for col in full_blocklist:
        if col in enum_map:
            logger.warning(
                "Coluna %r está em INVARIANT_BLOCKLIST mas apareceu em "
                "ENUM_MAP — removendo do cast. Verifique o cadastro.",
                col,
            )
            enum_map.pop(col, None)
        categorical_columns.discard(col)

    lf = pl.scan_parquet(path)
    schema = lf.collect_schema()
    schema_names = set(schema.names())

    cast_exprs: list[pl.Expr] = []

    # Booleans reais (0/1 ou S/N). Campos invertidos/direcionais ficam como Enum.
    boolean_columns = (
        set(BOOLEAN_TRUE_VALUES_BY_COLUMN) - set(INVERTED_BOOLEAN_FIELDS) - full_blocklist
        if apply_boolean_cast
        else set()
    )
    for column in boolean_columns:
        enum_map.pop(column, None)
        categorical_columns.discard(column)
        if column not in schema_names:
            continue
        if schema[column] == pl.Boolean:
            continue
        cast_exprs.append(_boolean_cast_expr(column))

    # Enums
    for column, target_enum in enum_map.items():
        if column not in schema_names:
            continue
        current = schema[column]
        if _is_enum_dtype(current, target_enum):
            continue  # idempotente
        # Polars exige cast intermediário para String quando origem
        # é Categorical com categorias diferentes (Polars #19389).
        cast_exprs.append(
            pl.col(column).cast(pl.String).cast(target_enum)
        )

    # Categoricals
    cat_dtype = pl.Categorical()
    for column in categorical_columns:
        if column not in schema_names:
            continue
        current = schema[column]
        if _is_categorical_dtype(current):
            continue
        if isinstance(current, pl.Enum):
            # Não rebaixar Enum→Categorical sem motivo
            continue
        cast_exprs.append(pl.col(column).cast(cat_dtype))

    if cast_exprs:
        logger.debug(
            "scan_parquet_typed(%s): aplicando %d casts", path, len(cast_exprs)
        )
        lf = lf.with_columns(cast_exprs)

    return lf


def cast_dataframe_typed(
    df: pl.DataFrame,
    *,
    codes_path: Path | None = None,
    extra_enum_map: Mapping[str, pl.Enum] | None = None,
    extra_categorical_columns: Iterable[str] = (),
    blocklist: Iterable[str] = (),
    apply_boolean_cast: bool = True,
) -> pl.DataFrame:
    """
    Versão eager de ``scan_parquet_typed`` para DataFrames já materializados.

    Útil quando o caller obteve o DataFrame de outra fonte (ex.: query
    DuckDB convertida para Polars) e quer aplicar tipagem uniforme.

    Args:
        df: DataFrame a tipar.
        codes_path, extra_enum_map, extra_categorical_columns, blocklist:
            Idênticos a ``scan_parquet_typed``.

    Returns:
        Novo DataFrame com schema tipado. O original é preservado.
    """
    lf = df.lazy()
    enum_map = dict(build_enum_map(codes_path))
    if extra_enum_map:
        enum_map.update(extra_enum_map)

    categorical_columns = set(DYNAMIC_CATEGORICAL_COLUMNS)
    categorical_columns.update(extra_categorical_columns)

    full_blocklist = set(INVARIANT_BLOCKLIST)
    full_blocklist.update(blocklist)
    for col in full_blocklist:
        enum_map.pop(col, None)
        categorical_columns.discard(col)

    schema = df.schema
    schema_names = set(schema.keys())
    cast_exprs: list[pl.Expr] = []

    boolean_columns = (
        set(BOOLEAN_TRUE_VALUES_BY_COLUMN) - set(INVERTED_BOOLEAN_FIELDS) - full_blocklist
        if apply_boolean_cast
        else set()
    )
    for column in boolean_columns:
        enum_map.pop(column, None)
        categorical_columns.discard(column)
        if column not in schema_names:
            continue
        if schema[column] == pl.Boolean:
            continue
        cast_exprs.append(_boolean_cast_expr(column))

    for column, target_enum in enum_map.items():
        if column not in schema_names:
            continue
        if _is_enum_dtype(schema[column], target_enum):
            continue
        cast_exprs.append(
            pl.col(column).cast(pl.String).cast(target_enum)
        )

    cat_dtype = pl.Categorical()
    for column in categorical_columns:
        if column not in schema_names:
            continue
        current = schema[column]
        if _is_categorical_dtype(current) or isinstance(current, pl.Enum):
            continue
        cast_exprs.append(pl.col(column).cast(cat_dtype))

    if not cast_exprs:
        return df
    return lf.with_columns(cast_exprs).collect()


# =====================================================================
# Validação e introspecção (úteis para differential_harness)
# =====================================================================


def validate_schema_post_cast(
    lf: pl.LazyFrame,
    *,
    codes_path: Path | None = None,
) -> dict[str, str]:
    """
    Valida que um LazyFrame tem o schema esperado após cast.

    Retorna um diff: ``{coluna: motivo}`` para colunas que deveriam
    estar tipadas mas não estão. Lista vazia = schema OK.

    Útil em ``differential_harness`` para garantir que o cast foi
    aplicado antes de comparar invariantes.

    Args:
        lf: LazyFrame a validar (tipicamente saída de
            ``scan_parquet_typed``).
        codes_path: Caminho do JSON.

    Returns:
        Dicionário com discrepâncias. Vazio se schema está correto.
    """
    enum_map = build_enum_map(codes_path)
    schema = lf.collect_schema()
    diffs: dict[str, str] = {}

    for column, target_enum in enum_map.items():
        if column not in schema.names():
            continue
        if column in INVARIANT_BLOCKLIST:
            continue
        current = schema[column]
        if not _is_enum_dtype(current, target_enum):
            diffs[column] = (
                f"esperado pl.Enum, encontrado {current!r}"
            )

    for column in DYNAMIC_CATEGORICAL_COLUMNS:
        if column not in schema.names():
            continue
        if column in INVARIANT_BLOCKLIST:
            continue
        current = schema[column]
        if not (_is_categorical_dtype(current) or isinstance(current, pl.Enum)):
            diffs[column] = (
                f"esperado pl.Categorical, encontrado {current!r}"
            )

    for column in BOOLEAN_TRUE_VALUES_BY_COLUMN:
        if column not in schema.names() or column in INVERTED_BOOLEAN_FIELDS:
            continue
        current = schema[column]
        if current != pl.Boolean:
            diffs[column] = f"esperado pl.Boolean, encontrado {current!r}"

    return diffs


def get_invariant_dtypes(lf: pl.LazyFrame) -> dict[str, str]:
    """
    Retorna o dtype atual das 5 invariantes fiscais.

    Usado pelo ``differential_harness`` para garantir que as invariantes
    nunca foram convertidas para Categorical/Enum (issue Polars #24034).

    Args:
        lf: LazyFrame a inspecionar.

    Returns:
        ``{coluna: dtype_repr}`` apenas para colunas presentes.
    """
    schema = lf.collect_schema()
    return {
        col: str(schema[col])
        for col in INVARIANT_BLOCKLIST
        if col in schema.names()
    }


def assert_no_invariant_categorized(lf: pl.LazyFrame) -> None:
    """
    Lança ``AssertionError`` se alguma invariante foi categorizada.

    Hard rule do plano-mestre: nenhuma invariante fiscal pode ser
    ``pl.Categorical`` ou ``pl.Enum``. Issue Polars #24034 infla 10×
    a serialização nesses casos.

    Args:
        lf: LazyFrame a verificar.

    Raises:
        AssertionError: Se alguma invariante está categorizada.
    """
    schema = lf.collect_schema()
    violations: list[str] = []
    for col in INVARIANT_BLOCKLIST:
        if col not in schema.names():
            continue
        dtype = schema[col]
        if isinstance(dtype, pl.Categorical) or isinstance(dtype, pl.Enum):
            violations.append(f"{col}: {dtype!r}")
    if violations:
        raise AssertionError(
            f"Invariantes fiscais categorizadas (proibido — Polars #24034): "
            f"{violations}. Veja plano Notion 358edc8b7d5d81cfb33ce023d4cee84f §C."
        )


# =====================================================================
# Reload helpers (útil em testes e refresh dinâmico)
# =====================================================================


def reload_fiscal_codes() -> None:
    """
    Limpa o cache de ``load_fiscal_codes``.

    Útil em testes que mudam o JSON em runtime ou após atualização do
    cadastro em produção (ex.: novo Ajuste SINIEF).
    """
    load_fiscal_codes.cache_clear()


__all__ = [
    "DEFAULT_CODES_PATH",
    "INVARIANT_BLOCKLIST",
    "COLUMN_TO_ENUM_KEY",
    "BOOLEAN_TRUE_VALUES_BY_COLUMN",
    "BOOLEAN_FALSE_VALUES_BY_COLUMN",
    "INVERTED_BOOLEAN_FIELDS",
    "DYNAMIC_CATEGORICAL_COLUMNS",
    "load_fiscal_codes",
    "build_enum_map",
    "build_categorical_map",
    "scan_parquet_typed",
    "cast_dataframe_typed",
    "validate_schema_post_cast",
    "get_invariant_dtypes",
    "assert_no_invariant_categorized",
    "reload_fiscal_codes",
]
