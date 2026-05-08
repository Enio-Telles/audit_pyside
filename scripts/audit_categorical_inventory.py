"""
audit_categorical_inventory.py
================================

Diagnóstico automatizado de candidatos a `pl.Categorical` / `pl.Enum` em
Parquets fiscais do `audit_pyside`.

Esta é a **PR 1** do plano de auditoria de campos categóricos. Apenas
mede e classifica — não altera schemas persistidos nem toca o pipeline
fiscal em `src/transformacao/`.

Uso
---
    # Modo dry-run com fixtures
    uv run python scripts/audit_categorical_inventory.py --dry-run

    # CNPJ específico
    uv run python scripts/audit_categorical_inventory.py \\
        --root /data/audit_pyside/parquets/04240370002877 \\
        --output docs/performance/categorical_candidates.md

    # Diretório raiz com vários CNPJs
    uv run python scripts/audit_categorical_inventory.py \\
        --root /data/audit_pyside/parquets \\
        --output docs/performance/categorical_candidates.md \\
        --json-out reports/categorical_candidates.json

Saídas
------
1. ``docs/performance/categorical_candidates.md`` — relatório legível.
2. ``reports/categorical_candidates.json`` — máquina-legível para PR 2.

Regras de classificação
-----------------------
Recomendações possíveis (em ordem de prioridade decrescente de bloqueio):

- ``PROIBIDO_CATEGORIZAR``: invariante fiscal, chave alta-cardinalidade
  ou texto livre. Defesa em profundidade — bloqueia mesmo se
  cardinalidade for baixa.
- ``MANTER_STRING``: cardinalidade > 20% ou ratio único/linhas alto.
- ``CATEGORICAL_EM_MEMORIA__ENUM``: domínio fechado oficial conhecido
  (CFOP, CST, UF, indicadores). Ganho de RSS ~80% + validação de
  domínio. Carrega de ``ref/fiscal_codes_2026.json``.
- ``CATEGORICAL_EM_MEMORIA__CATEGORICAL``: domínio dinâmico médio (NCM,
  CEST, unid). Ganho de RSS ~30-70% mas sem validação de domínio.
- ``MEDIR``: zona cinzenta 5%-20% de cardinalidade. Decidir após PR 2.

Riscos sinalizados via ``risk_flag``:
- ``polars_24034_high_cardinality``: cardinalidade alta + dtype string;
  categorizar inflaria 10× (Polars issue #24034).
- ``ncm_changes_yearly``: domínio muda anualmente; usar Categorical, não
  Enum.
- ``dirty_strings_normalize_first``: SPED tem strings sujas (espaços,
  case); normalizar antes de categorizar.
- ``date_field``: nunca categorizar (perde range queries).
- ``numeric_field``: nunca categorizar.

Referências
-----------
- Plano Notion: 358edc8b7d5d81cfb33ce023d4cee84f §C, §D, §K
- Polars User Guide: docs.pola.rs/user-guide/concepts/data-types/categoricals/
- Polars #24034 (high-cardinality regression)
- Polars #22586 (write_parquet use_pyarrow apaga Enum)
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

# polars é dependência já existente do audit_pyside
try:
    import polars as pl
except ImportError:  # pragma: no cover - infra
    print("ERRO: polars não está instalado. Rode `uv sync --group dev`.", file=sys.stderr)
    sys.exit(2)


# =====================================================================
# Configuração — listas declarativas (não importam dados reais)
# =====================================================================

#: Cardinalidade ratio (n_unique / n_rows) que define limites entre buckets.
THRESHOLD_FORTE = 0.01    # ratio <= 1% → CATEGORICAL forte
THRESHOLD_CANDIDATO = 0.05  # ratio <= 5% → CATEGORICAL candidato
THRESHOLD_MEDIR = 0.20    # ratio <= 20% → MEDIR
THRESHOLD_HARD_MAX = 65_536  # n_unique > 65k → categórico nunca compensa

#: Invariantes fiscais. Mesmo se cardinalidade for baixa, NUNCA categorizar.
#: Razão: issue Polars #24034 + invariantes do plano-mestre audit_pyside.
INVARIANTES_FISCAIS: frozenset[str] = frozenset({
    "id_agrupado",
    "id_agregado",
    "__qtd_decl_final_audit__",
    "q_conv",
    "q_conv_fisica",
})

#: Chaves de alta cardinalidade ou rastreabilidade. Não categorizar.
CHAVES_PROIBIDAS: frozenset[str] = frozenset({
    "chave_acesso", "Chv_nfe", "chv_nfe",
    "num_doc", "num_nfe", "Nro_doc", "nsu", "Nsu",
    "id_linha_origem", "codigo_fonte",
    "Cod_item", "cod_item",
    "Cod_barra", "cod_barra", "gtin", "GTIN", "ean",
    "cnpj_emitente", "cnpj_destinatario",
    "CNPJ_emit", "CNPJ_dest",
    "cnpj", "cpf",
})

#: Sufixos/prefixos que indicam descrição livre (texto longo).
PADROES_DESCRICAO: tuple[str, ...] = (
    "descricao", "Descr_item", "Descr_compl", "descricao_normalizada",
    "info_adicionais", "info_complementares", "obs", "observacoes",
    "x_just", "x_motivo", "x_nat",
)

#: Sufixos que indicam datas (nunca categorizar).
PADROES_DATA: tuple[str, ...] = (
    "Dt_", "dt_", "dh_", "dhemi", "dhsaient", "data",
)

#: Padrões para reconhecer campos numéricos disfarçados de string.
PADROES_NUMERICOS: tuple[str, ...] = (
    "vl_", "Vl_", "qtd", "qtde", "aliq", "aliquota",
    "base", "Base_", "imposto", "valor", "saldo",
)

#: Mapeamento de coluna → chave em fiscal_codes_2026.json.
#: Esta é a tabela canônica que casa nomes do schema com ENUM_MAP.
ENUM_FIELD_MAP: dict[str, str] = {
    # UF
    "uf": "uf",
    "UF": "uf",
    "emit_UF": "uf",
    "dest_UF": "uf",
    "co_uf_emit": "uf_codigo_ibge",
    "co_uf_dest": "uf_codigo_ibge",
    # CFOP — concatena entrada + saída + exterior
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
    # Tipo operação NFe
    "ide_tpNF": "tipo_operacao_nfe",
    "tipo_operacao": "indicador_operacao_sped_c170",
    "Tipo_operacao": "indicador_operacao_sped_c170",
    "Tipo_operacao_c170": "indicador_operacao_sped_c170",
    "ind_oper": "indicador_operacao_sped_c170",
    # Outros indicadores NFe
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
    # Regime
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

BOOLEAN_FIELDS: frozenset[str] = frozenset({
    "indFinal",
    "ide_indFinal",
    "indIntermed",
    "ide_indIntermed",
    "indEscala",
    "prod_indEscala",
    "indTot",
    "prod_indTot",
    "indPag",
    "detPag_indPag",
    "IND_DAD",
    "IND_PROF",
    "IND_SOC",
    "IND_ESC",
    "IND_ARM",
    "IND_REC",
    "IND_VEIC",
    "IND_NAV",
    "IND_AJ",
    "NAT_EXP",
})

INVERTED_BOOLEAN_FIELDS: frozenset[str] = frozenset({"IND_MOV", "ind_mov", "Ind_mov"})

#: Campos sabidos como dinâmicos (Categorical, NÃO Enum).
CATEGORICAL_FIELDS: frozenset[str] = frozenset({
    "ncm", "Ncm", "Ncm_c170", "prod_NCM", "ncm_padrao",
    "cest", "Cest", "Cest_c170", "prod_CEST", "cest_padrao",
    "unid", "Unid", "Unid_c170", "prod_uCom", "prod_uTrib", "unid_ref",
    "cnae", "cnae_principal", "cnae_secundario",
    "fonte", "fonte_xml", "regra_vinculo_xml",
    "status_xml", "fator_origem",
    "co_sefin_agr", "co_sefin_final",
    "it_in_st", "it_in_reducao_credito",
})

#: Campos com strings sujas no SPED — sinalizar para normalização prévia.
DIRTY_STRING_FIELDS: frozenset[str] = frozenset({
    "unid", "Unid", "prod_uCom", "prod_uTrib", "unid_ref",
})

#: Sentinela: ainda não calculado/aplicável.
SENTINEL_NA = -1.0


# =====================================================================
# Modelo de dados
# =====================================================================

Recommendation = Literal[
    "PROIBIDO_CATEGORIZAR",
    "MANTER_STRING",
    "CATEGORICAL_EM_MEMORIA__ENUM",
    "CATEGORICAL_EM_MEMORIA__CATEGORICAL",
    "BOOLEAN_EM_MEMORIA",
    "CATEGORICAL_PERSISTIDO_CANDIDATO",
    "MEDIR",
]

RiskFlag = Literal[
    "none",
    "polars_24034_high_cardinality",
    "ncm_changes_yearly",
    "dirty_strings_normalize_first",
    "date_field",
    "numeric_field",
    "fiscal_invariant",
    "high_cardinality_key",
    "free_text",
    "not_string",
]


@dataclass(slots=True)
class ColumnAudit:
    """Resultado de auditoria de uma coluna."""

    table: str
    column: str
    dtype_atual: str
    n_rows: int
    n_unique: int
    n_nulls: int
    null_ratio: float
    cardinality_ratio: float
    n_unique_per_million: float
    sample_values: list[str]
    recommendation: Recommendation
    dtype_polars_proposto: str
    risk_flag: RiskFlag
    enum_source: str | None = field(default=None)
    notes: str = field(default="")

    def to_dict(self) -> dict[str, Any]:
        d = {
            "table": self.table,
            "column": self.column,
            "dtype_atual": self.dtype_atual,
            "n_rows": self.n_rows,
            "n_unique": self.n_unique,
            "n_nulls": self.n_nulls,
            "null_ratio": round(self.null_ratio, 4),
            "cardinality_ratio": round(self.cardinality_ratio, 6),
            "n_unique_per_million": round(self.n_unique_per_million, 2),
            "sample_values": self.sample_values,
            "recommendation": self.recommendation,
            "dtype_polars_proposto": self.dtype_polars_proposto,
            "risk_flag": self.risk_flag,
        }
        if self.enum_source is not None:
            d["enum_source"] = self.enum_source
        if self.notes:
            d["notes"] = self.notes
        return d


# =====================================================================
# Lógica de classificação — funcionalmente pura, fácil de testar
# =====================================================================


def is_invariant(column: str) -> bool:
    """Coluna é invariante fiscal (`PROIBIDO_CATEGORIZAR` absoluto)."""
    return column in INVARIANTES_FISCAIS


def is_high_cardinality_key(column: str) -> bool:
    """Coluna é chave alta-cardinalidade (CNPJ, chave NF-e, etc)."""
    return column in CHAVES_PROIBIDAS


def is_free_text(column: str) -> bool:
    """Coluna é descrição livre (texto longo)."""
    return any(p in column for p in PADROES_DESCRICAO)


def is_date_field(column: str) -> bool:
    """Coluna parece ser data."""
    return any(column.startswith(p) for p in PADROES_DATA)


def is_likely_numeric(column: str) -> bool:
    """Coluna parece ser numérica (mesmo se está como string)."""
    return any(column.startswith(p) for p in PADROES_NUMERICOS)


def is_known_enum_field(column: str) -> bool:
    """Coluna é candidata a `pl.Enum` por mapeamento conhecido."""
    return column in ENUM_FIELD_MAP


def is_boolean_field(column: str) -> bool:
    """Coluna e boolean classica 0/1 ou S/N."""
    return column in BOOLEAN_FIELDS


def is_known_categorical_field(column: str) -> bool:
    """Coluna é candidata a `pl.Categorical` por mapeamento conhecido."""
    return column in CATEGORICAL_FIELDS


def is_dirty_string_field(column: str) -> bool:
    """Coluna tem strings sujas conhecidas (SPED) e exige normalização."""
    return column in DIRTY_STRING_FIELDS


def classify_column(
    table: str,
    column: str,
    dtype_atual: str,
    n_rows: int,
    n_unique: int,
    n_nulls: int,
    sample_values: list[str],
) -> ColumnAudit:
    """
    Classifica uma coluna com base em (nome, dtype, cardinalidade).

    Esta função é **determinística** e **pura** (sem I/O) — base do
    test_audit_categorical_candidates.py.
    """
    cardinality_ratio = n_unique / n_rows if n_rows > 0 else 0.0
    null_ratio = n_nulls / n_rows if n_rows > 0 else 0.0
    n_unique_per_million = (n_unique * 1_000_000) / max(n_rows, 1)

    # ---------- 1. Defesa em profundidade: invariantes ----------
    if is_invariant(column):
        return ColumnAudit(
            table=table, column=column, dtype_atual=dtype_atual,
            n_rows=n_rows, n_unique=n_unique, n_nulls=n_nulls,
            null_ratio=null_ratio, cardinality_ratio=cardinality_ratio,
            n_unique_per_million=n_unique_per_million,
            sample_values=sample_values,
            recommendation="PROIBIDO_CATEGORIZAR",
            dtype_polars_proposto="pl.String OR original numeric",
            risk_flag="fiscal_invariant",
            notes="Invariante fiscal — defesa em profundidade vs Polars #24034",
        )

    # ---------- 2. Chaves de alta cardinalidade ----------
    if is_high_cardinality_key(column):
        return ColumnAudit(
            table=table, column=column, dtype_atual=dtype_atual,
            n_rows=n_rows, n_unique=n_unique, n_nulls=n_nulls,
            null_ratio=null_ratio, cardinality_ratio=cardinality_ratio,
            n_unique_per_million=n_unique_per_million,
            sample_values=sample_values,
            recommendation="PROIBIDO_CATEGORIZAR",
            dtype_polars_proposto="pl.String",
            risk_flag="high_cardinality_key",
            notes="Chave alta-cardinalidade ou rastreabilidade",
        )

    # ---------- 3. Texto livre ----------
    if is_free_text(column):
        return ColumnAudit(
            table=table, column=column, dtype_atual=dtype_atual,
            n_rows=n_rows, n_unique=n_unique, n_nulls=n_nulls,
            null_ratio=null_ratio, cardinality_ratio=cardinality_ratio,
            n_unique_per_million=n_unique_per_million,
            sample_values=sample_values,
            recommendation="MANTER_STRING",
            dtype_polars_proposto="pl.String",
            risk_flag="free_text",
            notes="Descrição livre / texto longo",
        )

    # ---------- 4. Datas ----------
    if is_date_field(column):
        return ColumnAudit(
            table=table, column=column, dtype_atual=dtype_atual,
            n_rows=n_rows, n_unique=n_unique, n_nulls=n_nulls,
            null_ratio=null_ratio, cardinality_ratio=cardinality_ratio,
            n_unique_per_million=n_unique_per_million,
            sample_values=sample_values,
            recommendation="MANTER_STRING",
            dtype_polars_proposto="pl.Date OR pl.Datetime",
            risk_flag="date_field",
            notes="Data — usar pl.Date/pl.Datetime, nunca categórico",
        )

    # ---------- 5. Numérico disfarçado ----------
    if is_likely_numeric(column):
        return ColumnAudit(
            table=table, column=column, dtype_atual=dtype_atual,
            n_rows=n_rows, n_unique=n_unique, n_nulls=n_nulls,
            null_ratio=null_ratio, cardinality_ratio=cardinality_ratio,
            n_unique_per_million=n_unique_per_million,
            sample_values=sample_values,
            recommendation="MANTER_STRING",
            dtype_polars_proposto="numeric (pl.Int64 / pl.Float64 / pl.Decimal)",
            risk_flag="numeric_field",
            notes="Numérico — não categorizar; converter para tipo numérico apropriado",
        )

    # ---------- 6. Cardinalidade absoluta proibitiva ----------
    if n_unique > THRESHOLD_HARD_MAX:
        risk: RiskFlag = "polars_24034_high_cardinality"
        return ColumnAudit(
            table=table, column=column, dtype_atual=dtype_atual,
            n_rows=n_rows, n_unique=n_unique, n_nulls=n_nulls,
            null_ratio=null_ratio, cardinality_ratio=cardinality_ratio,
            n_unique_per_million=n_unique_per_million,
            sample_values=sample_values,
            recommendation="MANTER_STRING",
            dtype_polars_proposto="pl.String",
            risk_flag=risk,
            notes=f"n_unique={n_unique} > {THRESHOLD_HARD_MAX}; risco Polars #24034",
        )

    # ---------- 7. Boolean classico ----------
    if is_boolean_field(column):
        return ColumnAudit(
            table=table, column=column, dtype_atual=dtype_atual,
            n_rows=n_rows, n_unique=n_unique, n_nulls=n_nulls,
            null_ratio=null_ratio, cardinality_ratio=cardinality_ratio,
            n_unique_per_million=n_unique_per_million,
            sample_values=sample_values,
            recommendation="BOOLEAN_EM_MEMORIA",
            dtype_polars_proposto="pl.Boolean",
            risk_flag="none",
            notes="Boolean classico 0/1 ou S/N; IND_MOV invertido fica fora desta regra",
        )

    # ---------- 7. Conhecido como ENUM ----------
    if is_known_enum_field(column):
        enum_source = ENUM_FIELD_MAP[column]
        return ColumnAudit(
            table=table, column=column, dtype_atual=dtype_atual,
            n_rows=n_rows, n_unique=n_unique, n_nulls=n_nulls,
            null_ratio=null_ratio, cardinality_ratio=cardinality_ratio,
            n_unique_per_million=n_unique_per_million,
            sample_values=sample_values,
            recommendation="CATEGORICAL_EM_MEMORIA__ENUM",
            dtype_polars_proposto=f"pl.Enum(load('{enum_source}'))",
            risk_flag="none",
            enum_source=enum_source,
            notes="Domínio fechado oficial — ENUM com validação",
        )

    # ---------- 8. Conhecido como Categorical dinâmico ----------
    if is_known_categorical_field(column):
        risk_cat: RiskFlag = "none"
        notes = "Domínio dinâmico — Categorical sem validação"
        if column.lower() in ("ncm", "cest") or "ncm" in column.lower() or "cest" in column.lower():
            risk_cat = "ncm_changes_yearly"
            notes = "NCM/CEST mudam anualmente — Categorical (não Enum)"
        elif is_dirty_string_field(column):
            risk_cat = "dirty_strings_normalize_first"
            notes = "Strings sujas no SPED — normalizar antes de categorizar"
        return ColumnAudit(
            table=table, column=column, dtype_atual=dtype_atual,
            n_rows=n_rows, n_unique=n_unique, n_nulls=n_nulls,
            null_ratio=null_ratio, cardinality_ratio=cardinality_ratio,
            n_unique_per_million=n_unique_per_million,
            sample_values=sample_values,
            recommendation="CATEGORICAL_EM_MEMORIA__CATEGORICAL",
            dtype_polars_proposto="pl.Categorical()",
            risk_flag=risk_cat,
            notes=notes,
        )

    # ---------- 9. Não-string ----------
    if not _is_string_dtype(dtype_atual):
        return ColumnAudit(
            table=table, column=column, dtype_atual=dtype_atual,
            n_rows=n_rows, n_unique=n_unique, n_nulls=n_nulls,
            null_ratio=null_ratio, cardinality_ratio=cardinality_ratio,
            n_unique_per_million=n_unique_per_million,
            sample_values=sample_values,
            recommendation="MANTER_STRING",
            dtype_polars_proposto=dtype_atual,
            risk_flag="not_string",
            notes="Não é string — fora de escopo desta auditoria",
        )

    # ---------- 10. Heurística por cardinalidade ----------
    if cardinality_ratio <= THRESHOLD_FORTE:
        rec: Recommendation = "CATEGORICAL_EM_MEMORIA__CATEGORICAL"
        proposto = "pl.Categorical()"
        notes = f"Cardinalidade forte ({cardinality_ratio:.4%}) — Categorical em memória"
    elif cardinality_ratio <= THRESHOLD_CANDIDATO:
        rec = "CATEGORICAL_EM_MEMORIA__CATEGORICAL"
        proposto = "pl.Categorical()"
        notes = f"Cardinalidade candidata ({cardinality_ratio:.4%}) — Categorical em memória"
    elif cardinality_ratio <= THRESHOLD_MEDIR:
        rec = "MEDIR"
        proposto = "pl.String (medir benefício antes)"
        notes = f"Cardinalidade na zona cinzenta ({cardinality_ratio:.4%}) — decidir após PR 2"
    else:
        rec = "MANTER_STRING"
        proposto = "pl.String"
        notes = f"Cardinalidade alta ({cardinality_ratio:.4%}) — manter string"

    return ColumnAudit(
        table=table, column=column, dtype_atual=dtype_atual,
        n_rows=n_rows, n_unique=n_unique, n_nulls=n_nulls,
        null_ratio=null_ratio, cardinality_ratio=cardinality_ratio,
        n_unique_per_million=n_unique_per_million,
        sample_values=sample_values,
        recommendation=rec,
        dtype_polars_proposto=proposto,
        risk_flag="none",
        notes=notes,
    )


def _is_string_dtype(dtype_str: str) -> bool:
    """Verifica se dtype Polars repr é string."""
    return dtype_str in {"String", "Utf8", "Categorical", "Enum"} or "String" in dtype_str


# =====================================================================
# I/O — leitura de Parquets, geração de relatórios
# =====================================================================


def discover_parquets(root: Path) -> list[Path]:
    """Encontra arquivos `.parquet` recursivamente, ordenados deterministicamente."""
    if not root.exists():
        raise FileNotFoundError(f"Diretório raiz não existe: {root}")
    return sorted(root.rglob("*.parquet"))


def infer_table_name(parquet_path: Path) -> str:
    """Heurística: nome da tabela = stem do arquivo OU nome do diretório pai."""
    stem = parquet_path.stem
    parent = parquet_path.parent.name
    # Padrão audit_pyside: <cnpj>/<tabela>.parquet  OU  <cnpj>/<tabela>/parte.parquet
    if stem in {"part-0", "part-1", "data"}:
        return parent
    return stem


def audit_parquet_file(
    parquet_path: Path,
    sample_size: int = 5,
    logger: logging.Logger | None = None,
) -> Iterator[ColumnAudit]:
    """
    Audita um Parquet: para cada coluna, gera ColumnAudit.

    Usa `scan_parquet` lazy + agregação única para minimizar memória.
    """
    log = logger or logging.getLogger(__name__)
    table = infer_table_name(parquet_path)

    try:
        lf = pl.scan_parquet(parquet_path)
        schema = lf.collect_schema()
    except Exception as exc:
        log.warning("Falha ao abrir %s: %s", parquet_path, exc)
        return

    n_rows_df = lf.select(pl.len().alias("n")).collect()
    n_rows = int(n_rows_df["n"][0]) if n_rows_df.height > 0 else 0
    if n_rows == 0:
        log.info("%s vazio, pulando", parquet_path)
        return

    for column, dtype in schema.items():
        dtype_str = str(dtype)
        try:
            stats = lf.select([
                pl.col(column).n_unique().alias("n_unique"),
                pl.col(column).null_count().alias("n_nulls"),
            ]).collect()
            n_unique = int(stats["n_unique"][0])
            n_nulls = int(stats["n_nulls"][0])

            # Sample apenas para colunas string (evita custo em colunas numéricas grandes)
            sample_values: list[str] = []
            if _is_string_dtype(dtype_str):
                sample_df = (
                    lf.select(pl.col(column))
                    .drop_nulls()
                    .unique()
                    .head(sample_size)
                    .collect()
                )
                sample_values = [
                    str(v) for v in sample_df.get_column(column).to_list()
                ]
        except Exception as exc:
            log.warning("Falha em %s/%s: %s", table, column, exc)
            continue

        yield classify_column(
            table=table,
            column=column,
            dtype_atual=dtype_str,
            n_rows=n_rows,
            n_unique=n_unique,
            n_nulls=n_nulls,
            sample_values=sample_values,
        )


# =====================================================================
# Relatórios
# =====================================================================


def render_markdown_report(audits: list[ColumnAudit]) -> str:
    """Gera relatório Markdown agrupado por tabela."""
    if not audits:
        return "# Categorical Candidates\n\nNenhum Parquet encontrado.\n"

    timestamp = datetime.now(tz=UTC).isoformat(timespec="seconds")
    lines: list[str] = [
        "# Categorical Candidates — audit_pyside",
        "",
        f"_Gerado em {timestamp} por `scripts/audit_categorical_inventory.py`._",
        "",
        "## Resumo executivo",
        "",
    ]

    by_rec: dict[str, int] = {}
    for a in audits:
        by_rec[a.recommendation] = by_rec.get(a.recommendation, 0) + 1

    lines.append("| Recomendação | Colunas |")
    lines.append("|---|---|")
    for rec in sorted(by_rec):
        lines.append(f"| `{rec}` | {by_rec[rec]} |")
    lines.append("")

    # Risk flags
    risk_count: dict[str, int] = {}
    for a in audits:
        if a.risk_flag != "none":
            risk_count[a.risk_flag] = risk_count.get(a.risk_flag, 0) + 1
    if risk_count:
        lines.append("### Risk flags presentes")
        lines.append("")
        lines.append("| Risk flag | Colunas |")
        lines.append("|---|---|")
        for risk in sorted(risk_count):
            lines.append(f"| `{risk}` | {risk_count[risk]} |")
        lines.append("")

    # Detalhe por tabela
    lines.append("## Detalhe por tabela")
    lines.append("")

    by_table: dict[str, list[ColumnAudit]] = {}
    for a in audits:
        by_table.setdefault(a.table, []).append(a)

    for table in sorted(by_table):
        lines.append(f"### `{table}`")
        lines.append("")
        lines.append(
            "| Coluna | Dtype atual | n_rows | n_unique | "
            "Card. % | Recomendação | Dtype proposto | Risk | Notas |"
        )
        lines.append("|---|---|---:|---:|---:|---|---|---|---|")
        for a in sorted(by_table[table], key=lambda x: x.column):
            lines.append(
                f"| `{a.column}` | `{a.dtype_atual}` | {a.n_rows:,} | "
                f"{a.n_unique:,} | {a.cardinality_ratio:.2%} | "
                f"`{a.recommendation}` | `{a.dtype_polars_proposto}` | "
                f"`{a.risk_flag}` | {a.notes} |"
            )
        lines.append("")

    return "\n".join(lines) + "\n"


def render_json_report(audits: list[ColumnAudit]) -> str:
    """Gera relatório JSON estruturado para PR 2."""
    payload = {
        "generated_at": datetime.now(tz=UTC).isoformat(timespec="seconds"),
        "n_columns": len(audits),
        "audits": [a.to_dict() for a in audits],
    }
    return json.dumps(payload, indent=2, ensure_ascii=False)


# =====================================================================
# CLI
# =====================================================================


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--root",
        type=Path,
        help="Diretório raiz contendo Parquets (CNPJ ou pasta multi-CNPJ).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("docs/performance/categorical_candidates.md"),
        help="Caminho do relatório Markdown.",
    )
    parser.add_argument(
        "--json-out",
        type=Path,
        default=None,
        help="Caminho opcional do relatório JSON (default: ao lado do --output).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Modo dry-run: gera relatório sintético sem ler Parquets reais.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=5,
        help="Número de valores de amostra por coluna (default: 5).",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Logs detalhados.",
    )
    return parser.parse_args(argv)


def setup_logging(verbose: bool) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return logging.getLogger("audit_categorical")


def run_dry_run() -> list[ColumnAudit]:
    """Gera audits sintéticos para validar o relatório sem dados reais."""
    fixtures = [
        # tabela, col, dtype, n_rows, n_unique, n_nulls, sample
        ("c170_xml", "cfop", "String", 1_000_000, 180, 0, ["5102", "5101", "1102"]),
        ("c170_xml", "cst_icms", "String", 1_000_000, 28, 100, ["000", "060", "010"]),
        ("c170_xml", "ncm", "String", 1_000_000, 8_500, 50, ["12345678", "98765432"]),
        ("c170_xml", "id_agrupado", "String", 1_000_000, 250_000, 0, ["abc", "def"]),
        ("c170_xml", "q_conv", "Float64", 1_000_000, 950_000, 0, []),
        ("tb_documentos", "uf", "String", 500_000, 27, 0, ["RO", "SP", "MG"]),
        ("tb_documentos", "Chv_nfe", "String", 500_000, 500_000, 0, ["44...", "44..."]),
        ("tb_documentos", "Descr_item", "String", 500_000, 480_000, 0, ["...", "..."]),
        ("tb_documentos", "Dt_doc", "Date", 500_000, 365, 0, []),
        ("produtos_final", "unid", "String", 200_000, 75, 1_000, ["UN", "PC", "CX"]),
        ("produtos_final", "cest", "String", 200_000, 1_200, 50_000, ["28.038.00"]),
        ("calculos_mensais", "ano", "Int16", 60, 5, 0, []),
        ("calculos_mensais", "mes", "Int8", 60, 12, 0, []),
    ]
    audits: list[ColumnAudit] = []
    for table, col, dtype, n, u, nulls, sample in fixtures:
        audits.append(
            classify_column(table, col, dtype, n, u, nulls, sample)
        )
    return audits


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    log = setup_logging(args.verbose)

    if args.dry_run:
        log.info("Modo dry-run — gerando relatório sintético")
        audits = run_dry_run()
    else:
        if args.root is None:
            log.error("--root é obrigatório (ou use --dry-run)")
            return 2
        parquets = discover_parquets(args.root)
        log.info("Encontrados %d arquivos Parquet em %s", len(parquets), args.root)
        if not parquets:
            log.warning("Nenhum Parquet encontrado")
            return 0
        audits: list[ColumnAudit] = []
        for p in parquets:
            log.info("Auditando %s", p)
            audits.extend(audit_parquet_file(p, sample_size=args.sample_size, logger=log))

    md = render_markdown_report(audits)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(md, encoding="utf-8")
    log.info("Relatório Markdown gravado em %s (%d auditorias)", args.output, len(audits))

    json_path = args.json_out or args.output.with_suffix(".json")
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(render_json_report(audits), encoding="utf-8")
    log.info("Relatório JSON gravado em %s", json_path)

    # Resumo no stdout
    by_rec: dict[str, int] = {}
    for a in audits:
        by_rec[a.recommendation] = by_rec.get(a.recommendation, 0) + 1
    print("\n=== Resumo ===")
    for rec in sorted(by_rec):
        print(f"  {rec:50s} {by_rec[rec]:4d}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
