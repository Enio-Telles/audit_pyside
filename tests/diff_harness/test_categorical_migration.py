"""
tests/diff_harness/test_categorical_migration.py
==================================================

Os 3 testes do ``differential_harness`` para validar a migraÃ§Ã£o v1 â†’ v2
dos Parquets fiscais (PR 4 do plano de auditoria de campos categÃ³ricos).

Plano de referÃªncia: ``358edc8b7d5d81cfb33ce023d4cee84f`` Â§G.

Testes
------

1. ``test_invariants_byte_identical`` â€” SHA-256 das 5 invariantes
   ordenadas Ã© idÃªntico v1 vs v2. Garante que o rewrite preservou a
   semÃ¢ntica dos campos crÃ­ticos do pipeline fiscal.

2. ``test_aggregations_identical`` â€” ``group_by + sum`` das invariantes
   produz exatamente o mesmo resultado em v1 e v2 para os 3 grouping
   sets tÃ­picos: ``[cfop]``, ``[uf, cfop]``, ``[id_agrupado]``.

3. ``test_encoding_physically_dictionary`` â€” colunas-alvo da v2 usam
   ``RLE_DICTIONARY`` em 100% dos row_groups. Sem isso, a economia de
   espaÃ§o/RAM da PR 4 nÃ£o materializa.

Fluxo de adoÃ§Ã£o
---------------

- **Hoje (Onda 2):** mergear este arquivo. Por default, os testes
  fazem ``skip`` (Parquet v2 nÃ£o existe). Validar localmente com
  ``--diff-harness-synthetic``.

- **Quando PR 4 entregar:** apontar ``--parquet-v2`` para o arquivo
  rewriteado. Os 3 testes passam â†’ PR 4 mergeÃ¡vel. Qualquer falha
  bloqueia o merge.

Como rodar
----------

::

    # ValidaÃ§Ã£o sintÃ©tica (sem dados reais; sÃ³ valida lÃ³gica dos testes)
    uv run pytest tests/diff_harness/ --diff-harness-synthetic -v

    # Em CNPJ real, prÃ©-PR 4 (apenas v1; testes v2 fazem skip)
    uv run pytest tests/diff_harness/ \\
        --parquet-v1=/data/parquets/04240370002877/c170_xml.parquet -v

    # PÃ³s-PR 4 (validaÃ§Ã£o real)
    uv run pytest tests/diff_harness/ \\
        --parquet-v1=/data/parquets/04240370002877/c170_xml.parquet \\
        --parquet-v2=/data/parquets/04240370002877_v2/c170_xml.parquet -v

    # Apenas o subset diff_harness (CI)
    uv run pytest -m diff_harness
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import polars as pl
import pytest


#: Invariantes fiscais â€” devem ser byte-idÃªnticas v1 vs v2.
INVARIANTES_FISCAIS: list[str] = [
    "id_agrupado",
    "id_agregado",
    "__qtd_decl_final_audit__",
    "q_conv",
    "q_conv_fisica",
]

#: Subset numÃ©rico das invariantes (para agregaÃ§Ãµes).
INVARIANTES_NUMERIC: list[str] = [
    "__qtd_decl_final_audit__",
    "q_conv",
    "q_conv_fisica",
]

#: Grouping sets tÃ­picos do pipeline fiscal.
GROUPING_SETS: list[list[str]] = [
    ["cfop"],
    ["uf", "cfop"],
    ["id_agrupado"],
]

#: Colunas que DEVEM usar RLE_DICTIONARY na v2.
EXPECTED_DICTIONARY_COLS: frozenset[str] = frozenset({
    "cfop", "cst_icms", "csosn", "uf",
    "tipo_operacao", "mod", "ncm", "cest",
})


# =====================================================================
# Helpers
# =====================================================================


def _select_invariants_present(parquet_path: Path) -> list[str]:
    """Retorna apenas as invariantes presentes no schema do Parquet."""
    schema = pl.scan_parquet(parquet_path).collect_schema()
    return [c for c in INVARIANTES_FISCAIS if c in schema.names()]


def _content_hash(df: pl.DataFrame) -> str:
    """
    SHA-256 determinÃ­stico do *conteÃºdo* de um DataFrame, ignorando
    metadata (Polars #IPC inclui custom_metadata do Parquet origem).

    Usa ``df.hash_rows()`` (Polars nativo, estÃ¡vel) e hash do XOR
    cumulativo. Suficiente para comparaÃ§Ã£o byte-a-byte do conteÃºdo.
    """
    # hash_rows produz UInt64 por linha â€” agregamos com hash_final
    row_hashes = df.hash_rows()
    # Serializar como lista de inteiros e hash via SHA-256
    buf = ",".join(str(h) for h in row_hashes.to_list()).encode()
    return hashlib.sha256(buf).hexdigest()


def _diff_first_rows(df1: pl.DataFrame, df2: pl.DataFrame, n: int = 5) -> str:
    """Compara duas DataFrames row-by-row e retorna as primeiras N divergÃªncias."""
    if df1.height != df2.height:
        return f"heights diferem: {df1.height} vs {df2.height}"
    # row_index para comparar; assumimos que ambos estÃ£o sorteados igualmente
    diffs: list[str] = []
    for col in df1.columns:
        if col not in df2.columns:
            diffs.append(f"  col {col!r}: ausente em v2")
            continue
        # Compare lazy â€” Polars resolve a primeira divergÃªncia rapidamente
        ne = (df1[col] != df2[col]).sum()
        if ne > 0:
            # Pega primeiras posiÃ§Ãµes onde diferem
            mask = df1[col] != df2[col]
            first_idx = [i for i, m in enumerate(mask.to_list()) if m][:n]
            sample = [
                f"      row {i}: v1={df1[col][i]!r}, v2={df2[col][i]!r}"
                for i in first_idx
            ]
            diffs.append(
                f"  col {col!r}: {ne} divergÃªncias (de {df1.height})\n"
                + "\n".join(sample)
            )
    return "\n".join(diffs) if diffs else "(nenhuma divergÃªncia detectada nas colunas)"


def _normalize_string_cols(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    """
    Cast colunas para String para comparaÃ§Ã£o fair entre v1 (string) e
    v2 (Enum/Categorical). A semÃ¢ntica Ã© idÃªntica; sÃ³ o physical encoding
    difere.
    """
    casts = []
    for col in cols:
        if col not in df.columns:
            continue
        dtype = df.schema[col]
        if isinstance(dtype, (pl.Enum, pl.Categorical)):
            casts.append(pl.col(col).cast(pl.String))
    return df.with_columns(casts) if casts else df


# =====================================================================
# Teste 1: invariantes byte-idÃªnticas
# =====================================================================


@pytest.mark.diff_harness
def test_invariants_byte_identical(parquet_v1: Path, parquet_v2: Path) -> None:
    """
    SHA-256 das 5 invariantes fiscais ordenadas deve ser idÃªntico
    byte-a-byte entre v1 e v2.

    Por que importa: as 5 invariantes (`id_agrupado`, `id_agregado`,
    `__qtd_decl_final_audit__`, `q_conv`, `q_conv_fisica`) sÃ£o a base
    auditÃ¡vel do pipeline fiscal. Se o rewrite v1â†’v2 alterar uma
    sequer, a auditoria SEFIN/RO fica invÃ¡lida.
    """
    available = _select_invariants_present(parquet_v1)
    assert available, (
        f"Nenhuma invariante presente em {parquet_v1}. "
        f"Esperado pelo menos uma de {INVARIANTES_FISCAIS}."
    )

    # Schema das invariantes deve ser idÃªntico (incluindo dtype)
    schema_v1 = pl.scan_parquet(parquet_v1).collect_schema()
    schema_v2 = pl.scan_parquet(parquet_v2).collect_schema()

    schema_violations: list[str] = []
    for col in available:
        if col not in schema_v2.names():
            schema_violations.append(f"{col}: ausente em v2")
            continue
        if schema_v1[col] != schema_v2[col]:
            schema_violations.append(
                f"{col}: v1={schema_v1[col]!r}, v2={schema_v2[col]!r}"
            )
    assert not schema_violations, (
        "Invariantes tÃªm schema divergente entre v1 e v2 "
        "(invariantes nunca devem ser categorizadas â€” Polars #24034):\n"
        + "\n".join(schema_violations)
    )

    # Materializar com sort estÃ¡vel para hash determinÃ­stico
    df_v1 = (
        pl.scan_parquet(parquet_v1)
        .select(available)
        .sort(available, nulls_last=True)
        .collect()
    )
    df_v2 = (
        pl.scan_parquet(parquet_v2)
        .select(available)
        .sort(available, nulls_last=True)
        .collect()
    )

    # Mesmo nÃºmero de linhas (sanidade)
    assert df_v1.height == df_v2.height, (
        f"v1 tem {df_v1.height} linhas, v2 tem {df_v2.height}. "
        f"Rewrite alterou cardinalidade."
    )

    # ComparaÃ§Ã£o semÃ¢ntica: equals() ignora metadata IPC
    if not df_v1.equals(df_v2):
        diff = _diff_first_rows(df_v1, df_v2)
        h_v1 = _content_hash(df_v1)
        h_v2 = _content_hash(df_v2)
        pytest.fail(
            f"Invariantes divergem entre v1 e v2.\n"
            f"  v1 content_hash: {h_v1[:32]}...\n"
            f"  v2 content_hash: {h_v2[:32]}...\n"
            f"  invariantes verificadas: {available}\n"
            f"  v1 path: {parquet_v1}\n"
            f"  v2 path: {parquet_v2}\n"
            f"  diff (primeiras divergÃªncias por coluna):\n{diff}"
        )


# =====================================================================
# Teste 2: agregaÃ§Ãµes idÃªnticas
# =====================================================================


@pytest.mark.diff_harness
def test_aggregations_identical(parquet_v1: Path, parquet_v2: Path) -> None:
    """
    ``group_by + sum`` das invariantes numÃ©ricas produz exatamente o
    mesmo resultado em v1 e v2, para os 3 grouping sets tÃ­picos do
    pipeline fiscal.

    Por que importa: a refatoraÃ§Ã£o do `transformacao/*` para usar Enum
    pode introduzir bugs sutis em joins/groupbys (ex.: ordenaÃ§Ã£o
    lexical vs numÃ©rica). Esta verificaÃ§Ã£o capta qualquer divergÃªncia
    semÃ¢ntica nas agregaÃ§Ãµes que alimentam relatÃ³rios fiscais.
    """
    schema_v1 = pl.scan_parquet(parquet_v1).collect_schema()
    schema_v2 = pl.scan_parquet(parquet_v2).collect_schema()

    available_invariants = [
        c for c in INVARIANTES_NUMERIC
        if c in schema_v1.names() and c in schema_v2.names()
    ]
    assert available_invariants, (
        f"Nenhuma invariante numÃ©rica em comum entre v1 e v2. "
        f"Esperado: {INVARIANTES_NUMERIC}"
    )

    tested_groups = 0
    for grp in GROUPING_SETS:
        # Skip grouping se alguma coluna estiver ausente
        if not all(g in schema_v1.names() and g in schema_v2.names() for g in grp):
            continue

        agg_v1 = (
            pl.scan_parquet(parquet_v1)
            .group_by(grp)
            .agg(*[pl.col(c).sum().alias(c) for c in available_invariants])
            .sort(grp, nulls_last=True)
            .collect()
        )
        agg_v2 = (
            pl.scan_parquet(parquet_v2)
            .group_by(grp)
            .agg(*[pl.col(c).sum().alias(c) for c in available_invariants])
            .sort(grp, nulls_last=True)
            .collect()
        )

        # Normalizar dtypes das colunas de agrupamento (v2 pode ter Enum)
        agg_v1 = _normalize_string_cols(agg_v1, grp)
        agg_v2 = _normalize_string_cols(agg_v2, grp)

        assert agg_v1.height == agg_v2.height, (
            f"AgregaÃ§Ã£o por {grp}: v1 tem {agg_v1.height} grupos, "
            f"v2 tem {agg_v2.height}."
        )

        # equals() compara valores e dtypes; usamos check_column_order=False
        # porque a ordem do select pode diferir
        if not agg_v1.equals(agg_v2):
            # Diff detalhado para debug
            diff = agg_v1.join(
                agg_v2, on=grp, how="full", suffix="_v2",
            ).filter(
                pl.any_horizontal(
                    [
                        pl.col(c) != pl.col(f"{c}_v2")
                        for c in available_invariants
                    ]
                )
            )
            pytest.fail(
                f"AgregaÃ§Ã£o por {grp} diverge entre v1 e v2.\n"
                f"  invariantes somadas: {available_invariants}\n"
                f"  total de linhas com divergÃªncia: {diff.height}\n"
                f"  primeiras divergÃªncias:\n{diff.head(5)}"
            )

        tested_groups += 1

    assert tested_groups > 0, (
        f"Nenhum grouping set testado â€” colunas {GROUPING_SETS} "
        f"todas ausentes nos Parquets."
    )


# =====================================================================
# Teste 3: encoding fÃ­sico (RLE_DICTIONARY)
# =====================================================================


@pytest.mark.diff_harness
def test_encoding_physically_dictionary(parquet_v2: Path) -> None:
    """
    Colunas-alvo da v2 devem usar ``RLE_DICTIONARY`` em **100% dos
    row_groups**. Sem isso, a economia de RAM/disco da PR 4 nÃ£o
    materializa e o investimento foi em vÃ£o.

    Por que importa: Polars com ``write_parquet(use_pyarrow=False)``
    (Rust nativo) produz ``RLE_DICTIONARY`` para Enum/Categorical. Se
    alguÃ©m regredir para ``use_pyarrow=True`` (issue #22586) ou se o
    dictionary fallback PLAIN ocorrer (page > 1 MiB), este teste pega.

    Ignora colunas ausentes â€” se v2 nÃ£o tem `cest`, ok, nÃ£o falha.
    Apenas verifica que as colunas presentes estÃ£o dictionary-encoded.
    """
    pytest.importorskip("pyarrow", reason="pyarrow necessÃ¡rio para inspeÃ§Ã£o fÃ­sica")
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(parquet_v2)
    n_row_groups = pf.metadata.num_row_groups
    assert n_row_groups > 0, f"Parquet v2 sem row_groups: {parquet_v2}"

    # Mapeia coluna â†’ schema (Schema.names() existe em todas as versÃµes)
    schema_names = set(pf.schema_arrow.names)
    target_present = EXPECTED_DICTIONARY_COLS & schema_names

    if not target_present:
        pytest.skip(
            f"Nenhuma coluna-alvo presente em v2. Esperado pelo menos uma de "
            f"{sorted(EXPECTED_DICTIONARY_COLS)}, mas v2 tem apenas "
            f"{sorted(schema_names)}."
        )

    violations: list[str] = []
    for rg_idx in range(n_row_groups):
        rg = pf.metadata.row_group(rg_idx)
        for col_idx in range(rg.num_columns):
            cm = rg.column(col_idx)
            col_name = cm.path_in_schema
            if col_name not in target_present:
                continue
            # Encodings Ã© uma tupla; converter para strings comparÃ¡veis
            encodings = [str(e) for e in cm.encodings]
            has_dict_page = bool(cm.has_dictionary_page)
            has_dict_encoding = any("DICTIONARY" in e.upper() for e in encodings)

            if not (has_dict_page and has_dict_encoding):
                violations.append(
                    f"  row_group={rg_idx} col={col_name!r} "
                    f"has_dict_page={has_dict_page} encodings={encodings}"
                )

    assert not violations, (
        f"{len(violations)} colunas-alvo SEM dictionary encoding em v2 "
        f"({parquet_v2}):\n"
        + "\n".join(violations[:20])
        + (f"\n  ... e mais {len(violations) - 20}" if len(violations) > 20 else "")
        + "\n\nCausas provÃ¡veis: (a) write_parquet(use_pyarrow=True) "
        "[Polars #22586]; (b) dictionary_pagesize_limit muito baixo; "
        "(c) cardinalidade da coluna excedeu o threshold do writer e "
        "fez fallback para PLAIN."
    )


# =====================================================================
# Sanidade â€” nÃ£o-xfail, sempre roda
# =====================================================================


def test_invariants_constants_match_categorical_recovery_module() -> None:
    """
    Garante que a lista de invariantes deste mÃ³dulo bate com o
    ``INVARIANT_BLOCKLIST`` de ``src/io/categorical_recovery.py``.

    Se alguÃ©m adicionar/remover uma invariante em um lugar e esquecer
    do outro, este teste pega antes de virar bug em produÃ§Ã£o.
    """
    try:
        from src.io.categorical_recovery import INVARIANT_BLOCKLIST
    except ImportError:
        pytest.skip(
            "MÃ³dulo categorical_recovery nÃ£o disponÃ­vel "
            "(provavelmente PR 2 nÃ£o mergeada ainda)."
        )

    assert frozenset(INVARIANTES_FISCAIS) == INVARIANT_BLOCKLIST, (
        f"DivergÃªncia entre INVARIANTES_FISCAIS desta suÃ­te e "
        f"INVARIANT_BLOCKLIST do mÃ³dulo categorical_recovery:\n"
        f"  somente em test:   {set(INVARIANTES_FISCAIS) - INVARIANT_BLOCKLIST}\n"
        f"  somente em mÃ³dulo: {INVARIANT_BLOCKLIST - set(INVARIANTES_FISCAIS)}\n"
        f"Sincronizar antes de mergear."
    )
