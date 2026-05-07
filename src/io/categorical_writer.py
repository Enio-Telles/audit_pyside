"""
src/io/categorical_writer.py
=============================

Entrypoint offline de reescrita fisica side-by-side v1 -> v2 de Parquets
fiscais do `audit_pyside`.

Este modulo e a **PR 4** do plano de auditoria de campos categoricos
(<https://www.notion.so/358edc8b7d5d81cfb33ce023d4cee84f>). Implementa:

- ``rewrite_parquet_typed(input_path, output_path)`` — le um Parquet v1,
  aplica os casts categoricos (Enum/Categorical/Boolean) reutilizando os
  mapas de ``categorical_recovery``, e grava um v2 em path separado com
  encoding de dicionario materializado.

Este modulo NAO altera consumidores de leitura, nao troca o path padrao
do app e nao integra com GUI ou pipeline. E puramente offline e
side-by-side.

Design constraints
------------------
1. **Nunca sobrescreve o original** — ``output_path`` deve ser diferente
   de ``input_path``. Rollback e trivial: deletar o v2.
2. **Reusa mapas existentes** — ``ENUM_MAP``, ``CATEG_MAP``,
   ``INVARIANT_BLOCKLIST`` e ``BOOLEAN_*`` sao importados de
   ``categorical_recovery``. Nao ha lista propria de colunas.
3. **Writer nativo Polars** — ``use_pyarrow=False`` (default).
   ``pyarrow`` pode nao materializar ``RLE_DICTIONARY`` em todos os
   paths; o writer nativo do Polars e deterministico.
4. **Validacao pre-escrita** — invariantes fiscais sao checadas antes
   de gravar; se alguma foi categorizada, levanta ``AssertionError``.
5. **Falha rapida em CFOP/CST invalido** — ``strict_cast=True`` por
   default; ``False`` para migracao incremental com valores sujos.

Usage
-----
    from src.io.categorical_writer import rewrite_parquet_typed

    rewrite_parquet_typed(
        "dados/c170_xml.parquet",
        "dados_v2/c170_xml.parquet",
    )

References
----------
- Plano Notion: ``358edc8b7d5d81cfb33ce023d4cee84f`` §E.4, §G
- ADR-0005: ``docs/adr/0005-categorical-strategy.md``
- Polars #19389: ``scan_parquet`` rebaixa Enum
- Polars #24034: high-cardinality Categorical inflation
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import polars as pl

from src.io.categorical_recovery import (
    INVARIANT_BLOCKLIST,
    assert_no_invariant_categorized,
    build_categorical_map,
    build_enum_map,
    cast_dataframe_typed,
    get_invariant_dtypes,
    load_fiscal_codes,
    validate_schema_post_cast,
)

logger = logging.getLogger(__name__)


def rewrite_parquet_typed(
    input_path: str | Path,
    output_path: str | Path,
    *,
    codes_path: Path | None = None,
    strict_cast: bool = True,
    compression: str = "zstd",
    compression_level: int | None = None,
    statistics: bool = True,
    row_group_size: int | None = None,
    **write_kwargs: Any,
) -> dict[str, Any]:
    """
    Reescreve um Parquet fiscal v1 em v2 com tipagem categorica fisica.

    Le o Parquet de ``input_path``, aplica os casts categoricos
    (Enum/Categorical/Boolean) reutilizando os mapas de
    ``categorical_recovery``, e grava em ``output_path`` com encoding
    de dicionario materializado.

    O arquivo original em ``input_path`` **nunca** e alterado.
    ``output_path`` deve ser diferente de ``input_path``.

    Args:
        input_path: Caminho do Parquet v1 (original, somente leitura).
        output_path: Caminho do Parquet v2 (a ser criado). Deve ser
            diferente de ``input_path``.
        codes_path: Caminho do JSON de codigos fiscais. Default:
            ``DEFAULT_CODES_PATH`` de ``categorical_recovery``.
        strict_cast: Se ``True`` (default), valores fora do Enum levantam
            ``InvalidOperationError``. ``False`` para migracao incremental
            com valores sujos (ex.: "061" em Cst).
        compression: Codec de compressao (default: ``"zstd"``).
        compression_level: Nivel de compressao (default: None = default
            do codec).
        statistics: Gravar estatisticas de coluna (default: ``True``).
        row_group_size: Linhas por row group (default: None = auto
            Polars).
        **write_kwargs: Argumentos extras para ``df.write_parquet``.

    Returns:
        Dicionario com metadados da operacao:
        ``{input_path, output_path, n_rows, n_cols, n_cols_typed,
        invariant_dtypes, schema_diff}``.

    Raises:
        ValueError: Se ``output_path`` e igual a ``input_path``.
        FileNotFoundError: Se ``input_path`` nao existe.
        polars.exceptions.InvalidOperationError: Se o Parquet contem
            valores fora do dominio de um ``pl.Enum`` e
            ``strict_cast=True``.
        AssertionError: Se alguma invariante fiscal foi categorizada
            durante o cast.

    Example:
        >>> result = rewrite_parquet_typed(
        ...     "dados/c170_xml.parquet",
        ...     "dados_v2/c170_xml.parquet",
        ... )
        >>> print(result["n_cols_typed"])
        3
    """
    input_p = Path(input_path).resolve()
    output_p = Path(output_path).resolve()

    if input_p == output_p:
        raise ValueError(
            f"output_path deve ser diferente de input_path. "
            f"Ambos apontam para {input_p}. "
            f"PR4 e side-by-side; nunca sobrescreva o original."
        )

    if not input_p.exists():
        raise FileNotFoundError(f"Parquet v1 nao encontrado: {input_p}")

    # Garantir que o diretorio de saida existe
    output_p.parent.mkdir(parents=True, exist_ok=True)

    logger.info(
        "rewrite_parquet_typed: %s -> %s (strict_cast=%s)",
        input_p.name, output_p, strict_cast,
    )

    # 1. Ler v1 com scan nativo (sem cast ainda — o cast_dataframe_typed
    #    faz o trabalho eager para garantir que o schema escrito e exato)
    df_v1 = pl.read_parquet(input_p)

    # 2. Aplicar casts categoricos (reusa categorical_recovery)
    df_v2 = cast_dataframe_typed(
        df_v1,
        codes_path=codes_path,
        strict_cast=strict_cast,
    )

    # 3. Validacao pre-escrita: invariantes nao podem estar categorizadas
    assert_no_invariant_categorized(df_v2.lazy())

    # 4. Validar schema pos-cast
    schema_diff = validate_schema_post_cast(df_v2.lazy(), codes_path=codes_path)

    # 5. Gravar v2 com writer nativo Polars
    df_v2.write_parquet(
        output_p,
        compression=compression,
        compression_level=compression_level,
        statistics=statistics,
        row_group_size=row_group_size,
        use_pyarrow=False,
        **write_kwargs,
    )

    # 6. Coletar metadados
    n_rows = df_v2.height
    n_cols = df_v2.width
    invariant_dtypes = get_invariant_dtypes(df_v2.lazy())

    # Contar colunas que receberam cast (dtype != String)
    n_cols_typed = sum(
        1 for col, dtype in df_v2.schema.items()
        if isinstance(dtype, (pl.Enum, pl.Categorical))
        or dtype == pl.Boolean
    )

    result: dict[str, Any] = {
        "input_path": str(input_p),
        "output_path": str(output_p),
        "n_rows": n_rows,
        "n_cols": n_cols,
        "n_cols_typed": n_cols_typed,
        "invariant_dtypes": invariant_dtypes,
        "schema_diff": schema_diff,
    }

    logger.info(
        "rewrite_parquet_typed: concluido — %d linhas, %d/%d colunas tipadas",
        n_rows, n_cols_typed, n_cols,
    )

    if schema_diff:
        logger.warning(
            "rewrite_parquet_typed: schema_diff nao vazio: %s",
            schema_diff,
        )

    return result


__all__ = [
    "rewrite_parquet_typed",
]
