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
        input_p.name,
        output_p,
        strict_cast,
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
        1
        for col, dtype in df_v2.schema.items()
        if isinstance(dtype, (pl.Enum, pl.Categorical)) or dtype == pl.Boolean
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
        n_rows,
        n_cols_typed,
        n_cols,
    )

    if schema_diff:
        logger.warning(
            "rewrite_parquet_typed: schema_diff nao vazio: %s",
            schema_diff,
        )

    return result


def batch_rewrite_parquets(
    input_root: str | Path,
    output_root: str | Path,
    *,
    file_list: list[Path] | None = None,
    codes_path: Path | None = None,
    strict_cast: bool = True,
    min_size_mb: float = 0,
    max_files: int | None = None,
    compression: str = "zstd",
    compression_level: int | None = None,
    statistics: bool = True,
    row_group_size: int | None = None,
    **write_kwargs: Any,
) -> list[dict[str, Any]]:
    """
    Reescreve em lote todos os Parquets de ``input_root`` para ``output_root``.

    Quando ``file_list`` e fornecido, usa exatamente essa lista de arquivos
    (ignorando ``min_size_mb`` e ``max_files``). Quando ``None`` (default),
    descobre recursivamente arquivos ``*.parquet`` em ``input_root`` e
    aplica os filtros ``min_size_mb`` e ``max_files``.

    Args:
        input_root: Diretorio raiz com Parquets v1.
        output_root: Diretorio raiz para Parquets v2 (criado se nao existir).
        file_list: Lista opcional de paths de Parquet para processar.
            Quando fornecido, ``min_size_mb`` e ``max_files`` sao ignorados.
        codes_path: Caminho do JSON de codigos fiscais.
        strict_cast: Se ``True`` (default), valores fora do Enum levantam erro.
        min_size_mb: Tamanho minimo do arquivo em MB para incluir no lote.
            Ignorado quando ``file_list`` e fornecido.
        max_files: Numero maximo de arquivos a processar (default: None = todos).
            Ignorado quando ``file_list`` e fornecido.
        compression: Codec de compressao (default: ``"zstd"``).
        compression_level: Nivel de compressao.
        statistics: Gravar estatisticas de coluna (default: ``True``).
        row_group_size: Linhas por row group.
        **write_kwargs: Argumentos extras para ``df.write_parquet``.

    Returns:
        Lista de dicionarios com metadados de cada rewrite (mesmo formato
        de ``rewrite_parquet_typed``).

    Raises:
        FileNotFoundError: Se ``input_root`` nao existe.
    """
    input_root_p = Path(input_root).resolve()
    output_root_p = Path(output_root).resolve()

    if not input_root_p.exists():
        raise FileNotFoundError(f"Diretorio de entrada nao encontrado: {input_root_p}")

    # Usar file_list externo ou descobrir internamente
    if file_list is not None:
        parquet_files = list(file_list)
    else:
        # Descobrir Parquets
        parquet_files = sorted(input_root_p.rglob("*.parquet"))

        # Filtrar por tamanho minimo
        if min_size_mb > 0:
            min_bytes = min_size_mb * 1024 * 1024
            parquet_files = [p for p in parquet_files if p.stat().st_size >= min_bytes]

        # Limitar numero de arquivos
        if max_files is not None and max_files > 0:
            parquet_files = parquet_files[:max_files]

    if not parquet_files:
        logger.warning(
            "batch_rewrite_parquets: nenhum Parquet encontrado em %s (min_size_mb=%s)",
            input_root_p,
            min_size_mb,
        )
        return []

    logger.info(
        "batch_rewrite_parquets: %d Parquets encontrados em %s",
        len(parquet_files),
        input_root_p,
    )

    results: list[dict[str, Any]] = []
    errors: list[tuple[Path, str]] = []

    for i, parquet_path in enumerate(parquet_files, 1):
        # Calcular output_path preservando estrutura de pastas
        rel_path = parquet_path.relative_to(input_root_p)
        output_path = output_root_p / rel_path

        try:
            result = rewrite_parquet_typed(
                parquet_path,
                output_path,
                codes_path=codes_path,
                strict_cast=strict_cast,
                compression=compression,
                compression_level=compression_level,
                statistics=statistics,
                row_group_size=row_group_size,
                **write_kwargs,
            )
            results.append(result)
            logger.info(
                "[%d/%d] OK %s -> %s (%d linhas, %d colunas tipadas)",
                i,
                len(parquet_files),
                parquet_path.name,
                output_path.name,
                result["n_rows"],
                result["n_cols_typed"],
            )
        except Exception as exc:
            errors.append((parquet_path, str(exc)))
            logger.error(
                "[%d/%d] ERRO %s: %s",
                i,
                len(parquet_files),
                parquet_path.name,
                exc,
            )

    # Resumo final
    n_ok = len(results)
    n_err = len(errors)
    n_total = n_ok + n_err
    logger.info(
        "batch_rewrite_parquets: concluido — %d/%d OK, %d erro(s)",
        n_ok,
        n_total,
        n_err,
    )

    if errors:
        logger.warning("Erros:")
        for path, msg in errors:
            logger.warning("  %s: %s", path, msg)

    return results


__all__ = [
    "batch_rewrite_parquets",
    "rewrite_parquet_typed",
]
