from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq


def _safe_print(message: str) -> None:
    try:
        print(message)
    except UnicodeEncodeError:
        print(message.encode("ascii", errors="replace").decode("ascii"))


def salvar_para_parquet(
    df,
    caminho_saida: Path,
    nome_arquivo: str = None,
    schema=None,
    metadata: dict = None,
) -> bool:
    """
    Exporta um DataFrame ou LazyFrame do Polars para Parquet.

    Args:
        df: polars.DataFrame ou polars.LazyFrame.
        caminho_saida: diretorio (Path) ou caminho completo do arquivo.
        nome_arquivo: nome do arquivo, se caminho_saida for um diretorio.
        schema: pyarrow.Schema opcional para impor tipos.
        metadata: metadados por coluna {col_name: description}.

    Returns:
        True em caso de sucesso, False em caso de erro.
    """
    try:
        if nome_arquivo:
            if not str(nome_arquivo).lower().endswith(".parquet"):
                nome_arquivo = f"{nome_arquivo}.parquet"
            arquivo = caminho_saida / nome_arquivo
        else:
            arquivo = caminho_saida

        arquivo.parent.mkdir(parents=True, exist_ok=True)

        if isinstance(df, pl.LazyFrame):
            df = df.collect()

        if df.is_empty():
            _safe_print(f"   [!] Aviso: o DataFrame a ser salvo em {arquivo.name} esta vazio.")

        if schema or metadata:
            table = df.to_arrow()

            if schema:
                try:
                    table = table.cast(schema)
                except Exception as e_schema:
                    _safe_print(
                        f"   [!] Aviso de schema: falha ao impor schema estrito em {arquivo.name}: {e_schema}"
                    )

            if metadata:
                new_fields = []
                for field in table.schema:
                    if field.name in metadata:
                        desc_value = str(metadata[field.name]).encode("utf-8")
                        new_meta = {
                            **(field.metadata or {}),
                            b"description": desc_value,
                            b"comment": desc_value,
                        }
                        new_fields.append(field.with_metadata(new_meta))
                    else:
                        new_fields.append(field)

                table = pa.Table.from_batches(
                    table.to_batches(),
                    pa.schema(new_fields, metadata=table.schema.metadata),
                )

            pq.write_table(table, arquivo, compression="snappy")
        else:
            df.write_parquet(arquivo, compression="snappy")

        _safe_print(f"   [OK] Parquet salvo com sucesso: {arquivo.name}")
        return True

    except Exception as e:
        nome = arquivo.name if "arquivo" in locals() else str(nome_arquivo or caminho_saida)
        _safe_print(f"   [ERRO] Erro ao salvar arquivo Parquet {nome}: {e}")
        return False
