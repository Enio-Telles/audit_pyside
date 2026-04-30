from __future__ import annotations

import concurrent.futures
import json
import re
import shutil
import threading
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path, PureWindowsPath
from typing import Callable, Iterable, Sequence

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from rich import print as rprint

from utilitarios.conectar_oracle import conectar
from utilitarios.ler_sql import ler_sql
from utilitarios.project_paths import CNPJ_ROOT, SQL_ROOT
from utilitarios.sql_catalog import list_sql_entries, resolve_sql_path
from utilitarios.validar_cnpj import validar_cnpj

TAMANHO_LOTE_PADRAO = 50_000
MAX_WORKERS_PADRAO = 5

_thread_local = threading.local()


class SchemaMistoExtracaoError(RuntimeError):
    """Indica incompatibilidade de schema que exige reexecucao em modo texto."""


def _looks_like_windows_path(path: Path) -> bool:
    """Retorna True quando o caminho contém barras invertidas ou letra de unidade Windows."""
    texto = str(path)
    return "\\" in texto or bool(PureWindowsPath(texto).drive)


def _relative_sql_path(caminho: Path, raiz_sql: Path) -> Path:
    """Calcula caminho relativo mesmo quando a SQL usa sintaxe Windows no Linux."""
    try:
        return caminho.relative_to(raiz_sql)
    except ValueError:
        if not (_looks_like_windows_path(caminho) or _looks_like_windows_path(raiz_sql)):
            raise

    caminho_win = PureWindowsPath(str(caminho))
    raiz_win = PureWindowsPath(str(raiz_sql))
    try:
        rel_win = caminho_win.relative_to(raiz_win)
    except ValueError:
        raise ValueError(f"{str(caminho)!r} is not in the subpath of {str(raiz_sql)!r}") from None
    return Path(*rel_win.parts)


def _sql_stem(caminho: Path) -> str:
    """Retorna o stem de uma SQL mesmo quando o caminho usa barras Windows."""
    if _looks_like_windows_path(caminho):
        return PureWindowsPath(str(caminho)).stem
    return caminho.stem


@dataclass(frozen=True)
class ConsultaSql:
    """Representa uma consulta SQL junto com a raiz usada para calcular a saida."""

    caminho: Path
    raiz_sql: Path

    def __post_init__(self) -> None:
        """Normaliza ``caminho`` para Path, aceitando str do catálogo SQL."""
        # Garante que caminho seja sempre Path (pode vir como str do catalogo)
        object.__setattr__(self, "caminho", Path(self.caminho))

    @property
    def caminho_relativo(self) -> Path:
        """Caminho da consulta relativo à raiz SQL associada."""
        return _relative_sql_path(self.caminho, self.raiz_sql)


@dataclass
class ResultadoConsultaExtracao:
    """Resultado da extracao de uma consulta SQL individual."""

    consulta: ConsultaSql
    ok: bool
    arquivo_saida: Path | None = None
    linhas: int = 0
    ignorada: bool = False
    erro: str | None = None


def listar_diretorios_sql_padrao() -> list[Path]:
    """Retorna apenas a raiz SQL canonica do projeto."""

    return [SQL_ROOT]


def _deduplicar_preservando_ordem(caminhos: Iterable[Path]) -> list[Path]:
    """Remove caminhos duplicados preservando a ordem de aparecimento."""
    vistos: set[str] = set()
    resultado: list[Path] = []
    for caminho in caminhos:
        chave = str(caminho.resolve()).lower() if caminho.exists() else str(caminho).lower()
        if chave in vistos:
            continue
        vistos.add(chave)
        resultado.append(caminho)
    return resultado


def _normalizar_diretorios_sql(
    diretorios_sql: Sequence[Path | str] | Path | str | None = None,
) -> list[Path]:
    """Converte a entrada heterogênea de diretórios SQL em lista de Path sem duplicatas."""
    if diretorios_sql is None:
        return listar_diretorios_sql_padrao()

    if isinstance(diretorios_sql, (str, Path)):
        return _deduplicar_preservando_ordem([Path(diretorios_sql)])

    return _deduplicar_preservando_ordem(Path(item) for item in diretorios_sql)


def _resolver_raiz_sql(caminho_sql: Path, diretorios_sql: Sequence[Path]) -> Path:
    """Retorna a raiz SQL que contém o caminho, ou o diretório pai como fallback."""
    caminho_resolvido = caminho_sql.resolve() if caminho_sql.exists() else caminho_sql
    for raiz in diretorios_sql:
        try:
            caminho_resolvido.relative_to(raiz.resolve())
            return raiz
        except Exception:
            continue
    return caminho_sql.parent


def descobrir_consultas_sql(
    consultas_selecionadas: Sequence[Path | str] | None = None,
    diretorios_sql: Sequence[Path | str] | Path | str | None = None,
) -> list[ConsultaSql]:
    """Descobre as consultas SQL disponiveis ou normaliza uma selecao explicita."""

    # When caller provides explicit dirs (e.g. tests), use them directly;
    # otherwise fall back to the canonical SQL_ROOT via sql_catalog.
    diretorios_explicitados = diretorios_sql is not None
    diretorios = _normalizar_diretorios_sql(diretorios_sql)
    consultas: list[ConsultaSql] = []

    if consultas_selecionadas:
        for consulta in consultas_selecionadas:
            consulta_path = Path(consulta)
            if diretorios_explicitados:
                for raiz in diretorios:
                    candidato = (
                        (raiz / consulta_path).resolve()
                        if (raiz / consulta_path).exists()
                        else raiz / consulta_path
                    )
                    if candidato.exists():
                        consultas.append(ConsultaSql(caminho=Path(candidato), raiz_sql=raiz))
                        break
                else:
                    caminho = Path(resolve_sql_path(consulta))
                    consultas.append(ConsultaSql(caminho=caminho, raiz_sql=SQL_ROOT))
            else:
                caminho = Path(resolve_sql_path(consulta))
                raiz_consulta = _resolver_raiz_sql(caminho, diretorios)
                consultas.append(ConsultaSql(caminho=caminho, raiz_sql=raiz_consulta))
    else:
        if diretorios_explicitados:
            for raiz in diretorios:
                for sql_path in raiz.rglob("*.sql"):
                    consultas.append(ConsultaSql(caminho=Path(sql_path), raiz_sql=raiz))
        else:
            for entry in list_sql_entries():
                caminho = Path(entry.path)
                consultas.append(ConsultaSql(caminho=caminho, raiz_sql=SQL_ROOT))

    consultas_unicas: list[ConsultaSql] = []
    vistos: set[str] = set()
    for consulta in consultas:
        chave = str(consulta.caminho_relativo).lower()
        if chave in vistos:
            continue
        vistos.add(chave)
        consultas_unicas.append(consulta)

    return sorted(consultas_unicas, key=lambda item: str(item.caminho_relativo).lower())


def obter_caminho_saida_parquet(
    consulta: ConsultaSql,
    cnpj_limpo: str,
    pasta_saida_base: Path,
) -> Path:
    """Mantem a hierarquia relativa da SQL dentro de arquivos_parquet."""

    # Garantir que caminho seja Path (pode vir como str)
    caminho_path = (
        Path(consulta.caminho) if not isinstance(consulta.caminho, Path) else consulta.caminho
    )

    caminho_relativo = _relative_sql_path(caminho_path, consulta.raiz_sql)
    if caminho_relativo.parts and caminho_relativo.parts[0].lower() == "arquivos_parquet":
        caminho_relativo = (
            Path(*caminho_relativo.parts[1:]) if len(caminho_relativo.parts) > 1 else Path()
        )
    nome_arquivo = f"{_sql_stem(caminho_path)}_{cnpj_limpo}.parquet"
    return pasta_saida_base / caminho_relativo.parent / nome_arquivo


def _obter_conexao_thread():
    """Retorna a conexão Oracle da thread local, criando-a se necessário."""
    if not hasattr(_thread_local, "conexao") or _thread_local.conexao is None:
        _thread_local.conexao = conectar()
    return _thread_local.conexao


def fechar_conexao_thread() -> None:
    """Fecha e libera a conexão Oracle armazenada no armazenamento local da thread atual."""
    conexao = getattr(_thread_local, "conexao", None)
    if conexao is None:
        return
    try:
        conexao.close()
    except Exception:
        pass
    finally:
        _thread_local.conexao = None


def _montar_binds_cursor(
    cursor, cnpj_limpo: str, data_limite_input: str | None
) -> tuple[dict[str, str | None], bool]:
    """Monta o dicionário de binds para o cursor Oracle.

    Args:
        cursor: Cursor Oracle com ``bindnames()`` disponível.
        cnpj_limpo: CNPJ sem formatação (14 dígitos).
        data_limite_input: Data limite no formato ``YYYY-MM-DD`` ou ``None``.

    Returns:
        Tupla ``(binds, tem_bind_cnpj)`` onde ``tem_bind_cnpj`` indica se o SQL
        possui o bind ``:CNPJ`` obrigatório.
    """
    binds: dict[str, str | None] = {}
    tem_bind_cnpj = False
    aliases_padrao: dict[str, str | None] = {
        "DATA_LIMITE_PROCESSAMENTO": data_limite_input if data_limite_input else None,
        "DATA_INICIAL": None,
        "DATA_FINAL": data_limite_input if data_limite_input else None,
        "CODIGO_ITEM": None,
        "CHAVE_ACESSO": None,
        "MES": None,
        "ANO": None,
    }
    for nome_bind in cursor.bindnames():
        nome_maiusculo = nome_bind.upper()
        if nome_maiusculo == "CNPJ":
            binds[nome_bind] = cnpj_limpo
            tem_bind_cnpj = True
        elif nome_maiusculo in aliases_padrao:
            binds[nome_bind] = aliases_padrao[nome_maiusculo]
    return binds, tem_bind_cnpj


def _normalizar_valores_coluna(
    valores: list[object | None], forcar_texto: bool = False
) -> list[object | None]:
    """Homogeniza os tipos de uma coluna para evitar erros de schema misto no Polars.

    Args:
        valores: Lista de valores brutos retornados pelo cursor Oracle.
        forcar_texto: Quando ``True``, converte todos os valores para ``str``.

    Returns:
        Lista com tipos homogêneos compatíveis com ``pl.DataFrame``.
    """
    if forcar_texto:
        return [None if valor is None else str(valor) for valor in valores]

    tipos = {type(valor) for valor in valores if valor is not None}
    if len(tipos) <= 1:
        return valores

    if tipos.issubset({int, float, Decimal}):
        return [None if valor is None else float(valor) for valor in valores]

    return [None if valor is None else str(valor) for valor in valores]


def _criar_dataframe_lote(
    lote: list[tuple],
    colunas: list[str],
    schema_polars: dict[str, pl.DataType] | None = None,
    forcar_texto: bool = False,
) -> pl.DataFrame:
    """Constrói um DataFrame Polars a partir de um lote de tuplas do cursor Oracle.

    Args:
        lote: Linhas retornadas por ``cursor.fetchmany()``.
        colunas: Nomes de colunas na mesma ordem que as tuplas.
        schema_polars: Schema Polars a aplicar via ``cast``; ``None`` para inferência.
        forcar_texto: Converte todas as colunas para ``Utf8`` antes de criar o DataFrame.

    Returns:
        DataFrame Polars com os dados do lote.
    """
    try:
        if forcar_texto:
            raise TypeError("Modo texto solicitado.")
        df_lote = pl.DataFrame(lote, schema=colunas, orient="row")
    except Exception:
        colunas_dict = {
            coluna: _normalizar_valores_coluna(
                [linha[indice] for linha in lote],
                forcar_texto=forcar_texto,
            )
            for indice, coluna in enumerate(colunas)
        }
        df_lote = pl.DataFrame(colunas_dict)
    if schema_polars is not None:
        df_lote = df_lote.cast(schema_polars, strict=False)
    return df_lote


def _escrever_dataframe_vazio(colunas: list[str], arquivo_saida: Path) -> None:
    """Grava um Parquet vazio preservando o schema de colunas quando a consulta retorna zero linhas."""
    pl.DataFrame({coluna: [] for coluna in colunas}).write_parquet(
        arquivo_saida, compression="snappy"
    )


def _parquet_valido(arquivo: Path) -> bool:
    """Retorna True se o arquivo parquet existe, nao esta vazio e pode ser lido sem erro."""
    if not arquivo.exists() or arquivo.stat().st_size == 0:
        return False
    try:
        pl.scan_parquet(arquivo).collect_schema()
        return True
    except Exception:
        return False


def _checkpoint_dir(arquivo_saida: Path) -> Path:
    return arquivo_saida.with_name(f"{arquivo_saida.name}.resume")


def _manifesto_checkpoint_path(diretorio: Path) -> Path:
    return diretorio / "manifest.json"


def _carregar_manifesto_checkpoint(diretorio: Path) -> dict | None:
    caminho = _manifesto_checkpoint_path(diretorio)
    if not caminho.exists():
        return None
    try:
        with open(caminho, encoding="utf-8") as f:
            manifesto = json.load(f)
        if not isinstance(manifesto, dict) or not isinstance(manifesto.get("chunks"), list):
            return None
        return manifesto
    except Exception:
        return None


def _salvar_manifesto_checkpoint(diretorio: Path, manifesto: dict) -> None:
    diretorio.mkdir(parents=True, exist_ok=True)
    caminho = _manifesto_checkpoint_path(diretorio)
    tmp = caminho.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(manifesto, f, ensure_ascii=False, indent=2)
    tmp.replace(caminho)


def _remover_checkpoint(arquivo_saida: Path) -> None:
    shutil.rmtree(_checkpoint_dir(arquivo_saida), ignore_errors=True)


def _preparar_manifesto_checkpoint(
    arquivo_saida: Path,
    colunas: list[str],
    forcar_texto: bool,
) -> tuple[Path, dict]:
    diretorio = _checkpoint_dir(arquivo_saida)
    manifesto = _carregar_manifesto_checkpoint(diretorio)
    if manifesto is not None:
        chunks_validos = all((diretorio / item.get("arquivo", "")).exists() for item in manifesto["chunks"])
        compativel = (
            manifesto.get("colunas") == colunas
            and bool(manifesto.get("forcar_texto", False)) == bool(forcar_texto)
            and chunks_validos
        )
        if compativel:
            return diretorio, manifesto

    shutil.rmtree(diretorio, ignore_errors=True)
    diretorio.mkdir(parents=True, exist_ok=True)
    manifesto = {
        "versao": 1,
        "colunas": colunas,
        "forcar_texto": bool(forcar_texto),
        "chunks": [],
        "total_linhas": 0,
    }
    _salvar_manifesto_checkpoint(diretorio, manifesto)
    return diretorio, manifesto


def _pular_linhas_cursor(
    cursor,
    linhas: int,
    tamanho_lote: int,
    progresso: Callable[[str], None] | None = None,
    rotulo_consulta: str | None = None,
) -> None:
    restantes = int(linhas)
    puladas = 0
    while restantes > 0:
        lote = cursor.fetchmany(min(tamanho_lote, restantes))
        if not lote:
            raise RuntimeError(
                "Checkpoint de extracao maior que o resultado atual da consulta. "
                "Remova o checkpoint e execute novamente."
            )
        qtd = len(lote)
        restantes -= qtd
        puladas += qtd
        if progresso and rotulo_consulta:
            progresso(f"  {rotulo_consulta}: {puladas:,}/{linhas:,} linhas retomadas...")


def _schema_polars_de_primeiro_chunk(diretorio: Path, manifesto: dict) -> dict[str, pl.DataType] | None:
    chunks = manifesto.get("chunks") or []
    if not chunks:
        return None
    primeiro = diretorio / chunks[0]["arquivo"]
    try:
        return dict(pl.scan_parquet(primeiro).collect_schema())
    except Exception:
        return None


def _gravar_chunk_checkpoint(
    diretorio: Path,
    manifesto: dict,
    df_lote: pl.DataFrame,
) -> None:
    indice = len(manifesto["chunks"]) + 1
    nome = f"part_{indice:06d}.parquet"
    destino = diretorio / nome
    tmp = diretorio / f"{nome}.tmp"
    df_lote.write_parquet(tmp, compression="snappy")
    tmp.replace(destino)
    manifesto["chunks"].append({"arquivo": nome, "linhas": df_lote.height})
    manifesto["total_linhas"] = int(manifesto.get("total_linhas", 0)) + int(df_lote.height)
    _salvar_manifesto_checkpoint(diretorio, manifesto)


def _consolidar_chunks_checkpoint(
    diretorio: Path,
    manifesto: dict,
    arquivo_saida: Path,
) -> None:
    arquivo_tmp = arquivo_saida.with_suffix(".parquet.tmp")
    arquivo_tmp.unlink(missing_ok=True)
    chunks = [diretorio / item["arquivo"] for item in manifesto.get("chunks", [])]
    if not chunks:
        _escrever_dataframe_vazio(list(manifesto.get("colunas", [])), arquivo_saida)
        return

    writer: pq.ParquetWriter | None = None
    schema_arrow: pa.Schema | None = None
    try:
        for chunk in chunks:
            parquet_file = pq.ParquetFile(chunk)
            for batch in parquet_file.iter_batches():
                tabela = pa.Table.from_batches([batch])
                if schema_arrow is None:
                    schema_arrow = tabela.schema
                    writer = pq.ParquetWriter(arquivo_tmp, schema_arrow, compression="snappy")
                elif tabela.schema != schema_arrow:
                    try:
                        tabela = tabela.cast(schema_arrow, safe=False)
                    except Exception as exc:
                        raise SchemaMistoExtracaoError(str(exc)) from exc
                writer.write_table(tabela)
        if writer is not None:
            writer.close()
            writer = None
        arquivo_tmp.replace(arquivo_saida)
    finally:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass
        arquivo_tmp.unlink(missing_ok=True)


def _gravar_cursor_em_parquet(
    cursor,
    arquivo_saida: Path,
    tamanho_lote: int = TAMANHO_LOTE_PADRAO,
    progresso: Callable[[str], None] | None = None,
    rotulo_consulta: str | None = None,
    forcar_texto: bool = False,
) -> int:
    """Escreve o cursor em chunks retomaveis e consolida o parquet ao final."""
    colunas = [coluna[0].lower() for coluna in cursor.description]
    arquivo_saida.parent.mkdir(parents=True, exist_ok=True)
    arquivo_tmp = arquivo_saida.with_suffix(".parquet.tmp")

    # Limpa tmp de consolidacao anterior interrompida. Chunks de checkpoint sao preservados.
    arquivo_tmp.unlink(missing_ok=True)

    checkpoint, manifesto = _preparar_manifesto_checkpoint(arquivo_saida, colunas, forcar_texto)
    total_linhas = int(manifesto.get("total_linhas", 0))
    schema_polars = _schema_polars_de_primeiro_chunk(checkpoint, manifesto)

    if total_linhas > 0:
        if progresso and rotulo_consulta:
            progresso(f"Retomando {rotulo_consulta}: {total_linhas:,} linhas ja gravadas.")
        _pular_linhas_cursor(cursor, total_linhas, tamanho_lote, progresso, rotulo_consulta)

    try:
        while True:
            lote = cursor.fetchmany(tamanho_lote)
            if not lote:
                break

            df_lote = _criar_dataframe_lote(
                lote,
                colunas,
                schema_polars,
                forcar_texto=forcar_texto,
            )
            if schema_polars is None:
                schema_polars = df_lote.schema

            _gravar_chunk_checkpoint(checkpoint, manifesto, df_lote)
            total_linhas += df_lote.height

            if progresso and rotulo_consulta:
                progresso(f"  {rotulo_consulta}: {total_linhas:,} linhas lidas/gravadas...")

        _consolidar_chunks_checkpoint(checkpoint, manifesto, arquivo_saida)
        _remover_checkpoint(arquivo_saida)
    except SchemaMistoExtracaoError:
        raise

    return total_linhas


def _formatar_rotulo_consulta(consulta: ConsultaSql) -> str:
    """Formata o rótulo legível da consulta usando separadores Unix, independente do SO."""
    # Garantir que caminho seja Path
    caminho = Path(consulta.caminho) if not isinstance(consulta.caminho, Path) else consulta.caminho
    return str(_relative_sql_path(caminho, consulta.raiz_sql)).replace("\\", "/")


def _extrair_comandos_pre_sql(sql_texto: str) -> tuple[list[str], str]:
    """Separa diretivas ``-- PRE:`` do texto SQL principal.

    Args:
        sql_texto: Texto completo do arquivo SQL.

    Returns:
        Tupla ``(comandos_pre, sql_limpo)`` onde ``comandos_pre`` é a lista de
        comandos a executar antes da consulta e ``sql_limpo`` é o SQL sem as linhas PRE.
    """
    comandos_pre: list[str] = []
    linhas_sql: list[str] = []

    for linha in sql_texto.splitlines():
        linha_strip = linha.strip()
        if linha_strip.upper().startswith("-- PRE:"):
            comando = linha_strip[7:].strip()
            if comando:
                comandos_pre.append(comando.rstrip(";"))
            continue
        linhas_sql.append(linha)

    return comandos_pre, "\n".join(linhas_sql).strip()


def processar_consulta_oracle(
    consulta: ConsultaSql,
    cnpj_limpo: str,
    pasta_saida_base: Path,
    data_limite_input: str | None = None,
    tamanho_lote: int = TAMANHO_LOTE_PADRAO,
    progresso: Callable[[str], None] | None = None,
    pular_existente: bool = False,
) -> ResultadoConsultaExtracao:
    """Executa uma consulta SQL Oracle e grava o resultado em parquet por lotes."""

    # Garantir que caminho seja Path (defesa em profundidade)
    caminho_path = (
        Path(consulta.caminho) if not isinstance(consulta.caminho, Path) else consulta.caminho
    )
    rotulo_consulta = _formatar_rotulo_consulta(consulta)

    arquivo_saida_prev = obter_caminho_saida_parquet(consulta, cnpj_limpo, pasta_saida_base)

    # Remove tmp orphan de execucao anterior interrompida antes de qualquer decisao
    arquivo_tmp_prev = arquivo_saida_prev.with_suffix(".parquet.tmp")
    if arquivo_tmp_prev.exists():
        arquivo_tmp_prev.unlink(missing_ok=True)
        if progresso:
            progresso(f"  Removido arquivo temporario incompleto: {arquivo_tmp_prev.name}")

    if pular_existente and _parquet_valido(arquivo_saida_prev):
        _remover_checkpoint(arquivo_saida_prev)
        if progresso:
            progresso(f"Pulando {rotulo_consulta} (parquet valido ja existe: {arquivo_saida_prev.name})")
        return ResultadoConsultaExtracao(
            consulta=consulta,
            ok=True,
            arquivo_saida=arquivo_saida_prev,
            ignorada=True,
        )

    try:
        conexao = _obter_conexao_thread()
        if not conexao:
            return ResultadoConsultaExtracao(
                consulta=consulta,
                ok=False,
                erro="Falha ao obter conexao Oracle para a thread.",
            )

        sql_texto_bruto = ler_sql(caminho_path)
        if not sql_texto_bruto:
            return ResultadoConsultaExtracao(consulta=consulta, ok=True, ignorada=True)
        comandos_pre, sql_texto = _extrair_comandos_pre_sql(sql_texto_bruto)
        if not sql_texto:
            return ResultadoConsultaExtracao(consulta=consulta, ok=True, ignorada=True)

        with conexao.cursor() as cursor:
            cursor.arraysize = tamanho_lote
            cursor.prefetchrows = tamanho_lote

            for comando_pre in comandos_pre:
                cursor.execute(comando_pre)

            cursor.prepare(sql_texto)
            binds, tem_bind_cnpj = _montar_binds_cursor(cursor, cnpj_limpo, data_limite_input)

            if not tem_bind_cnpj:
                return ResultadoConsultaExtracao(
                    consulta=consulta,
                    ok=True,
                    ignorada=True,
                    erro="Consulta ignorada por nao possuir bind :CNPJ.",
                )

            if progresso:
                progresso(f"Executando {rotulo_consulta}...")

            cursor.execute(None, binds)

            arquivo_saida = obter_caminho_saida_parquet(consulta, cnpj_limpo, pasta_saida_base)
            try:
                total_linhas = _gravar_cursor_em_parquet(
                    cursor=cursor,
                    arquivo_saida=arquivo_saida,
                    tamanho_lote=tamanho_lote,
                    progresso=progresso,
                    rotulo_consulta=rotulo_consulta,
                )
            except SchemaMistoExtracaoError as exc:
                if arquivo_saida.exists():
                    arquivo_saida.unlink(missing_ok=True)
                _remover_checkpoint(arquivo_saida)
                if progresso:
                    progresso(
                        f"Aviso {rotulo_consulta}: schema misto detectado; reexecutando consulta em modo texto ({exc})"
                    )
                with conexao.cursor() as cursor_texto:
                    cursor_texto.arraysize = tamanho_lote
                    cursor_texto.prefetchrows = tamanho_lote
                    for comando_pre in comandos_pre:
                        cursor_texto.execute(comando_pre)
                    cursor_texto.prepare(sql_texto)
                    cursor_texto.execute(None, binds)
                    total_linhas = _gravar_cursor_em_parquet(
                        cursor=cursor_texto,
                        arquivo_saida=arquivo_saida,
                        tamanho_lote=tamanho_lote,
                        progresso=progresso,
                        rotulo_consulta=rotulo_consulta,
                        forcar_texto=True,
                    )

            if progresso:
                progresso(f"OK {rotulo_consulta}: {total_linhas:,} linhas -> {arquivo_saida.name}")

            return ResultadoConsultaExtracao(
                consulta=consulta,
                ok=True,
                arquivo_saida=arquivo_saida,
                linhas=total_linhas,
            )
    except Exception as exc:
        if progresso:
            progresso(
                f"Extracao interrompida em {rotulo_consulta}. "
                "Reexecute a mesma consulta para retomar pelos checkpoints gravados."
            )
        return ResultadoConsultaExtracao(
            consulta=consulta,
            ok=False,
            erro=str(exc),
        )


def executar_extracao_oracle(
    cnpj_input: str,
    data_limite_input: str | None = None,
    consultas_selecionadas: Sequence[Path | str] | None = None,
    pasta_saida_base: Path | None = None,
    diretorios_sql: Sequence[Path | str] | Path | str | None = None,
    max_workers: int = MAX_WORKERS_PADRAO,
    tamanho_lote: int = TAMANHO_LOTE_PADRAO,
    progresso: Callable[[str], None] | None = None,
    pular_existente: bool = False,
) -> list[ResultadoConsultaExtracao]:
    """Executa a extracao Oracle em paralelo por consulta, com escrita incremental em parquet."""

    if not validar_cnpj(cnpj_input):
        raise ValueError(f"CNPJ invalido: {cnpj_input}")

    cnpj_limpo = re.sub(r"[^0-9]", "", cnpj_input)
    consultas = descobrir_consultas_sql(
        consultas_selecionadas=consultas_selecionadas,
        diretorios_sql=diretorios_sql,
    )
    if not consultas:
        return []

    pasta_saida = pasta_saida_base or (CNPJ_ROOT / cnpj_limpo)
    pasta_saida.mkdir(parents=True, exist_ok=True)

    resultados: list[ResultadoConsultaExtracao] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futuros = {
            executor.submit(
                processar_consulta_oracle,
                consulta,
                cnpj_limpo,
                pasta_saida,
                data_limite_input,
                tamanho_lote,
                progresso,
                pular_existente,
            ): consulta
            for consulta in consultas
        }

        for futuro in concurrent.futures.as_completed(futuros):
            resultado = futuro.result()
            resultados.append(resultado)

            if progresso and resultado.erro:
                if resultado.ignorada:
                    progresso(f"Aviso {resultado.consulta.caminho.name}: {resultado.erro}")
                else:
                    progresso(f"Erro em {resultado.consulta.caminho.name}: {resultado.erro}")

        for _ in range(max_workers):
            executor.submit(fechar_conexao_thread)

    return sorted(resultados, key=lambda item: str(item.consulta.caminho_relativo).lower())


def imprimir_resumo_extracao(resultados: Sequence[ResultadoConsultaExtracao]) -> bool:
    """Imprime o resumo final da extracao para a interface de linha de comando."""

    sucesso = True
    for resultado in resultados:
        if resultado.ok:
            continue
        sucesso = False
        rprint(f"[red]Falha em {resultado.consulta.caminho.name}:[/red] {resultado.erro}")
    return sucesso
