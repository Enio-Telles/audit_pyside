from __future__ import annotations

import concurrent.futures
import re
import threading
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Callable, Iterable, Sequence

import polars as pl
import pyarrow.parquet as pq
from rich import print as rprint

from utilitarios.conectar_oracle import conectar
from utilitarios.ler_sql import ler_sql
from utilitarios.project_paths import CNPJ_ROOT, SQL_ARCHIVE_ROOT, SQL_ROOT
from utilitarios.sql_catalog import get_sql_id, list_sql_entries, resolve_sql_path
from utilitarios.validar_cnpj import validar_cnpj

TAMANHO_LOTE_PADRAO = 50_000
MAX_WORKERS_PADRAO = 5

_thread_local = threading.local()


@dataclass(frozen=True)
class ConsultaSql:
    """Representa uma consulta SQL junto com a raiz usada para calcular a saida."""

    caminho: Path
    raiz_sql: Path

    @property
    def caminho_relativo(self) -> Path:
        return self.caminho.relative_to(self.raiz_sql)


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
    if diretorios_sql is None:
        return listar_diretorios_sql_padrao()

    if isinstance(diretorios_sql, (str, Path)):
        return _deduplicar_preservando_ordem([Path(diretorios_sql)])

    return _deduplicar_preservando_ordem(Path(item) for item in diretorios_sql)


def _resolver_raiz_sql(caminho_sql: Path, diretorios_sql: Sequence[Path]) -> Path:
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
                    candidato = (raiz / consulta_path).resolve() if (raiz / consulta_path).exists() else raiz / consulta_path
                    if candidato.exists():
                        consultas.append(ConsultaSql(caminho=candidato, raiz_sql=raiz))
                        break
                else:
                    caminho = resolve_sql_path(consulta)
                    consultas.append(ConsultaSql(caminho=caminho, raiz_sql=SQL_ROOT))
            else:
                caminho = resolve_sql_path(consulta)
                raiz_consulta = _resolver_raiz_sql(caminho, diretorios)
                consultas.append(ConsultaSql(caminho=caminho, raiz_sql=raiz_consulta))
    else:
        if diretorios_explicitados:
            for raiz in diretorios:
                for sql_path in raiz.rglob("*.sql"):
                    consultas.append(ConsultaSql(caminho=sql_path, raiz_sql=raiz))
        else:
            for entry in list_sql_entries():
                caminho = entry.path
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

    caminho_relativo = consulta.caminho_relativo
    if caminho_relativo.parts and caminho_relativo.parts[0].lower() == "arquivos_parquet":
        caminho_relativo = Path(*caminho_relativo.parts[1:]) if len(caminho_relativo.parts) > 1 else Path()
    nome_arquivo = f"{consulta.caminho.stem}_{cnpj_limpo}.parquet"
    return pasta_saida_base / caminho_relativo.parent / nome_arquivo


def _obter_conexao_thread():
    if not hasattr(_thread_local, "conexao") or _thread_local.conexao is None:
        _thread_local.conexao = conectar()
    return _thread_local.conexao


def fechar_conexao_thread() -> None:
    conexao = getattr(_thread_local, "conexao", None)
    if conexao is None:
        return
    try:
        conexao.close()
    except Exception:
        pass
    finally:
        _thread_local.conexao = None


def _montar_binds_cursor(cursor, cnpj_limpo: str, data_limite_input: str | None) -> tuple[dict[str, str | None], bool]:
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


def _normalizar_valores_coluna(valores: list[object | None], forcar_texto: bool = False) -> list[object | None]:
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
    pl.DataFrame({coluna: [] for coluna in colunas}).write_parquet(arquivo_saida, compression="snappy")


def _gravar_cursor_em_parquet(
    cursor,
    arquivo_saida: Path,
    tamanho_lote: int = TAMANHO_LOTE_PADRAO,
    progresso: Callable[[str], None] | None = None,
    rotulo_consulta: str | None = None,
    forcar_texto: bool = False,
) -> int:
    """Escreve o resultado do cursor em lotes, evitando concentrar toda a consulta em memoria."""

    colunas = [coluna[0].lower() for coluna in cursor.description]
    arquivo_saida.parent.mkdir(parents=True, exist_ok=True)

    writer: pq.ParquetWriter | None = None
    schema_polars: dict[str, pl.DataType] | None = None
    schema_arrow = None
    total_linhas = 0

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

            tabela_arrow = df_lote.to_arrow()
            if schema_arrow is None:
                schema_arrow = tabela_arrow.schema
                writer = pq.ParquetWriter(arquivo_saida, schema_arrow, compression="snappy")
            elif tabela_arrow.schema != schema_arrow:
                tabela_arrow = tabela_arrow.cast(schema_arrow, safe=False)

            writer.write_table(tabela_arrow)
            total_linhas += df_lote.height

            if progresso and rotulo_consulta:
                progresso(f"  {rotulo_consulta}: {total_linhas:,} linhas gravadas...")
    finally:
        if writer is not None:
            writer.close()

    if schema_arrow is None:
        _escrever_dataframe_vazio(colunas, arquivo_saida)

    return total_linhas


def _formatar_rotulo_consulta(consulta: ConsultaSql) -> str:
    return str(consulta.caminho_relativo).replace("\\", "/")


def _extrair_comandos_pre_sql(sql_texto: str) -> tuple[list[str], str]:
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
) -> ResultadoConsultaExtracao:
    """Executa uma consulta SQL Oracle e grava o resultado em parquet por lotes."""

    rotulo_consulta = _formatar_rotulo_consulta(consulta)

    try:
        conexao = _obter_conexao_thread()
        if not conexao:
            return ResultadoConsultaExtracao(
                consulta=consulta,
                ok=False,
                erro="Falha ao obter conexao Oracle para a thread.",
            )

        sql_texto_bruto = ler_sql(consulta.caminho)
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
            except Exception as exc:
                if arquivo_saida.exists():
                    arquivo_saida.unlink(missing_ok=True)
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
