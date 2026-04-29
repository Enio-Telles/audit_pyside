from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from statistics import median
from typing import Any


def _root_dir() -> Path:
    return Path(__file__).resolve().parents[1]


def _caminho_log() -> Path:
    return _root_dir() / "logs" / "performance" / "perf_events.jsonl"


def _novo_bloco() -> dict[str, Any]:
    return {
        "count": 0,
        "total": 0.0,
        "min": float("inf"),
        "max": 0.0,
        "errors": 0,
        "duracoes": [],
        "linhas": 0,
        "colunas_max": 0,
        "cache_hits": 0,
        "cache_misses": 0,
    }


def _as_float(valor: Any, default: float = 0.0) -> float:
    try:
        return float(valor)
    except Exception:
        return default


def _as_int(valor: Any, default: int = 0) -> int:
    try:
        return int(valor)
    except Exception:
        return default


def _contexto(evento: dict[str, Any]) -> dict[str, Any]:
    ctx = evento.get("contexto")
    return ctx if isinstance(ctx, dict) else {}


def _extrair_linhas_colunas(ctx: dict[str, Any]) -> tuple[int, int]:
    linhas = 0
    for chave in (
        "linhas",
        "total_rows",
        "linhas_pagina",
        "linhas_processadas",
        "qtd_linhas",
    ):
        linhas = max(linhas, _as_int(ctx.get(chave)))
    colunas = 0
    for chave in ("colunas", "colunas_visiveis", "colunas_solicitadas"):
        colunas = max(colunas, _as_int(ctx.get(chave)))
    return linhas, colunas


def _atualizar_bloco(bloco: dict[str, Any], evento: dict[str, Any]) -> None:
    status = str(evento.get("status") or "ok")
    duracao = _as_float(evento.get("duracao_s"))
    ctx = _contexto(evento)
    linhas, colunas = _extrair_linhas_colunas(ctx)

    bloco["count"] += 1
    bloco["total"] += duracao
    bloco["min"] = min(bloco["min"], duracao)
    bloco["max"] = max(bloco["max"], duracao)
    bloco["duracoes"].append(duracao)
    bloco["linhas"] += linhas
    bloco["colunas_max"] = max(bloco["colunas_max"], colunas)
    if status != "ok":
        bloco["errors"] += 1

    for chave in ("cache_hit", "cache_hit_count"):
        valor = ctx.get(chave)
        if valor is True:
            bloco["cache_hits"] += 1
        elif valor is False:
            bloco["cache_misses"] += 1


def _prefixo_evento(nome: str) -> str:
    partes = (nome or "").split(".")
    return partes[0] if partes and partes[0] else "desconhecido"


def _percentil(valores: list[float], p: float) -> float:
    if not valores:
        return 0.0
    ordenados = sorted(valores)
    if len(ordenados) == 1:
        return ordenados[0]
    idx = round((len(ordenados) - 1) * p)
    return ordenados[max(0, min(idx, len(ordenados) - 1))]


def _formatar_linhas(
    agregados: dict[str, dict[str, Any]],
    sort_key: str = "total",
) -> list[tuple[str, int, float, float, float, float, float, float, int, int, int, int, int]]:
    linhas = []
    for nome, bloco in agregados.items():
        count = int(bloco["count"])
        duracoes = list(bloco.get("duracoes") or [])
        media = bloco["total"] / count if count else 0.0
        minimo = bloco["min"] if count else 0.0
        linhas.append(
            (
                nome,
                count,
                media,
                float(median(duracoes)) if duracoes else 0.0,
                _percentil(duracoes, 0.95),
                minimo,
                bloco["max"],
                bloco["total"],
                int(bloco["errors"]),
                int(bloco.get("linhas") or 0),
                int(bloco.get("colunas_max") or 0),
                int(bloco.get("cache_hits") or 0),
                int(bloco.get("cache_misses") or 0),
            )
        )
    indices = {
        "count": 1,
        "media": 2,
        "p95": 4,
        "max": 6,
        "total": 7,
        "erros": 8,
        "linhas": 9,
    }
    linhas.sort(key=lambda item: item[indices.get(sort_key, 7)], reverse=True)
    return linhas


def _imprimir_tabela(
    titulo: str,
    linhas: list[tuple[str, int, float, float, float, float, float, float, int, int, int, int, int]],
    largura_nome: int = 45,
    top: int | None = None,
) -> None:
    linhas = linhas[:top] if top else linhas
    print(titulo)
    print("-" * 148)
    print(
        f"{'Nome':{largura_nome}} {'Qtd':>6} {'Media':>8} {'P50':>8} {'P95':>8} {'Min':>8} {'Max':>8} {'Total':>9} {'Erros':>6} {'Linhas':>10} {'Cols':>5} {'Hit':>5} {'Miss':>5}"
    )
    print("-" * 148)
    for nome, qtd, media, p50, p95, minimo, maximo, total, erros, linhas_proc, colunas, hits, misses in linhas:
        print(
            f"{nome[:largura_nome]:{largura_nome}} {qtd:6d} {media:8.3f} {p50:8.3f} {p95:8.3f} {minimo:8.3f} {maximo:8.3f} {total:9.3f} {erros:6d} {linhas_proc:10d} {colunas:5d} {hits:5d} {misses:5d}"
        )
    print()


def _ler_eventos(caminho: Path) -> list[dict[str, Any]]:
    eventos: list[dict[str, Any]] = []
    with caminho.open("r", encoding="utf-8") as arquivo:
        for linha in arquivo:
            linha = linha.strip()
            if not linha:
                continue
            try:
                evento = json.loads(linha)
            except json.JSONDecodeError:
                continue
            if isinstance(evento, dict):
                eventos.append(evento)
    return eventos


def _agrupar(eventos: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    por_evento: dict[str, dict[str, Any]] = defaultdict(_novo_bloco)
    por_prefixo: dict[str, dict[str, Any]] = defaultdict(_novo_bloco)
    for evento in eventos:
        nome = str(evento.get("evento") or "desconhecido")
        prefixo = _prefixo_evento(nome)
        _atualizar_bloco(por_evento[nome], evento)
        _atualizar_bloco(por_prefixo[prefixo], evento)
    return por_evento, por_prefixo


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Resume logs/performance/perf_events.jsonl")
    parser.add_argument("--top", type=int, default=20, help="quantidade de linhas nos rankings principais")
    parser.add_argument("--sort", default="total", choices=["total", "max", "p95", "media", "count", "erros", "linhas"])
    args = parser.parse_args(argv)

    caminho = _caminho_log()
    if not caminho.exists():
        print(f"Arquivo de log nao encontrado: {caminho}")
        return 1

    eventos = _ler_eventos(caminho)
    por_evento, por_prefixo = _agrupar(eventos)

    print(f"Resumo de performance: {len(eventos)} eventos")
    print(f"Arquivo: {caminho}")
    print()

    _imprimir_tabela(
        "Por modulo (ordenado por tempo total)",
        _formatar_linhas(por_prefixo, "total"),
        largura_nome=30,
    )
    _imprimir_tabela(
        f"Top {args.top} eventos por {args.sort}",
        _formatar_linhas(por_evento, args.sort),
        largura_nome=58,
        top=args.top,
    )
    _imprimir_tabela(
        f"Top {args.top} piores casos individuais",
        _formatar_linhas(por_evento, "max"),
        largura_nome=58,
        top=args.top,
    )
    _imprimir_tabela(
        f"Top {args.top} por P95",
        _formatar_linhas(por_evento, "p95"),
        largura_nome=58,
        top=args.top,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
