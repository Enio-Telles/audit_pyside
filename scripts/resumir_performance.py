from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path


def _root_dir() -> Path:
    return Path(__file__).resolve().parents[1]


def _caminho_log() -> Path:
    return _root_dir() / "logs" / "performance" / "perf_events.jsonl"


def _novo_bloco() -> dict[str, float]:
    return {"count": 0, "total": 0.0, "min": float("inf"), "max": 0.0, "errors": 0}


def _atualizar_bloco(bloco: dict[str, float], duracao: float, status: str) -> None:
    bloco["count"] += 1
    bloco["total"] += duracao
    bloco["min"] = min(bloco["min"], duracao)
    bloco["max"] = max(bloco["max"], duracao)
    if status != "ok":
        bloco["errors"] += 1


def _prefixo_evento(nome: str) -> str:
    partes = (nome or "").split(".")
    return partes[0] if partes and partes[0] else "desconhecido"


def _formatar_linhas(
    agregados: dict[str, dict[str, float]]
) -> list[tuple[str, int, float, float, float, float, int]]:
    linhas = []
    for nome, bloco in agregados.items():
        media = bloco["total"] / bloco["count"] if bloco["count"] else 0.0
        minimo = bloco["min"] if bloco["count"] else 0.0
        linhas.append(
            (
                nome,
                int(bloco["count"]),
                media,
                minimo,
                bloco["max"],
                bloco["total"],
                int(bloco["errors"]),
            )
        )
    linhas.sort(key=lambda item: item[5], reverse=True)
    return linhas


def _imprimir_tabela(
    titulo: str,
    linhas: list[tuple[str, int, float, float, float, float, int]],
    largura_nome: int = 45,
) -> None:
    print(titulo)
    print("-" * 112)
    print(
        f"{'Nome':{largura_nome}} {'Qtd':>6} {'Media(s)':>10} {'Min(s)':>10} {'Max(s)':>10} {'Total(s)':>10} {'Erros':>7}"
    )
    print("-" * 112)
    for nome, qtd, media, minimo, maximo, total, erros in linhas:
        print(
            f"{nome[:largura_nome]:{largura_nome}} {qtd:6d} {media:10.3f} {minimo:10.3f} {maximo:10.3f} {total:10.3f} {erros:7d}"
        )
    print()


def main() -> int:
    caminho = _caminho_log()
    if not caminho.exists():
        print(f"Arquivo de log nao encontrado: {caminho}")
        return 1

    por_evento: dict[str, dict[str, float]] = defaultdict(_novo_bloco)
    por_prefixo: dict[str, dict[str, float]] = defaultdict(_novo_bloco)
    total_eventos = 0

    with caminho.open("r", encoding="utf-8") as arquivo:
        for linha in arquivo:
            linha = linha.strip()
            if not linha:
                continue
            try:
                evento = json.loads(linha)
            except json.JSONDecodeError:
                continue

            nome = str(evento.get("evento") or "desconhecido")
            status = str(evento.get("status") or "ok")
            duracao = float(evento.get("duracao_s") or 0.0)
            prefixo = _prefixo_evento(nome)

            _atualizar_bloco(por_evento[nome], duracao, status)
            _atualizar_bloco(por_prefixo[prefixo], duracao, status)
            total_eventos += 1

    print(f"Resumo de performance: {total_eventos} eventos")
    print()
    _imprimir_tabela("Por modulo", _formatar_linhas(por_prefixo), largura_nome=30)
    _imprimir_tabela("Por evento", _formatar_linhas(por_evento), largura_nome=55)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
