"""
pipeline_pos_fontes.py

Executa as etapas 4-10 do pipeline fiscal em sequencia, partindo do ponto em que
gerar_fontes_produtos ja terminou (nfce_agr presente).

Uso:
    PYTHONPATH=src python scripts/pipeline_pos_fontes.py --cnpj 04240370002877
    PYTHONPATH=src python scripts/pipeline_pos_fontes.py --cnpj 04240370002877 --aguardar

Flags:
    --aguardar   Bloqueia ate nfce_agr_{cnpj}.parquet aparecer antes de iniciar.
    --etapa N    Inicia a partir da etapa N (1=fatores, 2=c170_xml, ..., 8=resumo_global).
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path
from time import perf_counter

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def _log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def _etapa(nome: str, fn, *args) -> bool:
    _log(f"iniciando: {nome}")
    t0 = perf_counter()
    try:
        ok = fn(*args)
    except Exception as exc:
        _log(f"ERRO em {nome}: {exc}")
        return False
    elapsed = perf_counter() - t0
    if ok:
        _log(f"concluido: {nome} ({elapsed:.1f}s)")
    else:
        _log(f"FALHOU: {nome} ({elapsed:.1f}s)")
    return bool(ok)


def _aguardar_nfce_agr(pasta_brutos: Path, cnpj: str, timeout: int = 7200) -> bool:
    arq = pasta_brutos / f"nfce_agr_{cnpj}.parquet"
    if arq.exists():
        return True
    _log(f"aguardando {arq.name} (timeout {timeout}s)...")
    inicio = perf_counter()
    while perf_counter() - inicio < timeout:
        if arq.exists():
            _log(f"{arq.name} encontrado, iniciando pipeline.")
            return True
        time.sleep(15)
    _log(f"TIMEOUT: {arq.name} nao apareceu em {timeout}s.")
    return False


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Pipeline fiscal pos-fontes (etapas 4-10)")
    p.add_argument("--cnpj", default="04240370002877", help="CNPJ (so digitos)")
    p.add_argument(
        "--aguardar",
        action="store_true",
        help="Bloqueia ate nfce_agr aparecer antes de iniciar",
    )
    p.add_argument(
        "--etapa",
        type=int,
        default=1,
        metavar="N",
        help="Inicia a partir da etapa N (1=fatores .. 8=resumo_global)",
    )
    args = p.parse_args(argv)

    cnpj = re.sub(r"\D", "", args.cnpj)
    pasta_cnpj = ROOT / "dados" / "CNPJ" / cnpj
    pasta_brutos = pasta_cnpj / "arquivos_parquet"

    if not pasta_cnpj.exists():
        _log(f"pasta do CNPJ nao encontrada: {pasta_cnpj}")
        return 1

    if args.aguardar:
        if not _aguardar_nfce_agr(pasta_brutos, cnpj):
            return 1
    else:
        arq_nfce_agr = pasta_brutos / f"nfce_agr_{cnpj}.parquet"
        if not arq_nfce_agr.exists():
            _log(
                f"nfce_agr_{cnpj}.parquet nao encontrado. "
                "Execute com --aguardar ou aguarde gerar_fontes_produtos terminar."
            )
            return 1

    from transformacao.rastreabilidade_produtos.fatores_conversao import (
        calcular_fatores_conversao,
    )
    from transformacao.movimentacao_estoque_pkg.c170_xml import gerar_c170_xml
    from transformacao.movimentacao_estoque_pkg.c176_xml import gerar_c176_xml
    from transformacao.movimentacao_estoque_pkg.movimentacao_estoque import (
        gerar_movimentacao_estoque,
    )
    from transformacao.calculos_mensais_pkg.calculos_mensais import gerar_calculos_mensais
    from transformacao.calculos_anuais_pkg.calculos_anuais import gerar_calculos_anuais
    from transformacao.calculos_periodo_pkg.calculos_periodo import gerar_calculos_periodos
    from transformacao.resumo_global import gerar_aba_resumo_global

    etapas = [
        (1, "fatores_conversao",      calcular_fatores_conversao,    cnpj),
        (2, "c170_xml",               gerar_c170_xml,                cnpj),
        (3, "c176_xml",               gerar_c176_xml,                cnpj),
        (4, "movimentacao_estoque",   gerar_movimentacao_estoque,    cnpj),
        (5, "calculos_mensais",       gerar_calculos_mensais,        cnpj),
        (6, "calculos_anuais",        gerar_calculos_anuais,         cnpj),
        (7, "calculos_periodos",      gerar_calculos_periodos,       cnpj),
        (8, "resumo_global",          gerar_aba_resumo_global,       cnpj),
    ]

    t_total = perf_counter()
    for num, nome, fn, arg in etapas:
        if num < args.etapa:
            _log(f"pulando etapa {num}: {nome}")
            continue
        if not _etapa(f"{num}/{len(etapas)} {nome}", fn, arg):
            _log(f"pipeline interrompido na etapa {num} ({nome}).")
            return 1

    _log(f"pipeline concluido em {perf_counter() - t_total:.1f}s total.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
