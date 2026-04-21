#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
review_logs.py
Varre o workspace em busca de mensagens de erro/tracebacks e gera um relatório
em `output/log_review_report.txt`.

Usage:
  python scripts/review_logs.py [root_dir]

Saída:
  - Imprime relatório no stdout
  - Salva relatório em `output/log_review_report.txt`
"""

import os
import re
import sys
from pathlib import Path
from datetime import datetime

# Config
ROOT = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".")
OUT_DIR = ROOT / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
REPORT_PATH = OUT_DIR / "log_review_report.txt"

KEYWORDS = [
    "Traceback (most recent call last)",
    "Traceback",
    "Exception",
    "ERROR",
    "CRITICAL",
    "WARNING",
]
SKIP_DIRS = {
    ".git",
    "node_modules",
    "__pycache__",
    "venv",
    "env",
    "dist",
    "build",
    ".venv",
}
# file size limit (bytes) to skip very large files
MAX_FILE_SIZE = 10 * 1024 * 1024
MAX_MATCHES_PER_FILE = 5
MAX_TRACEBACK_LINES = 400

# timestamp regex (ISO, common variants, dd/mm/yyyy)
TIMESTAMP_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z|\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}|\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}|\d{2}/\d{2}/\d{4})"
)


# helper to detect text files
def is_likely_text(path: Path) -> bool:
    try:
        with open(path, "rb") as f:
            chunk = f.read(2048)
        if b"\x00" in chunk:
            return False
        return True
    except Exception:
        return False


def safe_read_text(path: Path) -> str | None:
    try:
        with open(path, "rb") as f:
            raw = f.read()
        try:
            return raw.decode("utf-8")
        except Exception:
            try:
                return raw.decode("latin-1")
            except Exception:
                return None
    except Exception:
        return None


def find_tracebacks(lines):
    tbs = []
    for i, line in enumerate(lines):
        if "Traceback (most recent call last)" in line:
            # capture following lines up to a reasonable limit
            start = i
            end = min(len(lines), i + MAX_TRACEBACK_LINES)
            snippet = lines[start:end]
            # try to find final exception line within the snippet
            final_exc = None
            for line in reversed(snippet):
                if re.search(
                    r"^[A-Za-z0-9_\.]+(?:Error|Exception|Warning|Exit|Interrupt)?:", line
                ) or re.search(r": .+", line):
                    final_exc = line.strip()
                    break
            tbs.append(
                {
                    "start_line": start + 1,
                    "snippet": "\n".join(snippet[:200]),
                    "final": final_exc,
                }
            )
    return tbs


def main():
    files_scanned = 0
    matches = {}
    tracebacks = []
    timestamps = set()

    for dirpath, dirnames, filenames in os.walk(ROOT):
        # filter directories in-place to avoid descending into heavy folders
        dirnames[:] = [
            d for d in dirnames if d not in SKIP_DIRS and not d.startswith(".")
        ]
        for fname in filenames:
            fpath = Path(dirpath) / fname
            files_scanned += 1
            try:
                size = fpath.stat().st_size
                if size > MAX_FILE_SIZE:
                    continue
            except Exception:
                continue

            if not is_likely_text(fpath):
                continue

            text = safe_read_text(fpath)
            if text is None:
                continue

            lines = text.splitlines()
            file_matches = []
            for idx, line in enumerate(lines):
                for kw in KEYWORDS:
                    if kw in line:
                        start = max(0, idx - 2)
                        end = min(len(lines), idx + 3)
                        snippet = "\n".join(lines[start:end])
                        file_matches.append(
                            {"line_no": idx + 1, "keyword": kw, "snippet": snippet}
                        )
                        # collect timestamps in snippet
                        for ts in TIMESTAMP_RE.findall(snippet):
                            timestamps.add(ts)
                        break
                if len(file_matches) >= MAX_MATCHES_PER_FILE:
                    break

            if file_matches:
                matches[str(fpath)] = file_matches
                # detect tracebacks inside file
                tbs = find_tracebacks(lines)
                if tbs:
                    for tb in tbs:
                        tb["file"] = str(fpath)
                    tracebacks.extend(tbs)

    # build report
    from datetime import timezone

    now = datetime.now(timezone.utc).isoformat() + "Z"
    report = []
    report.append("Relatório de revisão de logs")
    report.append("Gerado em: " + now)
    report.append("Raiz escaneada: " + str(Path(ROOT).resolve()))
    report.append(f"Arquivos escaneados: {files_scanned}")
    report.append(f"Arquivos com ocorrências: {len(matches)}")
    report.append("")

    # include up to 50 files with samples
    for i, (fpath, fm) in enumerate(list(matches.items())[:50], start=1):
        report.append(f"{i}) {fpath}")
        for m in fm[:MAX_MATCHES_PER_FILE]:
            report.append(f'  - Linha {m["line_no"]} | keyword={m["keyword"]}')
            for line in m["snippet"].splitlines():
                report.append("    " + line)
        report.append("")

    if tracebacks:
        report.append("\nTracebacks detectados:")
        for tb in tracebacks[:20]:
            report.append(f'- {tb["file"]} (linha {tb["start_line"]})')   
            for line in tb["snippet"].splitlines()[:50]:
                report.append("    " + line)
            if tb.get("final"):
                report.append("    Final exception: " + str(tb["final"]))
            report.append("")

    report.append("Timestamps encontrados (amostra):")
    for t in sorted(list(timestamps))[:50]:
        report.append("- " + t)
    report.append("")

    report.append("Resumo priorizado:")
    if tracebacks:
        report.append(
            f"- {len(tracebacks)} tracebacks detectados. Prioridade: Alta — investigar os primeiros listados."
        )
    else:
        report.append("- Nenhum traceback detectado.")

    has_error = False
    for fm in matches.values():
        for m in fm:
            if m["keyword"] in ("ERROR", "CRITICAL") or "ERROR" in m["keyword"]:
                has_error = True
                break
        if has_error:
            break

    if has_error:
        report.append(
            "- Mensagens com nível ERROR/CRITICAL encontradas. Prioridade: Alta."
        )
    else:
        report.append("- Nenhuma mensagem ERROR/CRITICAL detectada nas amostras.")

    report.append("")
    report.append("Próximos passos sugeridos:")
    report.append(
        "- Abrir os arquivos listados no relatório (comece pelos que contêm tracebacks)."
    )
    report.append(
        "- Reproduzir o cenário localmente com logging mais verboso ou usando testes."
    )
    report.append(
        "- Executar: PYTHONPATH=src python -m pytest tests/ (ou equivalente no Windows)"
    )
    report.append("- Posso gerar issues/patches ou ajudar a depurar interativamente.")

    # write report
    try:
        with open(REPORT_PATH, "w", encoding="utf-8") as f:
            f.write("\n".join(report))
    except Exception as e:
        print("Erro escrevendo relatório:", e, file=sys.stderr)

    # print to stdout
    print("\n".join(report))
    print("\nRelatório salvo em:", REPORT_PATH)


if __name__ == "__main__":
    main()
