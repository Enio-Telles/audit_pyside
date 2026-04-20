#!/usr/bin/env python3
import polars as pl
from pathlib import Path
from datetime import datetime

def human_size(n):
    n = float(n)
    for unit in ["B","KB","MB","GB","TB"]:
        if n < 1024.0:
            return f"{n:.1f}{unit}"
        n /= 1024.0
    return f"{n:.1f}PB"

root = Path('.')
ignore_dirs = {"docs/referencias",".venv","venv","__pycache__",".git"}
files = sorted([p for p in root.rglob('*.parquet') if not any(part in ignore_dirs for part in p.parts)])

out_dir = Path('docs/referencias')
out_dir.mkdir(parents=True, exist_ok=True)
out_md = out_dir / 'parquet_referencias.md'

lines = []
lines.append('# Referências de arquivos Parquet\n')
lines.append(f'Gerado em {datetime.utcnow().isoformat()} UTC\n\n')
lines.append('Listagem de arquivos .parquet detectados no repositório com metadados e amostras (até 3 linhas).\n\n')

for p in files:
    rel = p.as_posix()
    if rel.startswith('docs/referencias'):
        continue
    size = p.stat().st_size
    hr = human_size(size)
    lines.append(f'## {rel}\n')
    lines.append(f'- Caminho: {rel}\n')
    lines.append(f'- Tamanho: {hr} ({size} bytes)\n')
    try:
        df = pl.read_parquet(str(p))
        rows = df.height
        schema = df.schema
        lines.append(f'- Linhas: {rows}\n')
        lines.append('- Colunas:\n')
        for k, v in schema.items():
            lines.append(f'  - `{k}`: {v}\n')
        sample = df.head(3)
        if sample.height > 0:
            cols = sample.columns
            header = '| ' + ' | '.join(cols) + ' |'
            sep = '| ' + ' | '.join(['---']*len(cols)) + ' |'
            lines.append('\nAmostra (até 3 linhas):\n\n')
            lines.append(header + '\n')
            lines.append(sep + '\n')
            for row in sample.to_dicts():
                row_vals = []
                for c in cols:
                    v = row.get(c, '')
                    if v is None:
                        s = ''
                    else:
                        s = str(v)
                    s = s.replace('\n', ' ').replace('|', '\\|')
                    if len(s) > 120:
                        s = s[:117] + '...'
                    row_vals.append(s)
                lines.append('| ' + ' | '.join(row_vals) + ' |\n')
    except Exception as e:
        lines.append(f'- Erro ao ler: {e}\n')
    lines.append('\n---\n\n')

out_md.write_text(''.join(lines), encoding='utf-8')
print(f'Gerado {out_md} — arquivos processados: {len(files)}')
